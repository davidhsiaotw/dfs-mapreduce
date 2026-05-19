package main

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"io"
	"log"
	mr "mapreduce/messages"
	"mapreduce/util"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"plugin"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const basePath = "/bigdata/students/whsiao5/mr"
const dfsStorageDir = "/bigdata/students/whsiao5/mydfs"
const intermediateFilename = "inter-%s"
const indexFilename = "index-%s"
const pluginFile = "plugin.so"

type worker struct {
	id                string
	address           string
	masterAddr        string
	activeTasks       int32
	pluginMutex       sync.Mutex
	dfsControllerAddr string
	dfsClientPath     string
}

func newWorker(masterAddr, port, dfsControllerAddr, dfsClientPath string) *worker {
	hostname, _ := os.Hostname()
	shortName := strings.Split(hostname, ".")[0]
	return &worker{
		id:                fmt.Sprintf("%s-%s", shortName, port),
		address:           fmt.Sprintf("%s:%s", hostname, port),
		masterAddr:        masterAddr,
		dfsControllerAddr: dfsControllerAddr,
		dfsClientPath:     dfsClientPath,
	}
}

func (w *worker) startHeartbeats() {
	for {
		conn, err := net.Dial("tcp", w.masterAddr)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}
		handler := mr.NewMessageHandler(conn)
		for {
			cpuLoad, err := util.GetCpuUsage(500 * time.Millisecond)
			if err != nil {
				cpuLoad = 0
			}
			memLoad, err := util.GetMemoryUsage()
			if err != nil {
				memLoad = 0
			}

			err = handler.SendHeartbeat(w.id, w.address, uint32(cpuLoad), uint32(memLoad), uint32(atomic.LoadInt32(&w.activeTasks)))
			if err != nil {
				break
			}
			time.Sleep(5 * time.Second)
		}
		conn.Close()
	}
}

func (w *worker) handleTask(msgHandler *mr.MessageHandler, task *mr.TaskAssignment) {
	atomic.AddInt32(&w.activeTasks, 1)
	defer atomic.AddInt32(&w.activeTasks, -1)

	log.Printf("Received task: %s (%v) for job %s\n", task.TaskId, task.Type, task.JobId)

	var err error
	reduceData := make([]uint64, task.NumReducers)
	if task.Type == mr.TaskType_MAP {
		reduceData, err = w.runMap(task)
	} else {
		err = w.runReduce(task)
	}

	if err != nil {
		log.Printf("Task %s failed: %v\n", task.TaskId, err)
		msgHandler.SendTaskReport(task.JobId, task.TaskId, false, err.Error(), nil)
	} else {
		log.Printf("Task %s completed successfully\n", task.TaskId)
		msgHandler.SendTaskReport(task.JobId, task.TaskId, true, "", reduceData)
	}
}

func (w *worker) loadPlugin(jobBinary []byte, jobId string) (func([]byte) []util.KeyValue, func([]byte, [][]byte) []byte, error) {
	if len(jobBinary) == 0 {
		return nil, nil, fmt.Errorf("no job binary provided")
	}

	path := filepath.Join(basePath, jobId, pluginFile)

	w.pluginMutex.Lock()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path), 0755)
		err = os.WriteFile(path, jobBinary, 0644)
		if err != nil {
			w.pluginMutex.Unlock()
			return nil, nil, err
		}
	}
	w.pluginMutex.Unlock()

	p, err := plugin.Open(path)
	if err != nil {
		return nil, nil, err
	}

	mapSymbol, err := p.Lookup("Map")
	if err != nil {
		return nil, nil, err
	}
	mapFunc, ok := mapSymbol.(func([]byte) []util.KeyValue)
	if !ok {
		return nil, nil, fmt.Errorf("Map symbol has wrong type")
	}

	reduceSymbol, err := p.Lookup("Reduce")
	if err != nil {
		return nil, nil, err
	}
	reduceFunc, ok := reduceSymbol.(func([]byte, [][]byte) []byte)
	if !ok {
		return nil, nil, fmt.Errorf("Reduce symbol has wrong type")
	}

	return mapFunc, reduceFunc, nil
}

// runMap converts data into key-value via mapper and categorizes into intermediate files based on the hash value of the key, corresponding to a reducer.
func (w *worker) runMap(task *mr.TaskAssignment) ([]uint64, error) {
	chunkDir := filepath.Join(dfsStorageDir, task.InputFile)
	chunkPath := filepath.Join(chunkDir, fmt.Sprintf("chunk_%d", task.ChunkId))

	file, err := os.Open(chunkPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Chunk %d for file %s not found locally. Fetching from DFS...\n", task.ChunkId, task.InputFile)
			err = os.MkdirAll(chunkDir, 0755)
			if err != nil {
				return nil, fmt.Errorf("failed to create directory for chunk: %v", err)
			}

			cmd := exec.Command(w.dfsClientPath, w.dfsControllerAddr, "getchunk", task.InputFile, fmt.Sprintf("%d", task.ChunkId), chunkDir)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				return nil, fmt.Errorf("failed to retrieve chunk from DFS: %v", err)
			}

			file, err = os.Open(chunkPath)
			if err != nil {
				return nil, fmt.Errorf("chunk still not found after retrieval: %v", err)
			}
		} else {
			return nil, fmt.Errorf("failed to open chunk: %v", err)
		}
	}
	defer file.Close()

	mapFunc, _, err := w.loadPlugin(task.JobBinary, task.JobId)
	if err != nil {
		return nil, fmt.Errorf("failed to load plugin for map task: %v", err)
	}
	if mapFunc == nil {
		return nil, fmt.Errorf("mapFunc is nil in plugin")
	}

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 1<<12)
	scanner.Buffer(buf, 16<<20)
	tmpDir := filepath.Join(basePath, task.JobId)
	os.MkdirAll(tmpDir, 0755)

	var buffer []util.KeyValue
	spillIdx := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		kvs := mapFunc(line)

		for _, kv := range kvs {
			k := make([]byte, len(kv.Key))
			copy(k, kv.Key)
			v := make([]byte, len(kv.Value))
			copy(v, kv.Value)
			buffer = append(buffer, util.KeyValue{Key: k, Value: v})

			if len(buffer) >= 10000 {
				err := w.spill(buffer, task.TaskId, spillIdx, task.NumReducers, tmpDir)
				if err != nil {
					return nil, err
				}
				buffer = buffer[:0]
				spillIdx++
			}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("Error scanning input file for map task: %v\n", err)
	}

	if len(buffer) > 0 {
		err := w.spill(buffer, task.TaskId, spillIdx, task.NumReducers, tmpDir)
		if err != nil {
			return nil, err
		}
		spillIdx++
	}

	return w.mergeSpills(task.TaskId, spillIdx, task.NumReducers, tmpDir)
}

// spill partitions buffer data by key, sorts each partitioned data, then writes to a spill file and records offsets in an index file.
func (w *worker) spill(buffer []util.KeyValue, taskId string, spillIdx int, numReducers uint32, tmpDir string) error {
	partitions := make([][]util.KeyValue, numReducers)
	for _, kv := range buffer {
		r := w.ihash(kv.Key) % numReducers
		partitions[r] = append(partitions[r], kv)
	}

	spillPath := filepath.Join(tmpDir, fmt.Sprintf("spill-%s-%d", taskId, spillIdx))
	indexPath := filepath.Join(tmpDir, fmt.Sprintf("index-%s-%d", taskId, spillIdx))

	sf, err := os.Create(spillPath)
	if err != nil {
		return err
	}
	defer sf.Close()
	writer := bufio.NewWriter(sf)

	idxF, err := os.Create(indexPath)
	if err != nil {
		return err
	}
	defer idxF.Close()

	var offset int64 = 0

	for r := range numReducers {
		part := partitions[r]
		sort.Slice(part, func(i, j int) bool {
			return bytes.Compare(part[i].Key, part[j].Key) < 0
		})

		startOffset := offset
		for _, kv := range part {
			n, _ := writer.Write(kv.Key)
			offset += int64(n)
			writer.WriteByte('\t')
			offset++
			n, _ = writer.Write(kv.Value)
			offset += int64(n)
			writer.WriteByte('\n')
			offset++
		}
		writer.Flush()
		fmt.Fprintf(idxF, "%d %d %d\n", r, startOffset, offset)
	}

	return nil
}

// mergeSpill uses k-way merge to combines sorted paritioned data across spill files into an intermediate file and records offsets in an index file.
func (w *worker) mergeSpills(taskId string, numSpills int, numReducers uint32, tmpDir string) ([]uint64, error) {
	interPath := filepath.Join(tmpDir, fmt.Sprintf(intermediateFilename, taskId))
	indexPath := filepath.Join(tmpDir, fmt.Sprintf(indexFilename, taskId))

	outF, err := os.Create(interPath)
	if err != nil {
		return nil, err
	}
	defer outF.Close()
	writer := bufio.NewWriter(outF)

	idxF, err := os.Create(indexPath)
	if err != nil {
		return nil, err
	}
	defer idxF.Close()

	var currentOffset int64 = 0
	reduceData := make([]uint64, numReducers)

	for r := range numReducers {
		startOffset := currentOffset
		h := &util.ScannerHeap{}
		heap.Init(h)

		var openFiles []*os.File

		for i := range numSpills {
			spillPath := filepath.Join(tmpDir, fmt.Sprintf("spill-%s-%d", taskId, i))
			idxPath := filepath.Join(tmpDir, fmt.Sprintf("index-%s-%d", taskId, i))

			idxBytes, err := os.ReadFile(idxPath)
			if err != nil {
				continue
			}
			lines := strings.Split(string(idxBytes), "\n")
			var rId uint32
			var pStart, pEnd int64
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				fmt.Sscanf(line, "%d %d %d", &rId, &pStart, &pEnd)
				if rId == r {
					break
				}
			}

			if pEnd > pStart {
				f, err := os.Open(spillPath)
				if err != nil {
					continue
				}
				openFiles = append(openFiles, f)
				section := io.NewSectionReader(f, pStart, pEnd-pStart)
				s := util.NewKVScanner(section)
				if s.Next() {
					heap.Push(h, s)
				}
			}
		}

		for h.Len() > 0 {
			s := heap.Pop(h).(*util.KVScanner)
			n, _ := writer.Write(s.Current.Key)
			currentOffset += int64(n)
			writer.WriteByte('\t')
			currentOffset++
			n, _ = writer.Write(s.Current.Value)
			currentOffset += int64(n)
			writer.WriteByte('\n')
			currentOffset++
			if s.Next() {
				heap.Push(h, s)
			}
		}
		writer.Flush()
		fmt.Fprintf(idxF, "%d %d %d\n", r, startOffset, currentOffset)
		reduceData[r] = uint64(currentOffset - startOffset)

		for _, f := range openFiles {
			f.Close()
		}
	}

	for i := range numSpills {
		spillPath := filepath.Join(tmpDir, fmt.Sprintf("spill-%s-%d", taskId, i))
		idxPath := filepath.Join(tmpDir, fmt.Sprintf("index-%s-%d", taskId, i))
		os.Remove(spillPath)
		os.Remove(idxPath)
	}

	return reduceData, nil
}

// handleFetchIntermediate fetches partitioned data from intermediate files based on index files.
func (w *worker) handleFetchIntermediate(msgHandler *mr.MessageHandler, req *mr.FetchIntermediateRequest) {
	tmpDir := filepath.Join(basePath, req.JobId)

	idxPath := filepath.Join(tmpDir, fmt.Sprintf(indexFilename, req.TaskId))
	idxBytes, err := os.ReadFile(idxPath)
	if err != nil {
		msgHandler.SendResponse(false, "intermediate data index not found")
		return
	}

	msgHandler.SendResponse(true, "sending intermediate data...")

	lines := strings.Split(string(idxBytes), "\n")
	var pStart, pEnd int64 = 0, 0
	found := false
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		var rId uint32
		var start, end int64
		fmt.Sscanf(line, "%d %d %d", &rId, &start, &end)
		if rId == req.ReducerId {
			pStart = start
			pEnd = end
			found = true
			break
		}
	}

	if found && pEnd > pStart {
		interPath := filepath.Join(tmpDir, fmt.Sprintf(intermediateFilename, req.TaskId))
		fInter, err := os.Open(interPath)
		if err == nil {
			section := io.NewSectionReader(fInter, pStart, pEnd-pStart)
			content, err := io.ReadAll(section)
			if err == nil {
				msgHandler.WriteN(content)
			}
			fInter.Close()
		}
	}
}

// shuffle applied k-way merge on intermediate files retrieved from other workers, then group values into an array by key.
func (w *worker) shuffle(task *mr.TaskAssignment, tmpDir string) (string, error) {
	log.Printf("shuffling intermediate files for task %s (job %s)\n", task.TaskId, task.JobId)
	hostName, _ := os.Hostname()
	localNodeName := strings.Split(hostName, ".")[0]

	var fetchedFiles []string

	mapTaskInfo := task.MapTaskInfo

	for id, addr := range mapTaskInfo {
		if strings.HasPrefix(addr, localNodeName) {
			log.Printf("locally fetch data from %s on %s for task %s\n", fmt.Sprintf(indexFilename, id), localNodeName, task.TaskId)
			indexPath := filepath.Join(tmpDir, fmt.Sprintf(indexFilename, id))

			idxBytes, err := os.ReadFile(indexPath)
			if err != nil {
				continue
			}

			lines := strings.Split(string(idxBytes), "\n")
			var pStart, pEnd int64 = 0, 0
			found := false
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				var rId uint32
				var start, end int64
				fmt.Sscanf(line, "%d %d %d", &rId, &start, &end)
				if rId == task.ReducerId {
					pStart = start
					pEnd = end
					found = true
					break
				}
			}

			if found && pEnd > pStart {
				fetchedPath := filepath.Join(tmpDir, fmt.Sprintf("fetched-%s-%d", id, task.ReducerId))
				interPath := filepath.Join(tmpDir, fmt.Sprintf(intermediateFilename, id))

				src, err := os.Open(interPath)
				if err != nil {
					continue
				}
				dst, err := os.Create(fetchedPath)
				if err != nil {
					src.Close()
					continue
				}

				section := io.NewSectionReader(src, pStart, pEnd-pStart)
				io.Copy(dst, section)
				src.Close()
				dst.Close()
				fetchedFiles = append(fetchedFiles, fetchedPath)
			}
		} else {
			log.Printf("remotely fetch data from %s on %s for task %s\n", fmt.Sprintf(indexFilename, id), addr, task.TaskId)
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				log.Printf("Failed to connect to worker %s: %v\n", addr, err)
				continue
			}

			msgHandler := mr.NewMessageHandler(conn)
			err = msgHandler.SendFetchIntermediateRequest(task.JobId, task.ReducerId, id)
			if err != nil {
				conn.Close()
				continue
			}

			wrapper, err := msgHandler.Receive()
			if err != nil {
				conn.Close()
				continue
			}

			resp := wrapper.GetResponse()
			if resp == nil || !resp.Ok {
				conn.Close()
				continue
			}

			fetchedPath := filepath.Join(tmpDir, fmt.Sprintf("fetched-%s-%d", id, task.ReducerId))
			dst, err := os.Create(fetchedPath)
			if err != nil {
				conn.Close()
				continue
			}

			_, err = io.Copy(dst, conn)
			dst.Close()
			conn.Close()

			if err == nil || err == io.EOF {
				fetchedFiles = append(fetchedFiles, fetchedPath)
			}
		}
	}

	sortedFile := filepath.Join(tmpDir, fmt.Sprintf("sorted-%d", task.ReducerId))
	err := util.ExternalSort(fetchedFiles, sortedFile)
	if err != nil {
		return "", fmt.Errorf("external sort failed: %v", err)
	}

	for _, f := range fetchedFiles {
		os.Remove(f)
	}

	mergedPath := filepath.Join(tmpDir, fmt.Sprintf("merged-%d", task.ReducerId))
	mf, err := os.Create(mergedPath)
	if err != nil {
		return "", err
	}
	mw := bufio.NewWriter(mf)

	sf, err := os.Open(sortedFile)
	if err != nil {
		mf.Close()
		return "", err
	}

	scanner := util.NewKVScanner(sf)
	var currentKey []byte
	var values [][]byte

	for scanner.Next() {
		kv := scanner.Current
		if !bytes.Equal(kv.Key, currentKey) && currentKey != nil {
			mw.Write(currentKey)
			mw.WriteByte('\t')
			for i, v := range values {
				mw.Write(v)
				if i < len(values)-1 {
					mw.WriteByte(',')
				}
			}
			mw.WriteByte('\n')
			values = nil
		}
		currentKey = make([]byte, len(kv.Key))
		copy(currentKey, kv.Key)
		val := make([]byte, len(kv.Value))
		copy(val, kv.Value)
		values = append(values, val)
	}
	if currentKey != nil {
		mw.Write(currentKey)
		mw.WriteByte('\t')
		for i, v := range values {
			mw.Write(v)
			if i < len(values)-1 {
				mw.WriteByte(',')
			}
		}
		mw.WriteByte('\n')
	}
	mw.Flush()
	sf.Close()
	mf.Close()
	os.Remove(sortedFile)

	return mergedPath, nil
}

func (w *worker) runReduce(task *mr.TaskAssignment) error {
	hostname, _ := os.Hostname()
	log.Printf("Reduce task %s starting for job %s on worker %s\n", task.TaskId, task.JobId, hostname)

	tmpDir := filepath.Join(basePath, task.JobId)
	os.MkdirAll(tmpDir, 0755)

	mergedFile, err := w.shuffle(task, tmpDir)
	if err != nil {
		return err
	}

	_, reduceFunc, err := w.loadPlugin(task.JobBinary, task.JobId)
	if err != nil {
		return fmt.Errorf("failed to load plugin for reduce task: %v", err)
	}
	if reduceFunc == nil {
		return fmt.Errorf("reduceFunc is nil in plugin")
	}

	mergedF, err := os.Open(mergedFile)
	if err != nil {
		return err
	}
	defer mergedF.Close()

	resPath := filepath.Join(tmpDir, fmt.Sprintf("res-%s-%d", task.JobId, task.ReducerId))
	resFile, err := os.Create(resPath)
	if err != nil {
		return err
	}
	defer resFile.Close()
	resWriter := bufio.NewWriter(resFile)

	bufScanner := bufio.NewScanner(mergedF)
	bufR := make([]byte, 16<<20)
	bufScanner.Buffer(bufR, 16<<20)
	for bufScanner.Scan() {
		line := bufScanner.Bytes()
		parts := bytes.SplitN(line, []byte("\t"), 2)
		if len(parts) == 2 {
			key := parts[0]
			vals := bytes.Split(parts[1], []byte(","))
			res := reduceFunc(key, vals)
			resWriter.Write(key)
			resWriter.WriteString(": ")
			resWriter.Write(res)
			resWriter.WriteByte('\n')
		}
	}
	if err := bufScanner.Err(); err != nil {
		log.Printf("Error scanning merged file for reduce task: %v\n", err)
	}
	resWriter.Flush()

	log.Printf("Reduce task %s finished, output at %s\n", task.TaskId, resPath)

	log.Printf("Uploading %s to DFS...\n", resPath)
	cmd := exec.Command(w.dfsClientPath, w.dfsControllerAddr, "put", resPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to upload result to DFS: %v", err)
	}

	return nil
}

func (w *worker) ihash(s []byte) uint32 {
	h := uint32(0)
	for i := range s {
		h = h*31 + uint32(s[i])
	}
	return h & 0x7fffffff
}

func (w *worker) handleConnection(conn net.Conn) {
	handler := mr.NewMessageHandler(conn)
	defer handler.Close()

	for {
		wrapper, err := handler.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("Worker connection error: %v\n", err)
			}
			return
		}

		if task := wrapper.GetTaskAssign(); task != nil {
			w.handleTask(handler, task)
			return
		} else if req := wrapper.GetFetchInter(); req != nil {
			w.handleFetchIntermediate(handler, req)
			return
		}
	}
}

func main() {
	if len(os.Args) < 5 {
		fmt.Printf("Usage: %s <port> <master_addr> <dfs_controller_addr> <dfs_client_path>\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]
	masterAddr := os.Args[2]
	dfsControllerAddr := os.Args[3]
	dfsClientPath := os.Args[4]

	w := newWorker(masterAddr, port, dfsControllerAddr, dfsClientPath)
	go w.startHeartbeats()

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("Failed to listen: %v\n", err)
	}
	defer ln.Close()

	log.Printf("MapReduce Worker started on port %s\n", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go w.handleConnection(conn)
	}
}
