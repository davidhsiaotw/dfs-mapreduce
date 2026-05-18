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
