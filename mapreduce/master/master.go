package main

import (
	"dfs/messages"
	"fmt"
	"io"
	"log"
	mr "mapreduce/messages"
	"math"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const lbFactor = 0.95 // either 0.95 or 1.75
const numNodes = 21
const numReducersPerNode = 5
const maxNumReducers = lbFactor * numNodes * numReducersPerNode
const numChunksPerReducers = 5
const cpuThreshold = 80
const memThreshold = 80

type workerStatus struct {
	info          *mr.WorkerInfo
	lastHeartbeat time.Time
}

type MapTaskInfo struct {
	FileName string
	ChunkId  uint64
	NodeAddr string
}

type jobState struct {
	id         string
	request    *mr.JobRequest
	phase      mr.TaskType
	chunkNodes map[string][][]string // fileName -> chunkId -> node addresses

	mapTaskListMutex sync.Mutex
	mapTaskList      []MapTaskInfo

	numReducers     uint32
	reduceDataMutex sync.Mutex
	reduceData      []map[string]uint64 // reducerIdx -> workerAddress -> total bytes
}

type master struct {
	workers      map[string]*workerStatus
	workersMutex sync.RWMutex

	jobs      map[string]*jobState
	jobsMutex sync.RWMutex

	dfsController string
}

func newMaster(dfsController string) *master {
	return &master{
		workers:       make(map[string]*workerStatus),
		jobs:          make(map[string]*jobState),
		dfsController: dfsController,
	}
}

func (m *master) handleWorkerHeartbeat(hb *mr.Heartbeat) {
	m.workersMutex.Lock()
	defer m.workersMutex.Unlock()

	m.workers[hb.Worker.Id] = &workerStatus{
		info:          hb.Worker,
		lastHeartbeat: time.Now(),
	}
	// log.Printf("Heartbeat from worker %s (%s)\n", hb.Worker.Id, hb.Worker.Address)
}

func (m *master) monitorWorkers() {
	for {
		time.Sleep(2 * time.Second)
		m.workersMutex.Lock()
		now := time.Now()
		for id, ws := range m.workers {
			if now.Sub(ws.lastHeartbeat) > 15*time.Second {
				log.Printf("Worker %s is DEAD\n", id)
				delete(m.workers, id)
			}
		}
		m.workersMutex.Unlock()
	}
}

// getChunkInfoFromDFS gets each chunk's ID and stored locations.
func (m *master) getChunkInfoFromDFS(fileName string) ([][]string, int, error) {
	conn, err := net.Dial("tcp", m.dfsController)
	if err != nil {
		return nil, 0, err
	}
	defer conn.Close()

	handler := messages.NewMessageHandler(conn)
	err = handler.SendGetFileRequest(fileName)
	if err != nil {
		return nil, 0, err
	}

	wrapper, err := handler.Receive()
	if err != nil {
		return nil, 0, err
	}

	resp := wrapper.GetGetFileResp()
	if resp == nil || !resp.Resp.Ok {
		return nil, 0, fmt.Errorf("DFS error: %s", resp.GetResp().GetMessage())
	}

	chunkNodes := make([][]string, len(resp.Locations))
	for _, loc := range resp.Locations {
		var addrs []string
		for _, node := range loc.Nodes {
			addrs = append(addrs, node.Address)
		}
		chunkNodes[loc.Metadata.Id] = addrs
	}

	return chunkNodes, len(resp.Locations), nil
}

// handleJobSubmission gets all chunks' metadata (ID, stored locations) for each input file, converts them into a job, and dispatches tasks.
func (m *master) handleJobSubmission(msgHandler *mr.MessageHandler, req *mr.JobRequest) {
	jobId := fmt.Sprintf("%d", time.Now().UnixNano())
	log.Printf("Received job submission: %s, inputs: %v\n", jobId, req.InputFiles)

	chunkNodes := make(map[string][][]string)
	var mapTaskList []MapTaskInfo

	for _, inputFile := range req.InputFiles {
		fileChunkNodes, numChunks, err := m.getChunkInfoFromDFS(inputFile)
		if err != nil {
			log.Printf("Failed to get chunk info for %s: %v\n", inputFile, err)
			msgHandler.SendJobResponse(false, "Failed to get chunk info from DFS for "+inputFile+": "+err.Error(), "")
			return
		}
		chunkNodes[inputFile] = fileChunkNodes
		for i := range numChunks {
			mapTaskList = append(mapTaskList, MapTaskInfo{FileName: inputFile, ChunkId: uint64(i)})
		}
	}

	numReducers := req.NumReducers
	if numReducers == 0 {
		numReducers = uint32(math.Max(
			1, math.Min(
				math.Ceil(float64(len(mapTaskList))/float64(numChunksPerReducers)), math.Round(maxNumReducers)),
		),
		)
	}
	job := &jobState{
		id:          jobId,
		request:     req,
		phase:       mr.TaskType_MAP,
		chunkNodes:  chunkNodes,
		mapTaskList: mapTaskList,
		numReducers: numReducers,
		reduceData:  make([]map[string]uint64, numReducers),
	}

	m.jobsMutex.Lock()
	m.jobs[jobId] = job
	m.jobsMutex.Unlock()

	msgHandler.SendJobResponse(true, "Job accepted", jobId)

	go m.runJob(job, msgHandler)
}

func (m *master) runJob(job *jobState, clientHandler *mr.MessageHandler) {
	jobId := job.id

	var clientMu sync.Mutex
	sendProgress := func(phase string, completed, total uint32, isComplete, isError bool, msg string) {
		if clientHandler == nil {
			return
		}
		clientMu.Lock()
		defer clientMu.Unlock()
		clientHandler.SendJobProgress(jobId, phase, completed, total, isComplete, isError, msg)
	}

	log.Printf("Running job %s (MAP phase)\n", jobId)
	sendProgress("MAP", 0, uint32(len(job.mapTaskList)), false, false, "")

	var wg sync.WaitGroup
	var mapCompleted uint32
	for i := 0; i < len(job.mapTaskList); i++ {
		wg.Add(1)
		go func(taskIndex int) {
			defer wg.Done()
			if taskIndex > 0 {
				time.Sleep(time.Duration(rand.UintN(5)+2) * time.Second)
			}
			workerAddr, reduceData, err := m.assignMapTask(jobId, taskIndex)
			if reduceData == nil || err != nil {
				log.Printf("skip a map task %d (%s): %v\n", taskIndex, jobId, err)
				sendProgress("MAP", atomic.AddUint32(&mapCompleted, 0), uint32(len(job.mapTaskList)), false, true, err.Error())
				return
			}
			job.mapTaskListMutex.Lock()
			job.mapTaskList[taskIndex].NodeAddr = workerAddr
			job.mapTaskListMutex.Unlock()
			job.reduceDataMutex.Lock()
			for rId, bytes := range reduceData {
				if job.reduceData[rId] == nil {
					job.reduceData[rId] = make(map[string]uint64)
				}
				job.reduceData[rId][workerAddr] += bytes
			}
			job.reduceDataMutex.Unlock()

			c := atomic.AddUint32(&mapCompleted, 1)
			sendProgress("MAP", c, uint32(len(job.mapTaskList)), false, false, "")
		}(i)
	}
	wg.Wait()

	log.Printf("Job %s: MAP phase complete\n", jobId)

	job.phase = mr.TaskType_REDUCE
	log.Printf("Running job %s (REDUCE phase)\n", jobId)
	sendProgress("REDUCE", 0, uint32(job.numReducers), false, false, "")

	var reduceCompleted uint32
	for i := range int(job.numReducers) {
		wg.Add(1)
		go func(reducerId uint32) {
			defer wg.Done()
			if reducerId > 0 {
				time.Sleep(time.Duration(rand.UintN(5)+2) * time.Second)
			}
			err := m.assignReduceTask(jobId, reducerId)
			if err != nil {
				log.Printf("skip a reduce task %d (%s): %v\n", reducerId, jobId, err)
				sendProgress("MAP", atomic.AddUint32(&mapCompleted, 0), uint32(len(job.mapTaskList)), false, true, err.Error())
				return
			}
			c := atomic.AddUint32(&reduceCompleted, 1)
			sendProgress("REDUCE", c, uint32(job.numReducers), false, false, "")
		}(uint32(i))
	}
	wg.Wait()

	log.Printf("Job %s: REDUCE phase complete\n", jobId)

	sendProgress("COMPLETED", 0, 0, true, false, "Job completed successfully")
	m.jobsMutex.Lock()
	delete(m.jobs, jobId)
	m.jobsMutex.Unlock()
}

// selectWorker selects the most available node/worker from nodes, that has available resources and the least active tasks. If there is no chosen node/worker, it selects the most available worker from master's worker list.
func (m *master) selectWorker(nodes []string, delay bool) string {
	m.workersMutex.RLock()
	defer m.workersMutex.RUnlock()

	if len(m.workers) == 0 {
		return ""
	}

	var bestWorker string
	minTasks := uint32(0xffffffff)

	for _, addr := range nodes {
		nodeHost, _, err := net.SplitHostPort(addr)
		if err != nil {
			nodeHost = addr
		}
		for id, ws := range m.workers {
			workerHost, _, err := net.SplitHostPort(ws.info.Address)
			if err != nil {
				workerHost = ws.info.Address
			}
			if nodeHost == workerHost {
				// log.Printf("worker %s resource stats: %d%% cpu,%d%% mem, %d active tasks", wsHost, ws.info.CpuLoad, ws.info.MemLoad, ws.info.ActiveTasks)
				if ws.info.CpuLoad < cpuThreshold && ws.info.MemLoad < memThreshold {
					if ws.info.ActiveTasks < minTasks {
						minTasks = ws.info.ActiveTasks
						bestWorker = id
					}
				}
			}
		}
	}

	if bestWorker != "" {
		return bestWorker
	}

	if delay && nodes != nil {
		return ""
	}

	minTasks = uint32(0xffffffff)
	for id, ws := range m.workers {
		if ws.info.CpuLoad < cpuThreshold && ws.info.MemLoad < memThreshold {
			if ws.info.ActiveTasks < minTasks {
				minTasks = ws.info.ActiveTasks
				bestWorker = id
			}
		}
	}

	return bestWorker
}

// assignMapTask dispatches a map task to a node/worker. After trying five times, the map task is skipped.
func (m *master) assignMapTask(jobId string, taskIndex int) (string, []uint64, error) {
	m.jobsMutex.RLock()
	job := m.jobs[jobId]
	taskInfo := job.mapTaskList[taskIndex]
	m.jobsMutex.RUnlock()

	var retry uint8 = 0
	const limit uint8 = 5
	for {
		if retry >= limit {
			return "", nil, fmt.Errorf("failed to assign map task")
		}

		nodes := job.chunkNodes[taskInfo.FileName][taskInfo.ChunkId]
		workerId := m.selectWorker(nodes, retry <= (limit>>1))
		if workerId == "" {
			time.Sleep(3 * time.Second)
			retry += 1
			log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
			continue
		}

		m.workersMutex.RLock()
		ws := m.workers[workerId]
		m.workersMutex.RUnlock()

		conn, err := net.Dial("tcp", ws.info.Address)
		if err != nil {
			log.Printf("failed to connect to worker %s: %v\n", workerId, err)
			retry += 1
			log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
			continue
		}

		handler := mr.NewMessageHandler(conn)
		taskId := fmt.Sprintf("%s-%d", taskInfo.FileName, taskInfo.ChunkId)
		log.Printf("assign map task %s to worker %s", taskId, workerId)
		err = handler.SendTaskAssignment(jobId, taskId, mr.TaskType_MAP, taskInfo.FileName, taskInfo.ChunkId, job.request.JobBinary, uint32(job.numReducers), 0, nil)
		if err != nil {
			conn.Close()
			retry += 1
			log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
			continue
		}

		for {
			wrapper, err := handler.Receive()
			if err != nil {
				break
			}
			if report := wrapper.GetTaskReport(); report != nil {
				if report.Success {
					log.Printf("Task %s completed successfully\n", taskId)
					conn.Close()
					return ws.info.Address, report.ReduceData, nil
				} else {
					log.Printf("Task %s failed: %s. Retrying...\n", taskId, report.Message)
					break
				}
			}
		}
		conn.Close()
		retry += 1
		log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
		time.Sleep(3 * time.Second)
	}
}

// selectReduceWorker selects a worker for a reduce task based on maximum local data.
func (m *master) selectReduceWorker(reduceId uint32, workerData map[string]uint64, delay bool) string {
	m.workersMutex.RLock()
	defer m.workersMutex.RUnlock()

	if len(m.workers) == 0 {
		return ""
	}

	var bestWorker string
	var maxBytes uint64 = 0
	found := false

	for id, ws := range m.workers {
		workerHost, _, err := net.SplitHostPort(ws.info.Address)
		if err != nil {
			workerHost = ws.info.Address
		}

		if ws.info.CpuLoad < cpuThreshold && ws.info.MemLoad < memThreshold {
			var bytesOnWorker uint64 = 0
			for addr, b := range workerData {
				addrHost, _, err := net.SplitHostPort(addr)
				if err != nil {
					addrHost = addr
				}
				if addrHost == workerHost {
					log.Printf("worker at %s has %d bytes for reduce %d\n", addr, b, reduceId)
					bytesOnWorker += b
				}
			}

			if !found || bytesOnWorker > maxBytes {
				maxBytes = bytesOnWorker
				bestWorker = id
				found = true
			}
		}
	}

	if found {
		return bestWorker
	}

	if delay {
		return ""
	}

	var minTasks uint32 = 0xffffffff
	for id, ws := range m.workers {
		if ws.info.CpuLoad < cpuThreshold && ws.info.MemLoad < memThreshold {
			if ws.info.ActiveTasks < minTasks {
				minTasks = ws.info.ActiveTasks
				bestWorker = id
			}
		}
	}

	return bestWorker
}

// assignReduceTask dispatches a reduce task to a node/worker. After trying five times, the reduce is skipped.
func (m *master) assignReduceTask(jobId string, reducerId uint32) error {
	m.jobsMutex.RLock()
	job := m.jobs[jobId]
	m.jobsMutex.RUnlock()

	mapTaskInfo := make(map[string]string)
	for _, taskInfo := range job.mapTaskList {
		mapTaskInfo[fmt.Sprintf("%s-%d", taskInfo.FileName, taskInfo.ChunkId)] = taskInfo.NodeAddr
	}

	job.reduceDataMutex.Lock()
	workerData := job.reduceData[reducerId]
	job.reduceDataMutex.Unlock()

	var retry uint8 = 0
	const limit uint8 = 5
	for {
		if retry >= limit {
			return fmt.Errorf("failed to assign map task")
		}

		workerId := m.selectReduceWorker(reducerId, workerData, retry <= (limit>>1))
		if workerId == "" {
			time.Sleep(3 * time.Second)
			retry += 1
			log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
			continue
		}

		m.workersMutex.RLock()
		ws := m.workers[workerId]
		m.workersMutex.RUnlock()

		conn, err := net.Dial("tcp", ws.info.Address)
		if err != nil {
			log.Printf("failed to connect to worker %s: %v\n", workerId, err)
			retry += 1
			log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
			continue
		}

		handler := mr.NewMessageHandler(conn)
		taskId := fmt.Sprintf("reduce-%d", reducerId)
		log.Printf("assign reduce task %s to worker %s", taskId, workerId)
		err = handler.SendTaskAssignment(jobId, taskId, mr.TaskType_REDUCE, "", 0, job.request.JobBinary, uint32(job.numReducers), reducerId, mapTaskInfo)
		if err != nil {
			conn.Close()
			retry += 1
			log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
			continue
		}

		for {
			wrapper, err := handler.Receive()
			if err != nil {
				break
			}
			if report := wrapper.GetTaskReport(); report != nil {
				if report.Success {
					log.Printf("Task %s completed successfully\n", taskId)
					conn.Close()
					return nil
				} else {
					log.Printf("Task %s failed: %s. Retrying...\n", taskId, report.Message)
					break
				}
			}
		}
		conn.Close()
		retry += 1
		log.Printf("retrying to assign a map task (%d/%d)", retry, limit)
		time.Sleep(3 * time.Second)
	}
}

func (m *master) handleConnection(conn net.Conn) {
	handler := mr.NewMessageHandler(conn)
	defer handler.Close()

	for {
		wrapper, err := handler.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("connection error: %v\n", err)
			}
			return
		}

		switch msg := wrapper.Msg.(type) {
		case *mr.MapReduceWrapper_Heartbeat:
			m.handleWorkerHeartbeat(msg.Heartbeat)
		case *mr.MapReduceWrapper_JobReq:
			m.handleJobSubmission(handler, msg.JobReq)
		default:
			log.Printf("unhandled message type: %T\n", msg)
		}
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <port> <dfs_controller_addr>\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]
	dfsController := os.Args[2]

	m := newMaster(dfsController)
	go m.monitorWorkers()

	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v\n", err)
	}
	defer ln.Close()

	log.Printf("MapReduce Master started on port %s\n", port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go m.handleConnection(conn)
	}
}
