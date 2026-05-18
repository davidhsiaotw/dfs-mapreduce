package main

import (
	"dfs/messages"
	"fmt"
	"io"
	"log"
	mr "mapreduce/messages"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type workerStatus struct {
	info          *mr.WorkerInfo
	lastHeartbeat time.Time
}

type jobState struct {
	id          string
	request     *mr.JobRequest
	phase       mr.TaskType
	chunkNodes  map[string][][]string // fileName -> chunkId -> node addresses
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
		time.Sleep(5 * time.Second)
		m.workersMutex.Lock()
		now := time.Now()
		for id, ws := range m.workers {
			if now.Sub(ws.lastHeartbeat) > 30*time.Second {
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

	for _, inputFile := range req.InputFiles {
		fileChunkNodes, _, err := m.getChunkInfoFromDFS(inputFile)
		if err != nil {
			log.Printf("Failed to get chunk info for %s: %v\n", inputFile, err)
			msgHandler.SendJobResponse(false, "Failed to get chunk info from DFS for "+inputFile+": "+err.Error(), "")
			return
		}
		chunkNodes[inputFile] = fileChunkNodes
	}

	job := &jobState{
		id:          jobId,
		request:     req,
		phase:       mr.TaskType_MAP,
		chunkNodes:  chunkNodes,
	}

	m.jobsMutex.Lock()
	m.jobs[jobId] = job
	m.jobsMutex.Unlock()

	msgHandler.SendJobResponse(true, "Job accepted", jobId)
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
