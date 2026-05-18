package main

import (
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
}

type master struct {
	workers      map[string]*workerStatus
	workersMutex sync.RWMutex

	jobs      map[string]*jobState
	jobsMutex sync.RWMutex
}

func newMaster() *master {
	return &master{
		workers:       make(map[string]*workerStatus),
		jobs:          make(map[string]*jobState),
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

// handleJobSubmission gets all chunks' metadata (ID, stored locations) for each input file, converts them into a job, and dispatches tasks.
func (m *master) handleJobSubmission(msgHandler *mr.MessageHandler, req *mr.JobRequest) {
	jobId := fmt.Sprintf("%d", time.Now().UnixNano())
	log.Printf("Received job submission: %s, inputs: %v\n", jobId, req.InputFiles)

	job := &jobState{
		id:          jobId,
		request:     req,
		phase:       mr.TaskType_MAP,
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
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <port>\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]

	m := newMaster()
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
