package main

import (
	"fmt"
	"io"
	"log"
	mr "mapreduce/messages"
	"mapreduce/util"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type worker struct {
	id                string
	address           string
	masterAddr        string
	activeTasks       int32
	pluginMutex       sync.Mutex
	dfsControllerAddr string
	dfsClientPath     string
}

func newWorker(masterAddr, port string) *worker {
	hostname, _ := os.Hostname()
	shortName := strings.Split(hostname, ".")[0]
	return &worker{
		id:                fmt.Sprintf("%s-%s", shortName, port),
		address:           fmt.Sprintf("%s:%s", hostname, port),
		masterAddr:        masterAddr,
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

func (w *worker) handleConnection(conn net.Conn) {
	handler := mr.NewMessageHandler(conn)
	defer handler.Close()

	for {
		_, err := handler.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("Worker connection error: %v\n", err)
			}
			return
		}
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <port> <master_addr>\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]
	masterAddr := os.Args[2]

	w := newWorker(masterAddr, port)
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
