package main

import (
	mr "mapreduce/messages"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
)

func submitJob(masterAddr string, inputFiles []string, pluginPath string) {
	conn, err := net.Dial("tcp", masterAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Master: %v\n", err)
	}
	defer conn.Close()

	handler := mr.NewMessageHandler(conn)

	jobBinary, err := os.ReadFile(pluginPath)
	if err != nil {
		log.Fatalf("Failed to read plugin file: %v\n", err)
	}

	err = handler.SendJobRequest(inputFiles, jobBinary)
	if err != nil {
		log.Fatalf("Failed to send job request: %v\n", err)
	}

	wrapper, err := handler.Receive()
	if err != nil {
		log.Fatalf("Failed to receive response: %v\n", err)
	}

	resp := wrapper.GetJobResp()
	if resp == nil || !resp.Resp.Ok {
		log.Fatalf("Job rejected: %s\n", resp.GetResp().GetMessage())
	}

	fmt.Printf("Job submitted successfully! Job ID: %s\n", resp.JobId)
}

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("Usage: %s <master_addr> <input_file1,input_file2,...> <plugin_path>\n", os.Args[0])
		os.Exit(1)
	}

	masterAddr := os.Args[1]
	inputFiles := strings.Split(os.Args[2], ",")
	pluginPath := os.Args[3]

	fmt.Printf("input files: %v\n", inputFiles)

	submitJob(masterAddr, inputFiles, pluginPath)
}
