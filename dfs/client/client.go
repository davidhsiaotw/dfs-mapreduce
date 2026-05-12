package main

import (
	"bufio"
	"crypto/md5"
	"dfs/messages"
	"dfs/util"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

const baseChunkFilename = "chunk_"
const minChunkSize uint32 = 64 << 20      // 64 MiB
const defaultChunkSize uint32 = 128 << 20 // 128 MiB
const maxChunkSize uint32 = 256 << 20     // 256 MiB
const mergeThreshold uint64 = 5 << 30     // 5 GiB
const spacePadding uint64 = 256 << 20     // 256 MiB
var chunkSize uint32 = defaultChunkSize

var maxConcurrent = max((512<<20)/chunkSize, (512<<20)/maxChunkSize)

// calculateChunks
func calculateChunks(fileName string, isText bool, maxChunkSize uint32) ([]uint32, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := info.Size()

	var chunkSizes []uint32

	if !isText {
		numChunks := (fileSize + int64(maxChunkSize) - 1) / int64(maxChunkSize)
		for i := range numChunks {
			size := int64(maxChunkSize)
			if i == numChunks-1 {
				size = fileSize - i*int64(maxChunkSize)
			}
			chunkSizes = append(chunkSizes, uint32(size))
		}
		return chunkSizes, nil
	}

	var currentOffset int64 = 0
	for currentOffset < fileSize {
		remaining := fileSize - currentOffset
		if remaining <= int64(maxChunkSize) {
			chunkSizes = append(chunkSizes, uint32(remaining))
			break
		}

		targetOffset := currentOffset + int64(maxChunkSize) - 1
		_, err := file.Seek(targetOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}

		buf := make([]byte, 4096)
		var extraBytes uint32 = 0
		foundNewline := false

		for {
			n, err := file.Read(buf)
			if n > 0 {
				for i := range n {
					if buf[i] == '\n' {
						extraBytes += uint32(i) + 1
						foundNewline = true
						break
					}
				}
				if foundNewline {
					break
				}
				extraBytes += uint32(n)
			}
			if err != nil {
				break
			}
		}

		currentChunkSize := (maxChunkSize - 1) + extraBytes
		chunkSizes = append(chunkSizes, currentChunkSize)
		currentOffset += int64(currentChunkSize)
	}

	return chunkSizes, nil
}

// put retrieves nodes and chunks information from a controller, then sends file chunks to appropriate nodes in parallel.
func put(controllerAddr string, fileName string) {
	fmt.Println("PUT", fileName)

	info, err := os.Stat(fileName)
	if err != nil {
		log.Fatalln(err)
	}

	isText := strings.HasSuffix(strings.ToLower(fileName), ".txt")
	chunkSizes, err := calculateChunks(fileName, isText, chunkSize)
	if err != nil {
		log.Fatalln("failed to calculate chunks:", err)
	}
	numChunks := uint64(len(chunkSizes))

	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		log.Fatalln("failed to connect to controller:", err)
	}
	controllerHandler := messages.NewMessageHandler(conn)

	controllerHandler.SendPutFileRequest(filepath.Base(fileName), uint64(info.Size()), chunkSize, numChunks)
	controllerMsgWrapper, err := controllerHandler.Receive()
	if err != nil {
		log.Fatalln("failed to receive from controller:", err)
	}
	putFileRespWrapper := controllerMsgWrapper.GetPutFileResp()
	if putFileRespWrapper == nil {
		log.Fatalf("unexpected message from controller: %T\n", controllerMsgWrapper.Msg)
	}
	if putFileRespWrapper.Resp == nil {
		log.Fatalf("malformed response from controller: Resp field is nil\n")
	}
	if !putFileRespWrapper.Resp.Ok {
		log.Fatalf("controller rejected PUT: %v\n", putFileRespWrapper.Resp.Message)
	}
	controllerHandler.Close()

	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()

	var wg sync.WaitGroup

	fmt.Printf("concurrency limit: %d chunks (%d MiB each max)\n", maxConcurrent, chunkSize>>20)
	
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				renewConn, err := net.Dial("tcp", controllerAddr)
				if err == nil {
					renewHandler := messages.NewMessageHandler(renewConn)
					renewHandler.SendLeaseRenewRequest(filepath.Base(fileName))
					renewHandler.Receive() // wait for controller's response to prevent TCP RST
					renewConn.Close()
				}
			}
		}
	}()

	semaphore := make(chan struct{}, maxConcurrent)
	for _, alloc := range putFileRespWrapper.Allocations {
		log.Printf("uploading Chunk %d to %v\n", alloc.Metadata.Id, alloc.Nodes)

		expectedSize := chunkSizes[alloc.Metadata.Id]
		buffer := make([]byte, expectedSize)
		_, err := io.ReadFull(file, buffer)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Fatalf("error reading chunk %d: %v\n", alloc.Metadata.Id, err)
		}

		chunkData := buffer
		wg.Add(1)
		semaphore <- struct{}{}

		go func(alloc *messages.ChunkAllocation, data []byte) {
			defer wg.Done()
			defer func() { <-semaphore }()

			success := false
			var lastErr error

			for _, targetNode := range alloc.Nodes {
				nodeConn, err := net.Dial("tcp", targetNode.Address)
				if err != nil {
					log.Printf("failed to connect to candidate node %s: %v\n", targetNode, err)
					lastErr = err
					continue
				}
				nodeHandler := messages.NewMessageHandler(nodeConn)
				checksum := md5.Sum(data)

				err = nodeHandler.SendStoreChunkRequest(fileName, alloc.Metadata.Id, uint32(len(data)), checksum[:], true)
				if err != nil {
					log.Printf("failed to send store request to candidate node %s: %v\n", targetNode, err)
					nodeHandler.Close()
					lastErr = err
					continue
				}

				nodeMsgWrapper, err := nodeHandler.Receive()
				if err != nil {
					log.Printf("failed to receive response from candidate node %s: %v\n", targetNode, err)
					nodeHandler.Close()
					lastErr = err
					continue
				}
				resp := nodeMsgWrapper.GetResponse()
				if resp == nil {
					log.Printf("unexpected message from candidate node %s: %T\n", targetNode, nodeMsgWrapper.Msg)
					nodeHandler.Close()
					lastErr = fmt.Errorf("unexpected message from candidate node")
					continue
				}
				if !resp.Ok {
					log.Printf("message from server %s: %s\n", targetNode, resp.Message)
					nodeHandler.Close()
					lastErr = fmt.Errorf("message from server: %s", resp.Message)
					continue
				}

				_, err = nodeHandler.Write(data)
				if err != nil {
					log.Printf("failed to send data to candidate node %s: %v\n", targetNode, err)
					nodeHandler.Close()
					lastErr = err
					continue
				}

				nodeMsgWrapper, err = nodeHandler.Receive()
				if err != nil {
					log.Printf("failed to receive final response from candidate node %s: %v\n", targetNode, err)
					nodeHandler.Close()
					lastErr = err
					continue
				}
				resp = nodeMsgWrapper.GetResponse()
				if resp == nil || !resp.Ok {
					log.Printf("storage failed on candidate node %s: %v\n", targetNode, resp.GetMessage())
					nodeHandler.Close()
					lastErr = fmt.Errorf("storage failed on candidate node: %v", resp.GetMessage())
					continue
				}

				log.Printf("successfully uploaded chunk %d to candidate node %s\n", alloc.Metadata.Id, targetNode)
				nodeHandler.Close()
				success = true
				break
			}

			if !success {
				log.Fatalf("all candidates failed for chunk %d: %v\n", alloc.Metadata.Id, lastErr)
			}
		}(alloc, chunkData)
	}

	wg.Wait()
	close(done)

	fmt.Println("Storage complete!")
}

// get retrieves chunk locations from a controller, then retrieves file chunks from appropriate nodes in parallel and merges them if requested by the user.
func get(controllerAddr string, fileName string, dir string) {
	fmt.Println("GET", fileName)

	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		log.Fatalln("failed to connect to controller:", err)
	}
	controllerHandler := messages.NewMessageHandler(conn)

	err = controllerHandler.SendGetFileRequest(filepath.Base(fileName))
	if err != nil {
		log.Fatalln("failed to send GET request to controller:", err)
	}

	wrapper, err := controllerHandler.Receive()
	if err != nil {
		log.Fatalln("failed to receive from controller:", err)
	}
	getFileRespWrapper := wrapper.GetGetFileResp()
	if getFileRespWrapper == nil {
		log.Fatalf("unexpected message from controller: %T\n", wrapper.Msg)
	}
	if getFileRespWrapper.Resp == nil {
		log.Fatalf("malformed response from controller: Resp field is nil\n")
	}
	if !getFileRespWrapper.Resp.Ok {
		log.Fatalf("controller rejected GET: %v\n", getFileRespWrapper.Resp.Message)
	}
	controllerHandler.Close()

	fileSize := getFileRespWrapper.Metadata.Size
	freeSpace := util.GetFreeSpace(dir)
	if freeSpace < fileSize+spacePadding {
		log.Fatalf("not enough free space to retrieve file. Required: %d bytes, Available: %d bytes\n", fileSize, freeSpace)
	}

	merge := true
	if fileSize > mergeThreshold {
		fmt.Printf("File size is ~%d GB. Do you want to merge chunks? (y/N): ", (fileSize+(1<<29))>>30)
		reader := bufio.NewReader(os.Stdin)
		response, _ := reader.ReadString('\n')
		if strings.ToLower(strings.TrimSpace(response)) != "y" {
			merge = false
		}
	}

	partsDir := filepath.Join(dir, fileName+".parts")
	if err := os.MkdirAll(partsDir, 0755); err != nil {
		log.Fatalf("failed to create temporary directory: %v\n", err)
	}

	var wg sync.WaitGroup

	fmt.Printf("concurrency limit: %d chunks (%d MiB each)\n", maxConcurrent, chunkSize>>20)
	semaphore := make(chan struct{}, maxConcurrent)
	for _, loc := range getFileRespWrapper.Locations {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(loc *messages.ChunkLocation) {
			defer wg.Done()
			defer func() { <-semaphore }()

			sortNodeAddresses(loc)

			if !getChunk(loc, fileName, partsDir) {
				os.RemoveAll(partsDir)
				log.Fatalf("failed to retrieve chunk %d from any available node. Terminating GET.\n", loc.Metadata.Id)
			}
		}(loc)
	}
	wg.Wait()

	if merge {
		finalPath := filepath.Join(dir, fileName)
		mergeChunks(getFileRespWrapper, partsDir, finalPath)
		log.Printf("file %s retrieved and merged successfully.\n", fileName)
	} else {
		log.Printf("file %s chunks retrieved successfully into %s\n", fileName, partsDir)
	}
}

// sortNodeAddresses sorts the node addresses by prioritizing the local node (if present) and then by cluster affinity (e.g. orion nodes together, mc nodes together).
func sortNodeAddresses(loc *messages.ChunkLocation) {
	hostname, err := os.Hostname()
	if err != nil {
		return
	}

	var prefix string
	if strings.Contains(hostname, "orion") {
		prefix = "orion"
	} else if strings.Contains(hostname, "mc") {
		prefix = "mc"
	}

	sort.SliceStable(loc.Nodes, func(i, j int) bool {
		addrI := loc.Nodes[i].Address
		addrJ := loc.Nodes[j].Address

		if strings.HasPrefix(addrI, hostname+":") && !strings.HasPrefix(addrJ, hostname+":") {
			return true
		}
		if !strings.HasPrefix(addrI, hostname+":") && strings.HasPrefix(addrJ, hostname+":") {
			return false
		}

		if prefix != "" {
			if strings.Contains(addrI, prefix) && !strings.Contains(addrJ, prefix) {
				return true
			}
			if !strings.Contains(addrI, prefix) && strings.Contains(addrJ, prefix) {
				return false
			}
		}

		return false
	})
}

// getChunk tries to retrieve a chunk from every nodes in order. A node should respond within a short timeout and the chunk transfer should complete within a longer timeout, otherwise the client will try the next node. Returns true if the chunk was successfully retrieved from any node, false if all nodes failed.
func getChunk(loc *messages.ChunkLocation, fileName string, partsDir string) bool {
	success := false
	for _, addr := range loc.Nodes {
		nodeConn, err := net.Dial("tcp", addr.Address)
		if err != nil {
			log.Printf("failed to connect to node %s: %v\n", addr, err)
			continue
		}
		nodeHandler := messages.NewMessageHandler(nodeConn)

		err = nodeHandler.SendRetrieveChunkRequest(filepath.Base(fileName), loc.Metadata.Id)
		if err != nil {
			log.Printf("failed to send retrieve request to node %s: %v\n", addr, err)
			nodeConn.Close()
			continue
		}

		nodeHandler.SetReadDeadline(uint64(float64(util.DeadlineSeconds(1024))), time.Second)
		wrapper, err := nodeHandler.Receive()
		if err != nil {
			log.Printf("failed to receive response from node %s: %v\n", addr, err)
			nodeConn.Close()
			continue
		}

		retrieveChunkRespWrapper := wrapper.GetRetrieveChunkResp()
		if retrieveChunkRespWrapper == nil {
			log.Printf("unexpected message from node %s: %T\n", addr, wrapper.Msg)
			nodeConn.Close()
			continue
		}
		if retrieveChunkRespWrapper.Resp == nil {
			log.Printf("malformed response from node %s: Resp field is nil\n", addr)
			nodeConn.Close()
			continue
		}
		if !retrieveChunkRespWrapper.Resp.Ok {
			log.Printf("node %s responded with error: %s\n", addr, retrieveChunkRespWrapper.Resp.Message)
			nodeConn.Close()
			continue
		}
		partPath := filepath.Join(partsDir, fmt.Sprintf("%s%d", baseChunkFilename, loc.Metadata.Id))
		partFile, err := os.Create(partPath)
		if err != nil {
			os.RemoveAll(partsDir)
			log.Fatalf("failed to create part file %s: %v\n", partPath, err)
		}

		hash := md5.New()
		writer := io.MultiWriter(partFile, hash)

		chunkTransferTime := uint64(float64(util.DeadlineSeconds(uint64(retrieveChunkRespWrapper.Metadata.Size))))
		nodeHandler.SetReadDeadline(chunkTransferTime, time.Second)
		log.Printf("receiving chunk %d from node %s in at most %d seconds...\n", loc.Metadata.Id, addr, chunkTransferTime)
		_, err = io.CopyN(writer, nodeHandler, int64(retrieveChunkRespWrapper.Metadata.Size))
		partFile.Close()
		if err != nil {
			os.Remove(partPath)
			log.Printf("failed to receive chunk %d from node %s in %d seconds: %v\n", loc.Metadata.Id, addr, chunkTransferTime, err)
			nodeConn.Close()
			continue
		}

		log.Printf("received chunk %d from node %s, verifying checksum...\n", loc.Metadata.Id, addr)
		if !util.VerifyChecksum(hash.Sum(nil), retrieveChunkRespWrapper.Metadata.Checksum) {
			os.Remove(partPath)
			log.Printf("checksum mismatch for chunk %d from node %s\n", loc.Metadata.Id, addr)
			nodeConn.Close()
			continue
		}

		nodeConn.Close()
		success = true
		break
	}
	return success
}

func mergeChunks(getFileRespWrapper *messages.GetFileResponse, partsDir string, finalPath string) {
	fmt.Println("merging chunks...")
	finalFile, err := os.Create(finalPath)
	if err != nil {
		log.Fatalf("failed to create final file %s: %v\n", finalPath, err)
	}
	defer finalFile.Close()

	for i, loc := range getFileRespWrapper.Locations {
		partPath := filepath.Join(partsDir, fmt.Sprintf("%s%d", baseChunkFilename, loc.Metadata.Id))
		data, err := os.ReadFile(partPath)
		if err != nil {
			log.Fatalf("failed to read part file %s: %v\n", partPath, err)
		}
		if _, err := finalFile.Write(data); err != nil {
			log.Fatalf("failed to write to final file: %v\n", err)
		}
		os.Remove(partPath)
		if (i+1)%10 == 0 {
			log.Printf("merged chunks %d-%d\n", i-9, i)
		}
	}
	if len(getFileRespWrapper.Locations)%10 != 0 {
		total := len(getFileRespWrapper.Locations)
		start := (total / 10) * 10
		log.Printf("merged chunks %d-%d\n", start, total-1)
	}
	os.RemoveAll(partsDir)
}

func delete(controllerAddr string, fileName string) {
	fmt.Println("DELETE", fileName)

	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		log.Fatalln("failed to connect to controller:", err)
	}
	controllerHandler := messages.NewMessageHandler(conn)

	controllerHandler.SendDeleteFileRequest(filepath.Base(fileName))
	controllerMsgWrapper, err := controllerHandler.Receive()
	if err != nil {
		log.Fatalln("failed to receive from controller:", err)
	}
	deleteFileResp := controllerMsgWrapper.GetResponse()
	if deleteFileResp == nil {
		log.Fatalf("unexpected message from controller: %T\n", controllerMsgWrapper.Msg)
	}
	if !deleteFileResp.Ok {
		log.Fatalf("controller rejected DELETE: %s\n", deleteFileResp.Message)
	}
	controllerHandler.Close()

	fmt.Printf("file %s was deleted successfully.\n", fileName)
}

func list(controllerAddr string) {
	fmt.Println("LIST")

	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		log.Fatalln("failed to connect to controller:", err)
	}
	controllerHandler := messages.NewMessageHandler(conn)

	err = controllerHandler.SendListFilesRequest()
	if err != nil {
		log.Fatalln("failed to send list files request to controller:", err)
	}

	wrapper, err := controllerHandler.Receive()
	if err != nil {
		log.Fatalln("failed to receive from controller:", err)
	}
	listFilesRespWrapper := wrapper.GetListFilesResp()
	if listFilesRespWrapper == nil {
		log.Fatalf("unexpected message from controller: %T\n", wrapper.Msg)
	}
	if listFilesRespWrapper.Resp == nil {
		log.Fatalf("malformed response from controller: Resp field is nil\n")
	}
	if !listFilesRespWrapper.Resp.Ok {
		log.Fatalf("controller rejected LIST: %s\n", listFilesRespWrapper.Resp.Message)
	}
	controllerHandler.Close()

	if len(listFilesRespWrapper.Files) == 0 {
		fmt.Println("no files were found.")
	} else {
		for _, file := range listFilesRespWrapper.Files {
			fmt.Printf("%d %s\n", file.Size, file.Name)
		}
	}
}

func nodes(controllerAddr string) {
	fmt.Println("NODES")

	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		log.Fatalln("failed to connect to controller:", err)
	}
	controllerHandler := messages.NewMessageHandler(conn)

	err = controllerHandler.SendNodeInfoRequest()
	if err != nil {
		log.Fatalln("failed to send node stats request to controller:", err)
	}

	wrapper, err := controllerHandler.Receive()
	if err != nil {
		log.Fatalln("failed to receive from controller:", err)
	}
	statsRespWrapper := wrapper.GetStatsResp()
	if statsRespWrapper == nil {
		log.Fatalf("unexpected message from controller: %T\n", wrapper.Msg)
	}
	if statsRespWrapper.Resp == nil {
		log.Fatalf("malformed response from controller: Resp field is nil\n")
	}
	if !statsRespWrapper.Resp.Ok {
		log.Fatalf("controller rejected NODES request: %s\n", statsRespWrapper.Resp.Message)
	}
	controllerHandler.Close()

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 4, ' ', tabwriter.TabIndent)
	fmt.Printf("Total Storage Used: %d bytes\n", statsRespWrapper.TotalUsed)
	fmt.Printf("Total Storage Available: %d bytes\n", statsRespWrapper.TotalFree)
	fmt.Fprintln(w, "nodes\tused space\tfree space\trunning requests")
	for _, node := range statsRespWrapper.NodeStats {
		fmt.Fprintf(w, "%s\t%d\t%d\t%d\n", node.Id, node.UsedSpace, node.FreeSpace, node.RequestsHandled)
	}
	w.Flush()
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <controller_addr> put|get|delete <file-name> [--chunk-size <size>] [download-dir]\n", os.Args[0])
		fmt.Printf("       %s <controller_addr> list|nodes\n", os.Args[0])
		os.Exit(1)
	}

	controllerAddr := os.Args[1]
	action := strings.ToLower(os.Args[2])
	fileName := ""
	if len(os.Args) >= 4 {
		fileName = os.Args[3]
	}

	switch action {
	case "put":
		if len(os.Args) >= 5 {
			if os.Args[4] == "--chunk-size" {
				parsedChunkSize, err := strconv.ParseUint(os.Args[5], 10, 32)
				if err != nil {
					log.Fatalln("invalid chunk size")
				} else {
					chunkSize = uint32(parsedChunkSize)
				}
				if chunkSize == 0 {
					log.Printf("chunk size cannot be zero, using default %d bytes\n", defaultChunkSize)
					chunkSize = defaultChunkSize
				} else if chunkSize < minChunkSize {
					log.Printf("chunk size too small, using minimum %d bytes\n", minChunkSize)
					chunkSize = minChunkSize
				} else if chunkSize > maxChunkSize {
					log.Printf("chunk size too large, using maximum %d bytes\n", maxChunkSize)
					chunkSize = maxChunkSize
				}
			}
		}
		put(controllerAddr, fileName)

	case "get":
		dir := "."
		if len(os.Args) >= 5 {
			dir = os.Args[4]
		}
		get(controllerAddr, fileName, dir)

	case "delete":
		delete(controllerAddr, fileName)

	case "list":
		list(controllerAddr)

	case "nodes":
		nodes(controllerAddr)

	default:
		fmt.Printf("invalid action: %s\n", action)
	}
}
