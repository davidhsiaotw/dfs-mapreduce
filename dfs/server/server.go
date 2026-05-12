package main

import (
	"crypto/md5"
	"dfs/messages"
	"dfs/util"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const baseChunkFilename = "chunk_"
const checksumFileExt = ".chksum"
const baseStorageDir = "/bigdata/students/whsiao5/mydfs"
const replicationFactor = 3

var nodeDir = baseStorageDir
var controllerAddr string
var myAddr string

var activeRequests uint64
var activeWrites sync.Map

func startHeartbeats(controllerAddr string, nodeID string, myPort string) {
	for {
		conn, err := net.Dial("tcp", controllerAddr)
		if err != nil {
			log.Printf("failed to connect to controller at %s, retrying in 5s...\n", controllerAddr)
			time.Sleep(5 * time.Second)
			continue
		}

		handler := messages.NewMessageHandler(conn)
		for {
			usedSpace := util.GetDirSize(nodeDir)
			freeSpace := util.GetFreeSpace(baseStorageDir)
			reqCount := uint64(atomic.LoadUint64(&activeRequests))

			err := handler.SendHeartbeat(nodeID, usedSpace, freeSpace, reqCount)
			if err != nil {
				log.Printf("failed to send heartbeat, reconnecting...\n")
				conn.Close()
				break
			}

			wrapper, err := handler.Receive()
			if err != nil {
				log.Printf("failed to read heartbeat response, reconnecting...\n")
				conn.Close()
				break
			}

			if resp := wrapper.GetResponse(); resp != nil {
				if !resp.Ok {
					log.Printf("controller rejected heartbeat: %s. Re-registering...\n", resp.Message)
					conn.Close()
					nodeID = registerWithController(controllerAddr, myPort)
					break // reconnect and start heartbeats again
				}
			}

			time.Sleep(5 * time.Second)
		}
	}
}

// registerWithController attempts to register this node with the controller and returns the assigned node ID. It will retry every 5 seconds until successful.
func registerWithController(controllerAddr string, myPort string) string {
	hostname, _ := os.Hostname()
	shortName := strings.Split(hostname, ".")[0]
	nodeID := fmt.Sprintf("%s:%s", shortName, myPort)
	myAddr = fmt.Sprintf("%s:%s", hostname, myPort)

	for {
		conn, err := net.Dial("tcp", controllerAddr)
		if err != nil {
			log.Printf("failed to connect to controller at %s, retrying in 5s...\n", controllerAddr)
			time.Sleep(5 * time.Second)
			continue
		}

		handler := messages.NewMessageHandler(conn)
		err = handler.SendRegistrationRequest(nodeID, myAddr)
		if err != nil {
			log.Printf("failed to send registration request: %v, retrying...\n", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		wrapper, err := handler.Receive()
		if err != nil {
			log.Printf("failed to receive registration response: %v, retrying...\n", err)
			conn.Close()
			time.Sleep(5 * time.Second)
			continue
		}

		if resp := wrapper.GetResponse(); resp != nil && resp.Ok {
			log.Printf("successfully registered with controller: %s\n", resp.Message)
			conn.Close()
			return nodeID
		}

		log.Println("failed to register with controller, retrying...")
		conn.Close()
		time.Sleep(5 * time.Second)
	}
}

// runScrubber periodically checks the integrity of all local chunks and repairs if necessary.
func runScrubber() {
	for {
		time.Sleep(60 * time.Second)
		log.Println("running background scrubber...")

		files, err := os.ReadDir(".")
		if err != nil {
			continue
		}

		for _, f := range files {
			if f.IsDir() {
				fileName := f.Name()
				chunkFiles, err := os.ReadDir(fileName)
				if err != nil {
					continue
				}
				for _, cf := range chunkFiles {
					if strings.HasPrefix(cf.Name(), baseChunkFilename) && !strings.HasSuffix(cf.Name(), checksumFileExt) {
						// log.Printf("verifying chunk %s of %s\n", cf.Name(), fileName)
						chunkIDStr := strings.TrimPrefix(cf.Name(), baseChunkFilename)
						chunkID, err := strconv.ParseUint(chunkIDStr, 10, 64)
						if err == nil {
							chunkPath := filepath.Join(fileName, cf.Name())
							if _, ok := activeWrites.Load(chunkPath); ok {
								continue
							}
							ok, _, _ := verifyChunkIntegrity(fileName, chunkID)
							if !ok {
								log.Printf("scrubber found corruption: chunk %d of %s\n", chunkID, fileName)
								go repairChunk(fileName, chunkID)
							}
						}
					}
				}
			}
		}
	}
}

// handleStoreChunk creates a new file and a metadata file for the incoming chunk, then writes it to disk.
func handleStoreChunk(msgHandler *messages.MessageHandler, req *messages.StoreChunkRequest) {
	atomic.AddUint64(&activeRequests, 1)
	defer atomic.AddUint64(&activeRequests, ^uint64(0))

	log.Printf("storing chunk %d of file %s (size: %d)\n", req.Metadata.Id, req.Metadata.FileName, req.Metadata.Size)
	baseFileName := filepath.Base(req.Metadata.FileName)

	reportStatus := func(success bool) {
		conn, err := net.Dial("tcp", controllerAddr)
		if err == nil {
			handler := messages.NewMessageHandler(conn)
			handler.SendChunkStatusReport(baseFileName, req.Metadata.Id, success, myAddr)
			conn.Close()
		} else {
			log.Printf("failed to report status to controller: %v\n", err)
		}
	}

	if util.GetFreeSpace(baseStorageDir) < uint64(req.Metadata.Size) {
		msgHandler.SendResponse(false, "Insufficient disk space")
		reportStatus(false)
		return
	}

	fileDir := filepath.Join(".", baseFileName)
	if err := os.MkdirAll(fileDir, 0755); err != nil {
		msgHandler.SendResponse(false, "Failed to create file directory: "+err.Error())
		reportStatus(false)
		return
	}
	chunkPath := filepath.Join(fileDir, fmt.Sprintf("%s%d", baseChunkFilename, req.Metadata.Id))
	activeWrites.Store(chunkPath, struct{}{})
	defer activeWrites.Delete(chunkPath)

	file, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		msgHandler.SendResponse(false, "Failed to create chunk file: "+err.Error())
		reportStatus(false)
		return
	}

	msgHandler.SendResponse(true, "Ready to receive chunk data")

	hash := md5.New()
	writer := io.MultiWriter(file, hash)

	_, err = io.CopyN(writer, msgHandler, int64(req.Metadata.Size))
	if err != nil {
		file.Close()
		os.Remove(chunkPath)
		log.Printf("error receiving chunk %d: %v\n", req.Metadata.Id, err)
		reportStatus(false)
		return
	}

	file.Sync()
	file.Close()

	computedChecksum := hash.Sum(nil)
	if !util.VerifyChecksum(computedChecksum, req.Metadata.Checksum) {
		os.Remove(chunkPath)
		msgHandler.SendResponse(false, "Checksum mismatch")
		reportStatus(false)
		return
	}

	err = os.WriteFile(chunkPath+checksumFileExt, computedChecksum, 0644)
	if err != nil {
		log.Printf("Warning: failed to save metadata for %s: %v\n", chunkPath, err)
		os.Remove(chunkPath)
		os.Remove(chunkPath + checksumFileExt)
		msgHandler.SendResponse(false, "Failed to save metadata")
		reportStatus(false)
		return
	}

	msgHandler.SendResponse(true, "Chunk stored successfully")
	log.Printf("successfully stored chunk %d of %s\n", req.Metadata.Id, baseFileName)

	reportStatus(true)

	if req.IsOriginal {
		go manageReplication(baseFileName, req.Metadata.Id, req.Metadata.Size, computedChecksum, chunkPath, replicationFactor-1)
	}
}

// manageReplication keeps trying to replicate the chunk by requesting nodes from the controller and passing data to them until a number of replicas are successfully stored.
func manageReplication(fileName string, chunkID uint64, size uint32, checksum []byte, chunkPath string, requiredReplicas uint32) {
	for requiredReplicas > 0 {
		conn, err := net.Dial("tcp", controllerAddr)
		if err != nil {
			log.Printf("replication: failed to connect to controller: %v, retrying...\n", err)
			time.Sleep(2 * time.Second)
			continue
		}
		handler := messages.NewMessageHandler(conn)
		err = handler.SendReplicaNodesRequest(fileName, chunkID, size, requiredReplicas)
		if err != nil {
			log.Printf("replication: failed to request replica nodes: %v\n", err)
			conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		wrapper, err := handler.Receive()
		if err != nil {
			log.Printf("replication: failed to receive replica nodes: %v\n", err)
			conn.Close()
			time.Sleep(2 * time.Second)
			continue
		}
		conn.Close()

		resp := wrapper.GetReplicaNodesResp()
		if resp == nil || resp.Resp == nil || !resp.Resp.Ok {
			log.Printf("replication: controller rejected replica request or no nodes available, retrying...\n")
			time.Sleep(5 * time.Second)
			continue
		}

		for _, node := range resp.Nodes {
			if replicate(fileName, chunkID, size, checksum, chunkPath, node.Address) {
				requiredReplicas--
				if requiredReplicas == 0 {
					break
				}
			}
		}
	}
	log.Printf("replication complete for chunk %d of %s\n", chunkID, fileName)
}

// replicate sends a chunk file to a node
func replicate(fileName string, chunkID uint64, size uint32, checksum []byte, chunkPath string, nodeAddr string) bool {
	var file *os.File
	var err error
	for range 3 {
		file, err = os.Open(chunkPath)
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Printf("replication to %s failed: could not open chunk file: %v\n", nodeAddr, err)
		return false
	}
	defer file.Close()

	conn, err := net.Dial("tcp", nodeAddr)
	if err != nil {
		log.Printf("replication to %s failed: could not connect: %v\n", nodeAddr, err)
		return false
	}
	defer conn.Close()

	handler := messages.NewMessageHandler(conn)
	err = handler.SendStoreChunkRequest(fileName, chunkID, size, checksum, false)
	if err != nil {
		log.Printf("replication to %s failed: could not send request: %v\n", nodeAddr, err)
		return false
	}

	respWrapper, err := handler.Receive()
	if err != nil || respWrapper.GetResponse() == nil || !respWrapper.GetResponse().Ok {
		log.Printf("replication to %s failed: node rejected request\n", nodeAddr)
		return false
	}

	_, err = io.CopyN(handler, file, int64(size))
	if err != nil {
		log.Printf("replication to %s failed: error sending data: %v\n", nodeAddr, err)
		return false
	}

	finalRespWrapper, err := handler.Receive()
	if err != nil || finalRespWrapper.GetResponse() == nil || !finalRespWrapper.GetResponse().Ok {
		log.Printf("replication to %s failed: node reported storage failure\n", nodeAddr)
		return false
	}

	log.Printf("replication succeeded for chunk %d to %s\n", chunkID, nodeAddr)
	return true
}

func handleDispatchReplicaTask(req *messages.DispatchReplicaTask) {
	atomic.AddUint64(&activeRequests, 1)
	defer atomic.AddUint64(&activeRequests, ^uint64(0))

	log.Printf("received dispatch replica task for chunk %d of %s (count: %d)\n", req.Metadata.Id, req.Metadata.FileName, req.Count)

	baseFileName := filepath.Base(req.Metadata.FileName)
	chunkPath := filepath.Join(baseFileName, fmt.Sprintf("%s%d", baseChunkFilename, req.Metadata.Id))

	file, err := os.Open(chunkPath)
	if err != nil {
		log.Printf("dispatch task failed: chunk not found locally: %v\n", err)
		return
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return
	}
	size := uint32(info.Size())
	file.Close()

	var checksum []byte
	metaData, err := os.ReadFile(chunkPath + checksumFileExt)
	if err == nil {
		checksum = metaData
	}

	log.Printf("managing replication for chunk %d of %s\n", req.Metadata.Id, baseFileName)
	go manageReplication(baseFileName, req.Metadata.Id, size, checksum, chunkPath, req.Count)
}

// handleRetrieveChunk reads the requested chunk from disk and sends it back to the client, along with its checksum.
func handleRetrieveChunk(msgHandler *messages.MessageHandler, req *messages.RetrieveChunkRequest) {
	atomic.AddUint64(&activeRequests, 1)
	defer atomic.AddUint64(&activeRequests, ^uint64(0))

	log.Printf("retrieving chunk %d of file %s\n", req.Metadata.Id, req.Metadata.FileName)

	baseFileName := filepath.Base(req.Metadata.FileName)
	chunkPath := filepath.Join(baseFileName, fmt.Sprintf("%s%d", baseChunkFilename, req.Metadata.Id))
	if _, ok := activeWrites.Load(chunkPath); ok {
		msgHandler.SendRetrieveChunkResponse(false, "Chunk is currently being written", 0, nil)
		return
	}

	ok, checksum, size := verifyChunkIntegrity(req.Metadata.FileName, req.Metadata.Id)
	if !ok {
		log.Printf("corruption detected during retrieval: chunk %d of %s\n", req.Metadata.Id, req.Metadata.FileName)
		msgHandler.SendRetrieveChunkResponse(false, "Chunk corrupted", 0, nil)
		go repairChunk(req.Metadata.FileName, req.Metadata.Id)
		return
	}

	file, err := os.Open(chunkPath)
	if err != nil {
		msgHandler.SendRetrieveChunkResponse(false, "Chunk not found: "+err.Error(), 0, nil)
		return
	}
	defer file.Close()

	msgHandler.SendRetrieveChunkResponse(true, "Ready to send chunk data", size, checksum)

	if _, err := io.CopyN(msgHandler, file, int64(size)); err != nil {
		log.Printf("error sending chunk %d: %v\n", req.Metadata.Id, err)
		return
	}

	log.Printf("successfully sent chunk %d of file %s\n", req.Metadata.Id, req.Metadata.FileName)
}

// verifyChunkIntegrity checks if the chunk on disk matches its metadata checksum.
func verifyChunkIntegrity(fileName string, chunkId uint64) (bool, []byte, uint32) {
	baseFileName := filepath.Base(fileName)
	chunkPath := filepath.Join(baseFileName, fmt.Sprintf("%s%d", baseChunkFilename, chunkId))

	file, err := os.Open(chunkPath)
	if err != nil {
		return false, nil, 0
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return false, nil, 0
	}

	metaData, err := os.ReadFile(chunkPath + checksumFileExt)
	if err != nil {
		return false, nil, 0
	}

	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return false, nil, 0
	}
	computedChecksum := hash.Sum(nil)

	if !util.VerifyChecksum(computedChecksum, metaData) {
		return false, computedChecksum, uint32(info.Size())
	}
	return true, computedChecksum, uint32(info.Size())
}

// repairChunk coordinates the discovery and recovery of a corrupted chunk.
func repairChunk(fileName string, chunkId uint64) {
	log.Printf("initiating repair for chunk %d of %s...\n", chunkId, fileName)

	conn, err := net.Dial("tcp", controllerAddr)
	if err != nil {
		log.Printf("repair failed: could not connect to controller: %v\n", err)
		return
	}
	controllerHandler := messages.NewMessageHandler(conn)
	controllerHandler.SendChunkStatusReport(fileName, chunkId, false, myAddr)

	err = controllerHandler.SendGetFileRequest(fileName)
	if err != nil {
		log.Printf("repair failed: could not send GET request: %v\n", err)
		conn.Close()
		return
	}

	wrapper, err := controllerHandler.Receive()
	if err != nil || wrapper.GetGetFileResp() == nil {
		log.Printf("repair failed: could not receive locations: %v\n", err)
		conn.Close()
		return
	}
	conn.Close()

	locations := wrapper.GetGetFileResp().Locations
	var healthyNodes []string
	for _, loc := range locations {
		if loc.Metadata.Id == chunkId {
			for _, node := range loc.Nodes {
				if node.Address != myAddr {
					healthyNodes = append(healthyNodes, node.Address)
				}
			}
			break
		}
	}

	if len(healthyNodes) == 0 {
		log.Printf("repair failed: no other healthy replicas found for chunk %d\n", chunkId)
		return
	}

	success := false
	for _, peerAddr := range healthyNodes {
		log.Printf("attempting to pull chunk %d from peer %s\n", chunkId, peerAddr)
		peerConn, err := net.Dial("tcp", peerAddr)
		if err != nil {
			continue
		}
		peerHandler := messages.NewMessageHandler(peerConn)
		peerHandler.SendRetrieveChunkRequest(fileName, chunkId)

		respWrapper, err := peerHandler.Receive()
		if err != nil || respWrapper.GetRetrieveChunkResp() == nil || !respWrapper.GetRetrieveChunkResp().Resp.Ok {
			peerConn.Close()
			continue
		}
		resp := respWrapper.GetRetrieveChunkResp()

		baseFileName := filepath.Base(fileName)
		chunkPath := filepath.Join(baseFileName, fmt.Sprintf("%s%d", baseChunkFilename, chunkId))
		activeWrites.Store(chunkPath, struct{}{})
		defer activeWrites.Delete(chunkPath)

		file, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			peerConn.Close()
			continue
		}

		hash := md5.New()
		writer := io.MultiWriter(file, hash)
		_, err = io.CopyN(writer, peerHandler, int64(resp.Metadata.Size))
		file.Sync()
		file.Close()
		peerConn.Close()

		if err == nil && util.VerifyChecksum(hash.Sum(nil), resp.Metadata.Checksum) {
			os.WriteFile(chunkPath+checksumFileExt, resp.Metadata.Checksum, 0644)
			success = true
			log.Printf("successfully repaired chunk %d of %s from %s\n", chunkId, fileName, peerAddr)
			break
		}
		os.Remove(chunkPath)
	}

	if success {
		conn, err = net.Dial("tcp", controllerAddr)
		if err == nil {
			handler := messages.NewMessageHandler(conn)
			handler.SendChunkStatusReport(fileName, chunkId, true, myAddr)
			conn.Close()
		}
	}
}

func handleDeleteFile(msgHandler *messages.MessageHandler, req *messages.DeleteFileRequest) {
	atomic.AddUint64(&activeRequests, 1)
	defer atomic.AddUint64(&activeRequests, ^uint64(0))

	log.Printf("deleting file %s\n", req.Metadata.Name)

	baseFileName := filepath.Base(req.Metadata.Name)
	err := os.RemoveAll(baseFileName)
	if err != nil {
		log.Printf("failed to delete file %s: %v\n", req.Metadata.Name, err)
		msgHandler.SendResponse(false, "Failed to delete file: "+err.Error())
		return
	}

	msgHandler.SendResponse(true, "File deleted successfully")
	log.Printf("successfully deleted file %s\n", req.Metadata.Name)
}

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()

	for {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			if err != io.EOF {
				log.Println("error receiving message:", err)
			}
			return
		}

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_StoreChunkReq:
			handleStoreChunk(msgHandler, msg.StoreChunkReq)
		case *messages.Wrapper_RetrieveChunkReq:
			handleRetrieveChunk(msgHandler, msg.RetrieveChunkReq)
		case *messages.Wrapper_DeleteFileReq:
			handleDeleteFile(msgHandler, msg.DeleteFileReq)
		case *messages.Wrapper_DispatchReplicaTask:
			handleDispatchReplicaTask(msg.DispatchReplicaTask)
		case nil:
			return
		default:
			errMsg := fmt.Sprintf("Unexpected message type: %T", msg)
			log.Println(errMsg)
			msgHandler.SendResponse(false, errMsg)
		}
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Printf("Usage: %s <port> <controller_addr> [storage-dir]\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]
	controllerAddr = os.Args[2]
	nodeDir = filepath.Join(baseStorageDir)
	if len(os.Args) >= 4 {
		nodeDir = os.Args[3]
	}

	if err := os.MkdirAll(nodeDir, 0755); err != nil {
		log.Fatalf("failed to create storage directory %s: %v\n", nodeDir, err)
	}
	if err := os.Chdir(nodeDir); err != nil {
		log.Fatalf("failed to change directory to %s: %v\n", nodeDir, err)
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen on port %s: %v\n", port, err)
	}
	defer listener.Close()

	log.Printf("storage node started on port %s, storage dir: %s\n", port, nodeDir)

	nodeID := registerWithController(controllerAddr, port)

	go startHeartbeats(controllerAddr, nodeID, port)
	go runScrubber()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v\n", err)
			continue
		}
		log.Printf("accepted connection from %s\n", conn.RemoteAddr())
		handler := messages.NewMessageHandler(conn)
		go handleClient(handler)
	}
}
