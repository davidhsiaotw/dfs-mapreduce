package main

import (
	"dfs/messages"
	"dfs/util"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"slices"
	"sync"
	"time"
)

const replicationFactor = 3

type nodeInfo struct {
	id              string
	address         string
	usedSpace       uint64
	freeSpace       uint64
	requestsHandled uint64
	lastHeartbeat   time.Time
}

type chunkInfo struct {
	chunkId  uint64
	nodes    []string
	size     uint32
	checksum []byte
}

type fileMetadata struct {
	fileName  string
	size      uint64
	chunks    []chunkInfo
	createdAt time.Time
	leaseExpiry time.Time
}

type controller struct {
	nodes      map[string]*nodeInfo
	nodesMutex sync.RWMutex

	files      map[string]*fileMetadata
	filesMutex sync.RWMutex

	pendingFiles  map[string]*fileMetadata
	pendingChunks map[string]uint64
	pendingMutex  sync.RWMutex

	walFile  *os.File
	walCount int
}

func newController() *controller {
	return &controller{
		nodes:         make(map[string]*nodeInfo),
		files:         make(map[string]*fileMetadata),
		pendingFiles:  make(map[string]*fileMetadata),
		pendingChunks: make(map[string]uint64),
	}
}

func (c *controller) registerNode(id string, address string) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	c.nodes[id] = &nodeInfo{
		id:            id,
		address:       address,
		lastHeartbeat: time.Now(),
	}
	log.Printf("registered node: %s at %s\n", id, address)
}

func (c *controller) updateHeartbeat(msgHandler *messages.MessageHandler, hb *messages.Heartbeat) {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	if node, ok := c.nodes[hb.Node.Id]; ok {
		node.usedSpace = hb.Node.UsedSpace
		node.freeSpace = hb.Node.FreeSpace
		node.requestsHandled = hb.Node.RequestsHandled
		node.lastHeartbeat = time.Now()
		msgHandler.SendResponse(true, "Heartbeat acknowledged")
	} else {
		log.Printf("received heartbeat from unknown node: %s. Please re-register.\n", hb.Node.Id)
		msgHandler.SendResponse(false, "Unknown node. Please re-register.")
	}
}

func (c *controller) monitorHeartbeats() {
	for {
		time.Sleep(5 * time.Second)
		c.nodesMutex.Lock()
		now := time.Now()
		for id, node := range c.nodes {
			if now.Sub(node.lastHeartbeat) > 15*time.Second {
				log.Printf("❌ node %s is DEAD (no heartbeat for 15s) - removing from cluster\n", id)
				delete(c.nodes, id)
			}
		}
		c.nodesMutex.Unlock()
	}
}

func (c *controller) monitorLeases() {
	for {
		time.Sleep(10 * time.Second)
		c.pendingMutex.Lock()
		now := time.Now()
		var expired []string
		var expiredFiles []*fileMetadata

		for name, fileMeta := range c.pendingFiles {
			if now.After(fileMeta.leaseExpiry) {
				expired = append(expired, name)
				expiredFiles = append(expiredFiles, fileMeta)
			}
		}

		for _, name := range expired {
			log.Printf("lease expired for pending file %s, cleaning up\n", name)
			delete(c.pendingFiles, name)
			delete(c.pendingChunks, name)
		}
		c.pendingMutex.Unlock()

		for _, fileMeta := range expiredFiles {
			uniqueNodes := make(map[string]struct{})
			var targetNodes []string
			for _, chunk := range fileMeta.chunks {
				for _, nodeAddr := range chunk.nodes {
					if _, ok := uniqueNodes[nodeAddr]; !ok {
						uniqueNodes[nodeAddr] = struct{}{}
						targetNodes = append(targetNodes, nodeAddr)
					}
				}
			}

			if len(targetNodes) > 0 {
				var wg sync.WaitGroup
				sem := make(chan struct{}, 50)

				for _, nodeAddr := range targetNodes {
					wg.Add(1)
					go func(addr string, fileName string) {
						defer wg.Done()
						sem <- struct{}{}
						defer func() { <-sem }()

						conn, err := net.Dial("tcp", addr)
						if err == nil {
							defer conn.Close()
							nodeHandler := messages.NewMessageHandler(conn)
							nodeHandler.SendDeleteFileRequest(fileName)
						}
					}(nodeAddr, fileMeta.fileName)
				}
				wg.Wait()
			}
		}
	}
}

// selectNodes ramdonly chooses n nodes for storing a chunk, preferring those with fewer stored chunks and active requests.
func (c *controller) selectNodes(n int, chunkSize uint32, excludedNodes []string) []string {
	c.nodesMutex.Lock()
	defer c.nodesMutex.Unlock()

	type nodeScore struct {
		id         string
		usedSpace  uint64
		activeReqs uint64
	}

	var candidates []nodeScore
	poolSize := n * 2
	for id, info := range c.nodes {
		if slices.Contains(excludedNodes, info.address) {
			continue
		}
		if info.freeSpace < uint64(chunkSize) {
			continue
		}
		candidates = append(candidates, nodeScore{id, info.usedSpace, info.requestsHandled})

		if len(candidates) >= poolSize {
			break
		}
	}

	slices.SortFunc(candidates, func(i, j nodeScore) int {
		if i.usedSpace < j.usedSpace {
			return -1
		} else if i.usedSpace > j.usedSpace {
			return 1
		}

		if i.activeReqs < j.activeReqs {
			return -1
		} else if i.activeReqs > j.activeReqs {
			return 1
		}

		return 0
	})

	var selectedAddresses []string
	for i := 0; i < n && i < len(candidates); i++ {
		nodeID := candidates[i].id
		selectedAddresses = append(selectedAddresses, c.nodes[nodeID].address)
	}

	return selectedAddresses
}

// handlePutFile calculates number of chunks and selects nodes for each chunk.
func (c *controller) handlePutFile(msgHandler *messages.MessageHandler, req *messages.PutFileRequest) {
	log.Printf("allocation request: %s (%d bytes)\n", req.Metadata.Name, req.Metadata.Size)

	c.filesMutex.RLock()
	_, exists := c.files[req.Metadata.Name]
	c.filesMutex.RUnlock()
	if exists {
		log.Printf("error: %s already exists\n", req.Metadata.Name)
		msgHandler.SendPutFileResponse(false, "File already exists", nil)
		return
	}

	c.pendingMutex.Lock()
	_, pendingExists := c.pendingFiles[req.Metadata.Name]
	c.pendingMutex.Unlock()
	if pendingExists {
		log.Printf("error: %s is currently being uploaded\n", req.Metadata.Name)
		msgHandler.SendPutFileResponse(false, "File is currently being uploaded", nil)
		return
	}

	if req.Chunk.Size == 0 {
		log.Println("error: chunk size can't be zero")
		msgHandler.SendPutFileResponse(false, "Chunk size must be greater than zero", nil)
		return
	}

	numChunks := req.NumChunks
	if numChunks == 0 {
		numChunks = (req.Metadata.Size + uint64(req.Chunk.Size) - 1) / uint64(req.Chunk.Size)
	}

	var allocations []*messages.ChunkAllocation
	var metadataChunks []chunkInfo

	for i := uint64(0); i < uint64(numChunks); i++ {
		targets := c.selectNodes(replicationFactor, req.Chunk.Size, nil)
		if len(targets) < replicationFactor {
			log.Printf("error: not enough nodes (need %d, have %d)\n", replicationFactor, len(targets))
			msgHandler.SendPutFileResponse(false, "Insufficient nodes for replication", nil)
			return
		}

		allocations = append(allocations, &messages.ChunkAllocation{
			Metadata: &messages.ChunkInfo{Id: i},
			Nodes:    util.MapArrayAddressesToNodeInfo(targets),
		})

		metadataChunks = append(metadataChunks, chunkInfo{
			chunkId: i,
			nodes:   []string{},
		})
	}

	c.pendingMutex.Lock()
	fileMeta := &fileMetadata{
		fileName:  req.Metadata.Name,
		size:      req.Metadata.Size,
		chunks:    metadataChunks,
		createdAt: time.Now(),
		leaseExpiry: time.Now().Add(30 * time.Second),
	}
	c.pendingFiles[req.Metadata.Name] = fileMeta
	c.pendingChunks[req.Metadata.Name] = numChunks
	c.pendingMutex.Unlock()

	msgHandler.SendPutFileResponse(true, "Allocation successful", allocations)
	log.Printf("allocated %d chunks for %s (pending upload)\n", numChunks, req.Metadata.Name)
}

// handleGetFile looks up file metadata and returns chunk locations to the client.
func (c *controller) handleGetFile(msgHandler *messages.MessageHandler, req *messages.GetFileRequest) {
	log.Printf("retrieval request: %s\n", req.Metadata.Name)

	c.filesMutex.RLock()
	fileMeta, exists := c.files[req.Metadata.Name]
	c.filesMutex.RUnlock()

	if !exists {
		log.Printf("error: %s not found\n", req.Metadata.Name)
		msgHandler.SendGetFileResponse(false, "File not found", nil, 0)
		return
	}

	var locations []*messages.ChunkLocation
	for _, chunk := range fileMeta.chunks {
		locations = append(locations, &messages.ChunkLocation{
			Metadata: &messages.ChunkInfo{Id: chunk.chunkId, Size: chunk.size, Checksum: chunk.checksum},
			Nodes:    util.MapArrayAddressesToNodeInfo(chunk.nodes),
		})
	}

	msgHandler.SendGetFileResponse(true, "File found", locations, fileMeta.size)
	log.Printf("returned %d chunk locations and size %d for %s\n", len(locations), fileMeta.size, req.Metadata.Name)
}

func (c *controller) handleLeaseRenew(msgHandler *messages.MessageHandler, req *messages.LeaseRenewRequest) {
	c.pendingMutex.Lock()
	defer c.pendingMutex.Unlock()

	if fileMeta, exists := c.pendingFiles[req.Metadata.Name]; exists {
		fileMeta.leaseExpiry = time.Now().Add(time.Minute)
		msgHandler.SendResponse(true, "Lease renewed")
		log.Printf("renewed lease for pending file %s\n", req.Metadata.Name)
	} else {
		msgHandler.SendResponse(false, "File is not pending")
	}
}

func (c *controller) handleChunkStatusReport(req *messages.ChunkStatusReport) {
	if req.Success {
		log.Printf("chunk status report received: %s, chunk %d, success, node: %s\n", req.Metadata.FileName, req.Metadata.Id, req.Node.Address)
	} else {
		log.Printf("chunk status report received: %s, chunk %d, failure, node: %s\n", req.Metadata.FileName, req.Metadata.Id, req.Node.Address)

		c.filesMutex.Lock()
		if fileMeta, exists := c.files[req.Metadata.FileName]; exists {
			for i := range fileMeta.chunks {
				chunk := &fileMeta.chunks[i]
				if chunk.chunkId == req.Metadata.Id {
					var aliveNodes []string
					for _, addr := range chunk.nodes {
						if addr != req.Node.Address {
							aliveNodes = append(aliveNodes, addr)
						}
					}
					chunk.nodes = aliveNodes
					log.Printf("removed corrupted node %s from chunk %d of %s (active)\n", req.Node.Address, req.Metadata.Id, req.Metadata.FileName)
					c.appendWalRecord("PUT", req.Metadata.FileName, fileMeta)
					break
				}
			}
		}
		c.filesMutex.Unlock()

		c.pendingMutex.Lock()
		if fileMeta, exists := c.pendingFiles[req.Metadata.FileName]; exists {
			for i := range fileMeta.chunks {
				chunk := &fileMeta.chunks[i]
				if chunk.chunkId == req.Metadata.Id {
					var aliveNodes []string
					for _, addr := range chunk.nodes {
						if addr != req.Node.Address {
							aliveNodes = append(aliveNodes, addr)
						}
					}
					chunk.nodes = aliveNodes
					log.Printf("removed corrupted node %s from chunk %d of %s (pending)\n", req.Node.Address, req.Metadata.Id, req.Metadata.FileName)
					break
				}
			}
		}
		c.pendingMutex.Unlock()
		return
	}

	c.pendingMutex.Lock()
	fileMeta, isPending := c.pendingFiles[req.Metadata.FileName]

	if !isPending {
		c.pendingMutex.Unlock()
		c.filesMutex.Lock()
		fileMeta, _ = c.files[req.Metadata.FileName]
		defer c.filesMutex.Unlock()
	} else {
		defer c.pendingMutex.Unlock()
	}

	if fileMeta == nil {
		return
	}

	isFirstCopy := false
	for i := range fileMeta.chunks {
		chunkInfo := &fileMeta.chunks[i]
		if chunkInfo.chunkId == req.Metadata.Id {
			found := slices.Contains(chunkInfo.nodes, req.Node.Address)
			if !found {
				if len(chunkInfo.nodes) == 0 {
					isFirstCopy = true
				}
				chunkInfo.nodes = append(chunkInfo.nodes, req.Node.Address)
				if !isPending {
					c.appendWalRecord("PUT", req.Metadata.FileName, fileMeta)
				}
			}
			log.Printf("adding %s to chunk %d's nodes list\n", req.Node.Address, req.Metadata.Id)
			log.Println(chunkInfo.nodes)
			break
		}
	}

	if isPending && isFirstCopy {
		c.pendingChunks[req.Metadata.FileName]--
		if c.pendingChunks[req.Metadata.FileName] == 0 {
			c.filesMutex.Lock()
			c.files[req.Metadata.FileName] = fileMeta
			c.appendWalRecord("PUT", req.Metadata.FileName, fileMeta)
			c.filesMutex.Unlock()

			delete(c.pendingFiles, req.Metadata.FileName)
			delete(c.pendingChunks, req.Metadata.FileName)

			log.Printf("%s is fully stored (first copies) and added to metadata\n", req.Metadata.FileName)
		}
	}
}

func (c *controller) handleReplicaNodes(msgHandler *messages.MessageHandler, req *messages.ReplicaNodesRequest) {
	log.Printf("replica nodes requested: %d nodes for %s, chunk %d\n", req.Count, req.Chunk.FileName, req.Chunk.Id)

	c.filesMutex.RLock()
	fileMeta, exists := c.files[req.Chunk.FileName]
	c.filesMutex.RUnlock()

	if !exists {
		c.pendingMutex.RLock()
		fileMeta, exists = c.pendingFiles[req.Chunk.FileName]
		c.pendingMutex.RUnlock()
	}

	var excluded = []string{}
	if exists {
		for _, chunk := range fileMeta.chunks {
			if chunk.chunkId == req.Chunk.Id {
				excluded = chunk.nodes
				break
			}
		}
	}

	targets := c.selectNodes(int(req.Count), uint32(req.Chunk.Size), excluded)

	if len(targets) > 0 {
		msgHandler.SendReplicaNodesResponse(true, "Nodes allocated", util.MapArrayAddressesToNodeInfo(targets))
	} else {
		msgHandler.SendReplicaNodesResponse(false, "Insufficient nodes", nil)
	}
}

// handleDeleteFile removes file metadata and sends delete commands to all nodes storing its chunks. At most 50 concurrent delete requests are sent to nodes.
func (c *controller) handleDeleteFile(msgHandler *messages.MessageHandler, req *messages.DeleteFileRequest) {
	log.Printf("deletion request: %s\n", req.Metadata.Name)

	c.filesMutex.Lock()
	fileMeta, exists := c.files[req.Metadata.Name]
	if !exists {
		c.filesMutex.Unlock()
		log.Printf("error: %s not found\n", req.Metadata.Name)
		msgHandler.SendResponse(false, "File not found")
		return
	}
	delete(c.files, req.Metadata.Name)
	c.appendWalRecord("DELETE", req.Metadata.Name, nil)
	c.filesMutex.Unlock()

	uniqueNodes := make(map[string]struct{})
	var targetNodes []string
	for _, chunk := range fileMeta.chunks {
		for _, nodeAddr := range chunk.nodes {
			if _, ok := uniqueNodes[nodeAddr]; !ok {
				uniqueNodes[nodeAddr] = struct{}{}
				targetNodes = append(targetNodes, nodeAddr)
			}
		}
	}

	if len(targetNodes) > 0 {
		var wg sync.WaitGroup
		sem := make(chan struct{}, 50)

		for _, nodeAddr := range targetNodes {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				log.Printf("sending delete request to node %s for %s\n", addr, req.Metadata.Name)

				conn, err := net.Dial("tcp", addr)
				if err != nil {
					log.Printf("failed to connect to node %s to delete file %s: %v\n", addr, req.Metadata.Name, err)
					return
				}
				defer conn.Close()

				nodeHandler := messages.NewMessageHandler(conn)
				err = nodeHandler.SendDeleteFileRequest(req.Metadata.Name)
				if err != nil {
					log.Printf("failed to send delete request to node %s: %v\n", addr, err)
					return
				}

				respWrapper, err := nodeHandler.Receive()
				if err != nil {
					log.Printf("failed to receive acknowledgment from node %s: %v\n", addr, err)
					return
				}

				if resp := respWrapper.GetResponse(); resp == nil || !resp.Ok {
					log.Printf("node %s rejected delete request for %s\n", addr, req.Metadata.Name)
					return
				}
			}(nodeAddr)
		}

		wg.Wait()
	}

	msgHandler.SendResponse(true, "File deleted successfully")
	log.Printf("deleted metadata for %s\n", req.Metadata.Name)
}

// handleNodeStats compiles stats from all nodes and sends them back to the client.
func (c *controller) handleNodeStats(msgHandler *messages.MessageHandler) {
	log.Println("node stats request received")

	var totalUsed uint64
	var totalFree uint64

	c.nodesMutex.RLock()
	var stats []*messages.NodeInfo
	for _, node := range c.nodes {
		stats = append(stats, &messages.NodeInfo{
			Id:              node.id,
			UsedSpace:       node.usedSpace,
			FreeSpace:       node.freeSpace,
			RequestsHandled: node.requestsHandled,
		})
		totalUsed += node.usedSpace
		totalFree += node.freeSpace
	}
	c.nodesMutex.RUnlock()

	err := msgHandler.SendNodeInfoResponse(true, "Node info retrieved", totalUsed, totalFree, stats)
	if err != nil {
		log.Printf("failed to send node stats response: %v\n", err)
	}
}

func (c *controller) handleListFiles(msgHandler *messages.MessageHandler) {
	log.Println("list files request received")

	c.filesMutex.RLock()
	var info []*messages.FileInfo
	for name, md := range c.files {
		info = append(info, &messages.FileInfo{Name: name, Size: md.size})
	}
	c.filesMutex.RUnlock()

	err := msgHandler.SendListFilesResponse(true, "File list retrieved", info)
	if err != nil {
		log.Printf("failed to send list files response: %v\n", err)
	}
}

func (c *controller) runMaintenance() {
	for {
		time.Sleep(30 * time.Second)
		c.processReplication()
	}
}

func (c *controller) processReplication() {
	c.nodesMutex.RLock()
	activeAddresses := make(map[string]bool)
	for _, node := range c.nodes {
		activeAddresses[node.address] = true
	}
	c.nodesMutex.RUnlock()

	// Safety check: if no nodes are registered, wait before declaring all replicas dead.
	// This prevents destroying metadata immediately after a controller restart if nodes haven't re-registered yet.
	if len(activeAddresses) == 0 {
		log.Println("maintenance: zero active nodes registered, skipping replication check to prevent false dead-node eviction")
		return
	}

	c.filesMutex.Lock()
	defer c.filesMutex.Unlock()
	for fileName, fileMeta := range c.files {
		modified := false
		for i := range fileMeta.chunks {
			chunk := &fileMeta.chunks[i]

			var aliveNodes []string
			for _, addr := range chunk.nodes {
				if activeAddresses[addr] {
					aliveNodes = append(aliveNodes, addr)
				}
			}

			if len(aliveNodes) != len(chunk.nodes) {
				log.Printf("maintenance: removed dead nodes from chunk %d of %s. nodes: %v -> %v\n", chunk.chunkId, fileName, chunk.nodes, aliveNodes)
				chunk.nodes = aliveNodes
				modified = true
			}

			aliveCount := len(chunk.nodes)
			if aliveCount > 0 && aliveCount < replicationFactor {
				needed := replicationFactor - aliveCount
				sourceNode := chunk.nodes[0]
				log.Printf("maintenance: chunk %d of %s is under-replicated (%d/%d). triggering replication from %s\n", chunk.chunkId, fileName, aliveCount, replicationFactor, sourceNode)
				go c.dispatchReplication(sourceNode, fileName, chunk.chunkId, uint32(needed))
			} else if aliveCount == 0 {
				log.Printf("CRITICAL: chunk %d of %s has NO replicas remaining!\n", chunk.chunkId, fileName)
			}
		}
		if modified {
			c.appendWalRecord("PUT", fileName, fileMeta)
		}
	}
	
}

func (c *controller) dispatchReplication(src string, fname string, cid uint64, count uint32) {
	conn, err := net.Dial("tcp", src)
	if err != nil {
		log.Printf("replication failed: could not connect to source node %s: %v\n", src, err)
		return
	}
	defer conn.Close()

	handler := messages.NewMessageHandler(conn)
	err = handler.SendDispatchReplicaTask(fname, cid, count)
	if err != nil {
		log.Printf("replication failed: could not send task to %s: %v\n", src, err)
	}
}

func (c *controller) handleConnection(conn net.Conn) {
	msgHandler := messages.NewMessageHandler(conn)
	defer msgHandler.Close()

	for {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			if err != io.EOF {
				log.Printf("connection error: %v\n", err)
			}
			return
		}

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_RegistrationReq:
			reg := msg.RegistrationReq
			c.registerNode(reg.Node.Id, reg.Node.Address)
			msgHandler.SendResponse(true, "Registration successful")

		case *messages.Wrapper_Heartbeat:
			c.updateHeartbeat(msgHandler, msg.Heartbeat)

		case *messages.Wrapper_PutFileReq:
			c.handlePutFile(msgHandler, msg.PutFileReq)

		case *messages.Wrapper_GetFileReq:
			c.handleGetFile(msgHandler, msg.GetFileReq)

		case *messages.Wrapper_LeaseRenewReq:
			c.handleLeaseRenew(msgHandler, msg.LeaseRenewReq)

		case *messages.Wrapper_ChunkStatusReport:
			c.handleChunkStatusReport(msg.ChunkStatusReport)

		case *messages.Wrapper_ReplicaNodesReq:
			c.handleReplicaNodes(msgHandler, msg.ReplicaNodesReq)

		case *messages.Wrapper_DeleteFileReq:
			c.handleDeleteFile(msgHandler, msg.DeleteFileReq)

		case *messages.Wrapper_ListFilesReq:
			c.handleListFiles(msgHandler)

		case *messages.Wrapper_StatsReq:
			c.handleNodeStats(msgHandler)

		default:
			log.Printf("received unhandled message type: %T\n", msg)
		}
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <port>\n", os.Args[0])
		os.Exit(1)
	}

	port := os.Args[1]
	controller := newController()

	controller.recoverWAL()
	controller.initWAL()

	go controller.monitorHeartbeats()
	go controller.monitorLeases()
	go controller.runMaintenance()

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer listener.Close()

	log.Printf("controller started on port %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("failed to accept connection: %v\n", err)
			continue
		}
		go controller.handleConnection(conn)
	}
}
