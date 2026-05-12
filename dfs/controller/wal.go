package main

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"time"
)

const walPath = "/bigdata/students/whsiao5/controller_metadata.wal"

type WalChunkInfo struct {
	ChunkId  uint64   `json:"chunk_id"`
	Nodes    []string `json:"nodes"`
	Size     uint32   `json:"size"`
	Checksum []byte   `json:"checksum"`
}

type WalFileMetadata struct {
	FileName    string         `json:"file_name"`
	Size        uint64         `json:"size"`
	Chunks      []WalChunkInfo `json:"chunks"`
	CreatedAt   time.Time      `json:"created_at"`
	LeaseExpiry time.Time      `json:"lease_expiry,omitempty"`
}

type WalRecord struct {
	Op       string           `json:"op"`
	FileName string           `json:"file_name"`
	FileMeta *WalFileMetadata `json:"file_meta,omitempty"`
}

// toWalFileMeta converts internal struct to WAL struct
func toWalFileMeta(meta *fileMetadata) *WalFileMetadata {
	if meta == nil {
		return nil
	}
	walChunks := make([]WalChunkInfo, len(meta.chunks))
	for i, c := range meta.chunks {
		walChunks[i] = WalChunkInfo{
			ChunkId:  c.chunkId,
			Nodes:    c.nodes,
			Size:     c.size,
			Checksum: c.checksum,
		}
	}
	return &WalFileMetadata{
		FileName:    meta.fileName,
		Size:        meta.size,
		Chunks:      walChunks,
		CreatedAt:   meta.createdAt,
		LeaseExpiry: meta.leaseExpiry,
	}
}

// fromWalFileMeta converts WAL struct back to internal struct
func fromWalFileMeta(walMeta *WalFileMetadata) *fileMetadata {
	if walMeta == nil {
		return nil
	}
	chunks := make([]chunkInfo, len(walMeta.Chunks))
	for i, wc := range walMeta.Chunks {
		chunks[i] = chunkInfo{
			chunkId:  wc.ChunkId,
			nodes:    wc.Nodes,
			size:     wc.Size,
			checksum: wc.Checksum,
		}
	}
	return &fileMetadata{
		fileName:    walMeta.FileName,
		size:        walMeta.Size,
		chunks:      chunks,
		createdAt:   walMeta.CreatedAt,
		leaseExpiry: walMeta.LeaseExpiry,
	}
}

// initWAL opens the WAL file for appending
func (c *controller) initWAL() {
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to open WAL file: %v", err)
	}
	c.walFile = file
}

// recoverWAL reads the WAL file to rebuild the files map.
func (c *controller) recoverWAL() {
	file, err := os.Open(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		log.Fatalf("failed to open WAL file for recovery: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024) // 10MB maximum line size
	c.filesMutex.Lock()
	defer c.filesMutex.Unlock()

	for scanner.Scan() {
		line := scanner.Text()
		var record WalRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			log.Printf("failed to unmarshal WAL record: %v\n", err)
			continue
		}

		c.walCount++

		switch record.Op {
		case "PUT":
			if record.FileMeta != nil {
				c.files[record.FileMeta.FileName] = fromWalFileMeta(record.FileMeta)
			}
		case "DELETE":
			delete(c.files, record.FileName)
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("error reading WAL file: %v\n", err)
	}

	log.Printf("recovered %d files from WAL (%d records)\n", len(c.files), c.walCount)
}

// appendWalRecord logs a mutation. Caller MUST hold c.filesMutex.Lock().
func (c *controller) appendWalRecord(op string, fileName string, fileMeta *fileMetadata) {
	if c.walFile == nil {
		return
	}

	record := WalRecord{
		Op:       op,
		FileName: fileName,
		FileMeta: toWalFileMeta(fileMeta),
	}

	data, err := json.Marshal(record)
	if err != nil {
		log.Printf("failed to marshal WAL record: %v\n", err)
		return
	}

	data = append(data, '\n')
	if _, err := c.walFile.Write(data); err != nil {
		log.Printf("failed to write to WAL: %v\n", err)
		return
	}
	c.walFile.Sync()

	c.walCount++
	if c.walCount >= 1000 {
		c.compactWAL()
	}
}

// compactWAL squashes the WAL. Caller MUST hold c.filesMutex.Lock().
func (c *controller) compactWAL() {
	log.Println("starting WAL compaction...")
	tmpPath := walPath + ".tmp"
	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Printf("failed to create temporary WAL file for compaction: %v\n", err)
		return
	}

	var records [][]byte
	for _, meta := range c.files {
		record := WalRecord{
			Op:       "PUT",
			FileName: meta.fileName,
			FileMeta: toWalFileMeta(meta),
		}
		data, err := json.Marshal(record)
		if err == nil {
			records = append(records, append(data, '\n'))
		}
	}

	for _, data := range records {
		if _, err := tmpFile.Write(data); err != nil {
			log.Printf("failed to write to temporary WAL: %v\n", err)
			tmpFile.Close()
			return
		}
	}
	tmpFile.Sync()
	tmpFile.Close()

	if err := os.Rename(tmpPath, walPath); err != nil {
		log.Printf("failed to rename temporary WAL: %v\n", err)
		return
	}

	c.walFile.Close()
	file, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed to re-open WAL file after compaction: %v", err)
	}
	c.walFile = file
	c.walCount = len(c.files)
	log.Println("WAL compaction complete")
}