package messages

import (
	"encoding/binary"
	"google.golang.org/protobuf/proto"
	"net"
	"time"
)

type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	m := &MessageHandler{
		conn: conn,
	}

	return m
}

func (m *MessageHandler) Read(p []byte) (n int, err error) {
	return m.conn.Read(p)
}

func (m *MessageHandler) ReadN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := m.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Write(p []byte) (n int, err error) {
	return m.conn.Write(p)
}

func (m *MessageHandler) WriteN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := m.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Send(wrapper *Wrapper) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	if err := m.WriteN(prefix); err != nil {
		return err
	}
	return m.WriteN(serialized)
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	if err := m.ReadN(prefix); err != nil {
		return nil, err
	}

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	if err := m.ReadN(payload); err != nil {
		return nil, err
	}

	wrapper := &Wrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}

func (m *MessageHandler) SetReadDeadline(t uint64, unit time.Duration) {
	if t == 0 {
		m.conn.SetReadDeadline(time.Time{})
	} else {
		m.conn.SetReadDeadline(time.Now().Add(time.Duration(t) * unit))
	}
}

func (m *MessageHandler) SetWriteDeadline(t uint64, unit time.Duration) {
	if t == 0 {
		m.conn.SetWriteDeadline(time.Time{})
	} else {
		m.conn.SetWriteDeadline(time.Now().Add(time.Duration(t) * unit))
	}
}

// --- Convenience methods ---

func (m *MessageHandler) SendResponse(ok bool, msg string) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_Response{
			Response: &Response{Ok: ok, Message: msg},
		},
	})
}

func (m *MessageHandler) SendDeleteFileRequest(fileName string) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_DeleteFileReq{
			DeleteFileReq: &DeleteFileRequest{
				Metadata: &FileInfo{Name: fileName},
			},
		},
	})
}

func (m *MessageHandler) SendPutFileRequest(fileName string, size uint64, chunkSize uint32, numChunks uint64) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_PutFileReq{
			PutFileReq: &PutFileRequest{
				Metadata:  &FileInfo{Name: fileName, Size: size},
				Chunk:     &ChunkInfo{Size: chunkSize},
				NumChunks: numChunks,
			},
		},
	})
}

func (m *MessageHandler) SendPutFileResponse(ok bool, msg string, allocations []*ChunkAllocation) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_PutFileResp{
			PutFileResp: &PutFileResponse{
				Resp:        &Response{Ok: ok, Message: msg},
				Allocations: allocations,
			},
		},
	})
}

func (m *MessageHandler) SendGetFileRequest(fileName string) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_GetFileReq{
			GetFileReq: &GetFileRequest{
				Metadata: &FileInfo{Name: fileName},
			},
		},
	})
}

func (m *MessageHandler) SendGetFileResponse(ok bool, msg string, locations []*ChunkLocation, size uint64) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_GetFileResp{
			GetFileResp: &GetFileResponse{
				Resp:      &Response{Ok: ok, Message: msg},
				Metadata:  &FileInfo{Size: size},
				Locations: locations,
			},
		},
	})
}

func (m *MessageHandler) SendLeaseRenewRequest(fileName string) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_LeaseRenewReq{
			LeaseRenewReq: &LeaseRenewRequest{
				Metadata: &FileInfo{Name: fileName},
			},
		},
	})
}

func (m *MessageHandler) SendListFilesRequest() error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_ListFilesReq{
			ListFilesReq: &ListFilesRequest{},
		},
	})
}

func (m *MessageHandler) SendListFilesResponse(ok bool, msg string, files []*FileInfo) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_ListFilesResp{
			ListFilesResp: &ListFilesResponse{
				Resp:  &Response{Ok: ok, Message: msg},
				Files: files,
			},
		},
	})
}

func (m *MessageHandler) SendNodeInfoRequest() error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_StatsReq{
			StatsReq: &NodeInfoRequest{},
		},
	})
}

func (m *MessageHandler) SendNodeInfoResponse(ok bool, msg string, totalUsed uint64, totalFree uint64, info []*NodeInfo) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_StatsResp{
			StatsResp: &NodeInfoResponse{
				Resp:      &Response{Ok: ok, Message: msg},
				TotalUsed: totalUsed,
				TotalFree: totalFree,
				NodeStats: info,
			},
		},
	})
}

func (m *MessageHandler) SendRegistrationRequest(nodeId string, address string) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_RegistrationReq{
			RegistrationReq: &RegistrationRequest{
				Node: &NodeInfo{Id: nodeId, Address: address},
			},
		},
	})
}

func (m *MessageHandler) SendHeartbeat(nodeId string, usedSpace uint64, freeSpace uint64, requestsHandled uint64) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_Heartbeat{
			Heartbeat: &Heartbeat{
				Node: &NodeInfo{Id: nodeId, UsedSpace: usedSpace, FreeSpace: freeSpace, RequestsHandled: requestsHandled},
			},
		},
	})
}

func (m *MessageHandler) SendChunkStatusReport(fileName string, chunkId uint64, success bool, nodeAddress string) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_ChunkStatusReport{
			ChunkStatusReport: &ChunkStatusReport{
				Metadata: &ChunkInfo{Id: chunkId, FileName: fileName},
				Success:  success,
				Node:     &NodeInfo{Address: nodeAddress},
			},
		},
	})
}

func (m *MessageHandler) SendReplicaNodesRequest(fileName string, chunkId uint64, size uint32, count uint32) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_ReplicaNodesReq{
			ReplicaNodesReq: &ReplicaNodesRequest{
				Chunk: &ChunkInfo{Id: chunkId, FileName: fileName, Size: size},
				Count:    count,
			},
		},
	})
}

func (m *MessageHandler) SendReplicaNodesResponse(ok bool, message string, nodes []*NodeInfo) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_ReplicaNodesResp{
			ReplicaNodesResp: &ReplicaNodesResponse{
				Resp:  &Response{Ok: ok, Message: message},
				Nodes: nodes,
			},
		},
	})
}

func (m *MessageHandler) SendDispatchReplicaTask(fileName string, chunkId uint64, count uint32) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_DispatchReplicaTask{
			DispatchReplicaTask: &DispatchReplicaTask{
				Metadata: &ChunkInfo{Id: chunkId, FileName: fileName},
				Count:    count,
			},
		},
	})
}

func (m *MessageHandler) SendStoreChunkRequest(fileName string, chunkId uint64, size uint32, checksum []byte, isOriginal bool) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_StoreChunkReq{
			StoreChunkReq: &StoreChunkRequest{
				Metadata:   &ChunkInfo{Id: chunkId, FileName: fileName, Size: size, Checksum: checksum},
				IsOriginal: isOriginal,
			},
		},
	})
}

func (m *MessageHandler) SendRetrieveChunkRequest(fileName string, chunkId uint64) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_RetrieveChunkReq{
			RetrieveChunkReq: &RetrieveChunkRequest{
				Metadata: &ChunkInfo{Id: chunkId, FileName: fileName},
			},
		},
	})
}

func (m *MessageHandler) SendRetrieveChunkResponse(ok bool, msg string, size uint32, checksum []byte) error {
	return m.Send(&Wrapper{
		Msg: &Wrapper_RetrieveChunkResp{
			RetrieveChunkResp: &RetrieveChunkResponse{
				Resp:     &Response{Ok: ok, Message: msg},
				Metadata: &ChunkInfo{Size: size, Checksum: checksum},
			},
		},
	})
}
