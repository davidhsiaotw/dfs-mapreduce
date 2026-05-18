package messages

import (
	"encoding/binary"
	"google.golang.org/protobuf/proto"
	"net"
)

type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	return &MessageHandler{conn: conn}
}

func (m *MessageHandler) ReadN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := m.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *MessageHandler) WriteN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := m.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Send(wrapper *MapReduceWrapper) error {
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

func (m *MessageHandler) Receive() (*MapReduceWrapper, error) {
	prefix := make([]byte, 8)
	if err := m.ReadN(prefix); err != nil {
		return nil, err
	}

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	if err := m.ReadN(payload); err != nil {
		return nil, err
	}

	wrapper := &MapReduceWrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}

// --- Convenience methods ---

func (m *MessageHandler) SendResponse(ok bool, msg string) error {
	return m.Send(&MapReduceWrapper{
		Msg: &MapReduceWrapper_Response{
			Response: &Response{Ok: ok, Message: msg},
		},
	})
}

func (m *MessageHandler) SendHeartbeat(id, address string, cpu uint32, mem uint32, active uint32) error {
	return m.Send(&MapReduceWrapper{
		Msg: &MapReduceWrapper_Heartbeat{
			Heartbeat: &Heartbeat{
				Worker: &WorkerInfo{
					Id:          id,
					Address:     address,
					CpuLoad:     cpu,
					MemLoad:     mem,
					ActiveTasks: active,
				},
			},
		},
	})
}
