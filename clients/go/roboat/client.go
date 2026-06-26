// Package roboat provides a thin Go client for the roboatd local IPC socket.
//
// Frame format: [uint32 big-endian length][JSON bytes]
// Binary data is base64-encoded within JSON payloads.
//
// Usage:
//
//	rt, err := roboat.NewDaemon("/var/run/roboat/roboatd.sock")
//	stream, err := rt.Dial("127.0.0.1:11411", "control")
//	stream.Send([]byte("hello"))
//	data, err := stream.Recv()
package roboat

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

const defaultSocket = "/var/run/roboat/roboatd.sock"
const maxMsgSize = 4 * 1024 * 1024

// Daemon is a connection to a local roboatd IPC socket.
type Daemon struct {
	conn    net.Conn
	mu      sync.Mutex // guards writes
	streams sync.Map   // map[uint32]*Stream
	pending sync.Map   // map[string]chan result (dial request_id → result)
	incoming chan *Stream
	nextReqID atomic.Uint64
	closed    chan struct{}
}

type result struct {
	stream *Stream
	err    error
}

// Stream is a bidirectional data channel managed by the daemon.
type Stream struct {
	StreamID    uint32
	Class       string
	FromAgentID string
	daemon      *Daemon
	recvQueue   chan []byte
}

// NewDaemon connects to the roboatd IPC socket at socketPath.
// Pass an empty string to use the default path (/var/run/roboat/roboatd.sock).
func NewDaemon(socketPath string) (*Daemon, error) {
	if socketPath == "" {
		socketPath = defaultSocket
	}
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("roboat: connect to daemon: %w", err)
	}
	d := &Daemon{
		conn:     conn,
		incoming: make(chan *Stream, 64),
		closed:   make(chan struct{}),
	}
	go d.recvLoop()
	return d, nil
}

// Listen registers this process as a responder. The daemon will notify via
// Incoming() for each new inbound tunnel connection.
func (d *Daemon) Listen(agentID string, registryToken string) error {
	msg := map[string]any{"op": "listen", "agent_id": agentID}
	if registryToken != "" {
		msg["registry_token"] = registryToken
	}
	if err := d.writeMsg(msg); err != nil {
		return err
	}
	resp, err := d.readOnce()
	if err != nil {
		return err
	}
	if resp["op"] == "error" {
		return fmt.Errorf("roboat: listen: %v", resp["message"])
	}
	if resp["op"] != "listening" {
		return fmt.Errorf("roboat: unexpected response: %v", resp["op"])
	}
	return nil
}

// Incoming returns a channel on which new inbound streams arrive.
func (d *Daemon) Incoming(ctx context.Context) (<-chan *Stream, error) {
	return d.incoming, nil
}

// Dial connects to a remote agent. targetAgentID is "agt_xxx" or "host:port".
func (d *Daemon) Dial(targetAgentID string, streamClass string) (*Stream, error) {
	if streamClass == "" {
		streamClass = "control"
	}
	reqID := fmt.Sprintf("r%d", d.nextReqID.Add(1))
	ch := make(chan result, 1)
	d.pending.Store(reqID, ch)

	if err := d.writeMsg(map[string]any{
		"op":              "dial",
		"target_agent_id": targetAgentID,
		"stream_class":    streamClass,
		"request_id":      reqID,
	}); err != nil {
		d.pending.Delete(reqID)
		return nil, err
	}

	r := <-ch
	return r.stream, r.err
}

// Close shuts down the daemon connection.
func (d *Daemon) Close() error {
	select {
	case <-d.closed:
	default:
		close(d.closed)
	}
	return d.conn.Close()
}

// Send sends data on the stream.
func (s *Stream) Send(data []byte) error {
	return s.daemon.writeMsg(map[string]any{
		"op":        "send",
		"stream_id": s.StreamID,
		"data":      base64.StdEncoding.EncodeToString(data),
	})
}

// Recv waits for the next data chunk from the remote peer.
func (s *Stream) Recv() ([]byte, error) {
	data, ok := <-s.recvQueue
	if !ok {
		return nil, io.EOF
	}
	return data, nil
}

// Close closes the stream.
func (s *Stream) Close() error {
	return s.daemon.writeMsg(map[string]any{
		"op":        "close",
		"stream_id": s.StreamID,
	})
}

// ── Internal ──────────────────────────────────────────────────────────────────

func (d *Daemon) writeMsg(obj map[string]any) error {
	payload, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, err := d.conn.Write(header); err != nil {
		return err
	}
	_, err = d.conn.Write(payload)
	return err
}

func (d *Daemon) readMsg() (map[string]any, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(d.conn, header); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length > maxMsgSize {
		return nil, fmt.Errorf("roboat: message too large: %d", length)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(d.conn, buf); err != nil {
		return nil, err
	}
	var msg map[string]any
	return msg, json.Unmarshal(buf, &msg)
}

func (d *Daemon) readOnce() (map[string]any, error) {
	return d.readMsg()
}

func (d *Daemon) recvLoop() {
	for {
		msg, err := d.readMsg()
		if err != nil {
			return
		}
		d.dispatch(msg)
	}
}

func (d *Daemon) dispatch(msg map[string]any) {
	op, _ := msg["op"].(string)
	switch op {
	case "recv":
		sid := uint32(mustFloat(msg["stream_id"]))
		v, ok := d.streams.Load(sid)
		if !ok {
			return
		}
		s := v.(*Stream)
		raw, _ := base64.StdEncoding.DecodeString(mustString(msg["data"]))
		s.recvQueue <- raw

	case "incoming":
		sid := uint32(mustFloat(msg["stream_id"]))
		s := &Stream{
			StreamID:    sid,
			Class:       mustString(msg["class"]),
			FromAgentID: mustString(msg["from_agent_id"]),
			daemon:      d,
			recvQueue:   make(chan []byte, 64),
		}
		d.streams.Store(sid, s)
		d.incoming <- s

	case "connected":
		reqID := mustString(msg["request_id"])
		sid := uint32(mustFloat(msg["stream_id"]))
		s := &Stream{
			StreamID:    sid,
			Class:       "control",
			FromAgentID: mustString(msg["target_agent_id"]),
			daemon:      d,
			recvQueue:   make(chan []byte, 64),
		}
		d.streams.Store(sid, s)
		if v, ok := d.pending.LoadAndDelete(reqID); ok {
			v.(chan result) <- result{stream: s}
		}

	case "closed":
		sid := uint32(mustFloat(msg["stream_id"]))
		if v, ok := d.streams.LoadAndDelete(sid); ok {
			close(v.(*Stream).recvQueue)
		}

	case "error":
		reqID := mustString(msg["request_id"])
		if v, ok := d.pending.LoadAndDelete(reqID); ok {
			v.(chan result) <- result{err: fmt.Errorf("%v", msg["message"])}
		}
	}
}

func mustFloat(v any) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return 0
}

func mustString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
