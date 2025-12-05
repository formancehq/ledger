package raft

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Transport handles network communication between Raft nodes
type Transport struct {
	id       uint64
	addr     string
	listener net.Listener
	peers    map[uint64]string // peer ID -> address
	mu       sync.RWMutex

	// Channel for incoming messages
	recvCh chan raftpb.Message

	// Channels for outgoing messages per peer
	sendChs map[uint64]chan raftpb.Message

	// Channel for reporting unreachable peers
	unreachableCh chan uint64

	logger *zap.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewTransport creates a new transport
func NewTransport(id uint64, addr string, logger *zap.Logger) *Transport {
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{
		id:            id,
		addr:          addr,
		peers:         make(map[uint64]string),
		recvCh:        make(chan raftpb.Message, 100),
		sendChs:       make(map[uint64]chan raftpb.Message),
		unreachableCh: make(chan uint64, 100),
		logger:        logger,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start starts the transport server
func (t *Transport) Start() error {
	listener, err := net.Listen("tcp", t.addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", t.addr, err)
	}
	t.listener = listener

	// Start accepting connections
	go t.acceptLoop()

	return nil
}

// Stop stops the transport
func (t *Transport) Stop() {
	t.cancel()
	if t.listener != nil {
		t.listener.Close()
	}
	close(t.recvCh)
	close(t.unreachableCh)
	for _, ch := range t.sendChs {
		close(ch)
	}
}

// AddPeer adds a peer to the transport
func (t *Transport) AddPeer(id uint64, addr string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.peers[id] = addr
	if _, exists := t.sendChs[id]; !exists {
		t.sendChs[id] = make(chan raftpb.Message, 100)
		go t.sendLoop(id, addr)
	}
}

// RemovePeer removes a peer from the transport
func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.peers, id)
	if ch, exists := t.sendChs[id]; exists {
		close(ch)
		delete(t.sendChs, id)
	}
}

// Send sends a message to a peer
func (t *Transport) Send(msg raftpb.Message) {
	t.mu.RLock()
	ch, exists := t.sendChs[msg.To]
	t.mu.RUnlock()

	if exists {
		select {
		case ch <- msg:
		case <-t.ctx.Done():
		default:
			t.logger.Warn("Send channel full, dropping message", zap.Uint64("to", msg.To))
		}
	}
}

// Recv returns the channel for receiving messages
func (t *Transport) Recv() <-chan raftpb.Message {
	return t.recvCh
}

// Unreachable returns the channel for reporting unreachable peers
func (t *Transport) Unreachable() <-chan uint64 {
	return t.unreachableCh
}

// acceptLoop accepts incoming connections
func (t *Transport) acceptLoop() {
	for {
		conn, err := t.listener.Accept()
		if err != nil {
			select {
			case <-t.ctx.Done():
				return
			default:
				t.logger.Error("Failed to accept connection", zap.Error(err))
				continue
			}
		}

		go t.handleConnection(conn)
	}
}

// handleConnection handles a single connection
func (t *Transport) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		select {
		case <-t.ctx.Done():
			return
		default:
			var msg raftpb.Message
			if err := t.readMessage(conn, &msg); err != nil {
				if err != io.EOF {
					t.logger.Error("Failed to read message", zap.Error(err))
				}
				return
			}

			// Message already has From set by the sender
			select {
			case t.recvCh <- msg:
			case <-t.ctx.Done():
				return
			default:
				t.logger.Warn("Recv channel full, dropping message")
			}
		}
	}
}

// sendLoop sends messages to a peer
func (t *Transport) sendLoop(peerID uint64, addr string) {
	for {
		select {
		case <-t.ctx.Done():
			return
		case msg, ok := <-t.sendChs[peerID]:
			if !ok {
				return
			}

			// Connect to peer
			conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
			if err != nil {
				t.logger.Warn("Failed to connect to peer", zap.Uint64("peer", peerID), zap.String("addr", addr), zap.Error(err))
				// Report peer as unreachable
				select {
				case t.unreachableCh <- peerID:
				case <-t.ctx.Done():
					return
				default:
					// Channel full, skip
				}
				continue
			}

			// Send message
			if err := t.writeMessage(conn, msg); err != nil {
				t.logger.Warn("Failed to send message", zap.Uint64("peer", peerID), zap.Error(err))
				conn.Close()
				// Report peer as unreachable
				select {
				case t.unreachableCh <- peerID:
				case <-t.ctx.Done():
					return
				default:
					// Channel full, skip
				}
				continue
			}

			conn.Close()
		}
	}
}

// readMessage reads a message from a connection
func (t *Transport) readMessage(conn net.Conn, msg *raftpb.Message) error {
	// Read message size
	var size uint32
	if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
		return err
	}

	// Read message data
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return err
	}

	// Unmarshal message
	return msg.Unmarshal(data)
}

// writeMessage writes a message to a connection
func (t *Transport) writeMessage(conn net.Conn, msg raftpb.Message) error {
	// Marshal message
	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	// Write message size
	size := uint32(len(data))
	if err := binary.Write(conn, binary.BigEndian, size); err != nil {
		return err
	}

	// Write message data
	_, err = conn.Write(data)
	return err
}
