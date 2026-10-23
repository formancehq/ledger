package wasm

import (
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"

	"github.com/tetratelabs/wazero/api"
)

type network struct {
	mu      sync.Mutex
	nextID  uint32
	clients map[uint32]net.Conn
}

func newNetwork() *network {
	return &network{nextID: 1, clients: make(map[uint32]net.Conn)}
}

func (n *network) closeAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, conn := range n.clients {
		_ = conn.Close()
		delete(n.clients, id)
	}
}

// connect(host_ptr, host_len, port) -> connection handle, or ^uint32(0).
func (n *network) connect(ctx context.Context, module api.Module, hostPtr, hostLen, port uint32) uint32 {
	hostBytes, ok := module.Memory().Read(hostPtr, hostLen)
	if !ok {
		return ^uint32(0)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(string(hostBytes), strconv.FormatUint(uint64(port), 10)))
	if err != nil {
		return ^uint32(0)
	}

	n.mu.Lock()
	id := n.nextID
	n.nextID++
	n.clients[id] = conn
	n.mu.Unlock()

	return id
}

// read(handle, buffer_ptr, buffer_cap) -> bytes read, or ^uint32(0).
func (n *network) read(ctx context.Context, module api.Module, id, bufferPtr, bufferCap uint32) uint32 {
	conn, ok := n.get(id)
	if !ok {
		return ^uint32(0)
	}
	buffer, ok := module.Memory().Read(bufferPtr, bufferCap)
	if !ok {
		return ^uint32(0)
	}
	count, err := contextConnRead(ctx, conn, buffer)
	if err != nil && count == 0 && !errors.Is(err, io.EOF) {
		return ^uint32(0)
	}

	return uint32(count)
}

// write(handle, buffer_ptr, buffer_len) -> bytes written, or ^uint32(0).
func (n *network) write(ctx context.Context, module api.Module, id, bufferPtr, bufferLen uint32) uint32 {
	conn, ok := n.get(id)
	if !ok {
		return ^uint32(0)
	}
	buffer, ok := module.Memory().Read(bufferPtr, bufferLen)
	if !ok {
		return ^uint32(0)
	}
	count, err := contextConnWrite(ctx, conn, buffer)
	if err != nil && count == 0 {
		return ^uint32(0)
	}

	return uint32(count)
}

func contextConnRead(ctx context.Context, conn net.Conn, buffer []byte) (int, error) {
	return contextConnIO(ctx, conn, func() (int, error) { return conn.Read(buffer) })
}

func contextConnWrite(ctx context.Context, conn net.Conn, buffer []byte) (int, error) {
	return contextConnIO(ctx, conn, func() (int, error) { return conn.Write(buffer) })
}

func contextConnIO(ctx context.Context, conn net.Conn, operation func() (int, error)) (int, error) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	count, err := operation()
	close(done)

	return count, err
}

// close(handle) -> zero on success, or ^uint32(0).
func (n *network) close(_ context.Context, _ api.Module, id uint32) uint32 {
	n.mu.Lock()
	conn, ok := n.clients[id]
	if ok {
		delete(n.clients, id)
	}
	n.mu.Unlock()
	if !ok {
		return ^uint32(0)
	}
	if err := conn.Close(); err != nil {
		return ^uint32(0)
	}

	return 0
}

func (n *network) get(id uint32) (net.Conn, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	conn, ok := n.clients[id]

	return conn, ok
}
