package wasm

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/tetratelabs/wazero/api"
)

type network struct {
	mu        sync.Mutex
	nextID    uint32
	clients   map[uint32]net.Conn
	requests  map[uint32]*asyncRequest
	requestID uint32
	workers   chan struct{}
	log       Logger
	active    atomic.Int64
	max       atomic.Int64
	count     atomic.Uint64
}

const asyncRequestMask = uint32(1 << 31)

type asyncRequest struct {
	mu       sync.Mutex
	ready    bool
	response []byte
	err      error
}

func newNetwork(log Logger) *network {
	return &network{nextID: 1, clients: make(map[uint32]net.Conn), requests: make(map[uint32]*asyncRequest), requestID: 1, workers: make(chan struct{}, 8), log: log}
}

func (n *network) closeAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for id, conn := range n.clients {
		_ = conn.Close()
		delete(n.clients, id)
	}
}

// request submits one opaque request and returns a handle immediately. The
// request is executed by a bounded host worker and can be polled/read later.
func (n *network) request(ctx context.Context, module api.Module, hostPtr, hostLen, port, requestPtr, requestLen uint32) uint32 {
	hostBytes, ok := module.Memory().Read(hostPtr, hostLen)
	if !ok {
		return ^uint32(0)
	}
	requestBytes, ok := module.Memory().Read(requestPtr, requestLen)
	if !ok {
		return ^uint32(0)
	}
	state := &asyncRequest{}
	n.mu.Lock()
	id := asyncRequestMask | n.requestID
	n.requestID++
	n.requests[id] = state
	n.mu.Unlock()
	host := string(hostBytes)
	payload := append([]byte(nil), requestBytes...)
	go n.runRequest(ctx, id, state, host, port, payload)
	return id
}

func (n *network) runRequest(ctx context.Context, id uint32, state *asyncRequest, host string, port uint32, payload []byte) {
	n.workers <- struct{}{}
	defer func() { <-n.workers }()
	finish := n.begin("request")
	defer finish()
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.FormatUint(uint64(port), 10)))
	if err == nil {
		_, err = contextConnWrite(ctx, conn, payload)
	}
	var response []byte
	if err == nil {
		response, err = contextReadAll(ctx, conn)
	}
	if conn != nil {
		_ = conn.Close()
	}
	state.mu.Lock()
	state.response = response
	state.err = err
	state.ready = true
	state.mu.Unlock()
	_ = id
}

// pollRequest returns 0 while pending, 1 when ready, and ^uint32(0) on error.
func (n *network) pollRequest(_ context.Context, _ api.Module, id uint32) uint32 {
	state, ok := n.getRequest(id)
	if !ok {
		return ^uint32(0)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.ready {
		return 0
	}
	if state.err != nil {
		return ^uint32(0)
	}
	return 1
}

func (n *network) readRequest(_ context.Context, module api.Module, id, bufferPtr, bufferCap uint32) uint32 {
	state, ok := n.getRequest(id)
	if !ok {
		return ^uint32(0)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.ready || state.err != nil || uint32(len(state.response)) > bufferCap {
		return ^uint32(0)
	}
	if !module.Memory().Write(bufferPtr, state.response) {
		return ^uint32(0)
	}
	return uint32(len(state.response))
}

func (n *network) responseSize(_ context.Context, _ api.Module, id uint32) uint32 {
	state, ok := n.getRequest(id)
	if !ok {
		return ^uint32(0)
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.ready || state.err != nil || uint64(len(state.response)) > uint64(^uint32(0)) {
		return ^uint32(0)
	}
	return uint32(len(state.response))
}

func (n *network) getRequest(id uint32) (*asyncRequest, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	state, ok := n.requests[id]
	return state, ok
}

// connect(host_ptr, host_len, port) -> connection handle, or ^uint32(0).
func (n *network) connect(ctx context.Context, module api.Module, hostPtr, hostLen, port uint32) uint32 {
	hostBytes, ok := module.Memory().Read(hostPtr, hostLen)
	if !ok {
		return ^uint32(0)
	}
	finish := n.begin("connect")
	defer finish()
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
	finish := n.begin("read")
	defer finish()
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
	finish := n.begin("write")
	defer finish()
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

func contextReadAll(ctx context.Context, conn net.Conn) ([]byte, error) {
	type result struct {
		data []byte
		err  error
	}
	done := make(chan result, 1)
	go func() {
		data, err := io.ReadAll(conn)
		done <- result{data: data, err: err}
	}()
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, ctx.Err()
	case result := <-done:
		return result.data, result.err
	}
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

func (n *network) begin(operation string) func() {
	active := n.active.Add(1)
	for {
		max := n.max.Load()
		if active <= max || n.max.CompareAndSwap(max, active) {
			break
		}
	}
	if active > 1 && n.log != nil {
		n.log(1, fmt.Sprintf("wasm network overlap: operation=%s active=%d max_active=%d", operation, active, n.max.Load()))
	}
	return func() {
		current := n.active.Add(-1)
		count := n.count.Add(1)
		if n.log != nil && count%100 == 0 {
			n.log(1, fmt.Sprintf("wasm network: operation=%s operations=%d active=%d max_active=%d", operation, count, current, n.max.Load()))
		}
	}
}

// close(handle) -> zero on success, or ^uint32(0).
func (n *network) close(_ context.Context, _ api.Module, id uint32) uint32 {
	if id&asyncRequestMask != 0 {
		n.mu.Lock()
		_, ok := n.requests[id]
		if ok {
			delete(n.requests, id)
		}
		n.mu.Unlock()
		if !ok {
			return ^uint32(0)
		}
		return 0
	}
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
