package raft

import (
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// WALInterceptor wraps a WAL and allows intercepting method calls.
// All interceptors are optional - if not set, the call passes through to the underlying WAL.
type WALInterceptor struct {
	delegate WAL
	mu       sync.RWMutex

	// Interceptors for raft.Storage methods
	OnInitialState func(delegate WAL) (raftpb.HardState, raftpb.ConfState, error)
	OnEntries      func(delegate WAL, lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	OnTerm         func(delegate WAL, i uint64) (uint64, error)
	OnLastIndex    func(delegate WAL) (uint64, error)
	OnFirstIndex   func(delegate WAL) (uint64, error)
	OnSnapshot     func(delegate WAL) (raftpb.Snapshot, error)

	// Interceptors for WAL-specific methods
	OnCreateSnapshot func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error
	OnCompact        func(delegate WAL, u uint64) error
	OnAppend         func(delegate WAL, state raftpb.HardState, entries []raftpb.Entry) error
	OnApplySnapshot  func(delegate WAL, snapshot raftpb.Snapshot) error
	OnClose          func(delegate WAL) error
}

// NewWALInterceptor creates a new WALInterceptor wrapping the given WAL.
func NewWALInterceptor(delegate WAL) *WALInterceptor {
	return &WALInterceptor{
		delegate: delegate,
	}
}

// Delegate returns the underlying WAL.
func (w *WALInterceptor) Delegate() WAL {
	return w.delegate
}

// InitialState implements raft.Storage.
func (w *WALInterceptor) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	w.mu.RLock()
	interceptor := w.OnInitialState
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.InitialState()
}

// Entries implements raft.Storage.
func (w *WALInterceptor) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	w.mu.RLock()
	interceptor := w.OnEntries
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, lo, hi, maxSize)
	}
	return w.delegate.Entries(lo, hi, maxSize)
}

// Term implements raft.Storage.
func (w *WALInterceptor) Term(i uint64) (uint64, error) {
	w.mu.RLock()
	interceptor := w.OnTerm
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, i)
	}
	return w.delegate.Term(i)
}

// LastIndex implements raft.Storage.
func (w *WALInterceptor) LastIndex() (uint64, error) {
	w.mu.RLock()
	interceptor := w.OnLastIndex
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.LastIndex()
}

// FirstIndex implements raft.Storage.
func (w *WALInterceptor) FirstIndex() (uint64, error) {
	w.mu.RLock()
	interceptor := w.OnFirstIndex
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.FirstIndex()
}

// Snapshot implements raft.Storage.
func (w *WALInterceptor) Snapshot() (raftpb.Snapshot, error) {
	w.mu.RLock()
	interceptor := w.OnSnapshot
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.Snapshot()
}

// CreateSnapshot implements WAL.
func (w *WALInterceptor) CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error {
	w.mu.RLock()
	interceptor := w.OnCreateSnapshot
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, i, r, data)
	}
	return w.delegate.CreateSnapshot(i, r, data)
}

// Compact implements WAL.
func (w *WALInterceptor) Compact(u uint64) error {
	w.mu.RLock()
	interceptor := w.OnCompact
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, u)
	}
	return w.delegate.Compact(u)
}

// Append implements WAL.
func (w *WALInterceptor) Append(state raftpb.HardState, entries []raftpb.Entry) error {
	w.mu.RLock()
	interceptor := w.OnAppend
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, state, entries)
	}
	return w.delegate.Append(state, entries)
}

// ApplySnapshot implements WAL.
func (w *WALInterceptor) ApplySnapshot(snapshot raftpb.Snapshot) error {
	w.mu.RLock()
	interceptor := w.OnApplySnapshot
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, snapshot)
	}
	return w.delegate.ApplySnapshot(snapshot)
}

// Close implements WAL.
func (w *WALInterceptor) Close() error {
	w.mu.RLock()
	interceptor := w.OnClose
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.Close()
}

// Setter methods for interceptors

func (w *WALInterceptor) SetInitialStateInterceptor(fn func(delegate WAL) (raftpb.HardState, raftpb.ConfState, error)) {
	w.mu.Lock()
	w.OnInitialState = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetEntriesInterceptor(fn func(delegate WAL, lo, hi, maxSize uint64) ([]raftpb.Entry, error)) {
	w.mu.Lock()
	w.OnEntries = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetTermInterceptor(fn func(delegate WAL, i uint64) (uint64, error)) {
	w.mu.Lock()
	w.OnTerm = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetLastIndexInterceptor(fn func(delegate WAL) (uint64, error)) {
	w.mu.Lock()
	w.OnLastIndex = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetFirstIndexInterceptor(fn func(delegate WAL) (uint64, error)) {
	w.mu.Lock()
	w.OnFirstIndex = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetSnapshotInterceptor(fn func(delegate WAL) (raftpb.Snapshot, error)) {
	w.mu.Lock()
	w.OnSnapshot = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetCreateSnapshotInterceptor(fn func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error) {
	w.mu.Lock()
	w.OnCreateSnapshot = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetCompactInterceptor(fn func(delegate WAL, u uint64) error) {
	w.mu.Lock()
	w.OnCompact = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetAppendInterceptor(fn func(delegate WAL, state raftpb.HardState, entries []raftpb.Entry) error) {
	w.mu.Lock()
	w.OnAppend = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetApplySnapshotInterceptor(fn func(delegate WAL, snapshot raftpb.Snapshot) error) {
	w.mu.Lock()
	w.OnApplySnapshot = fn
	w.mu.Unlock()
}

func (w *WALInterceptor) SetCloseInterceptor(fn func(delegate WAL) error) {
	w.mu.Lock()
	w.OnClose = fn
	w.mu.Unlock()
}

// ClearInterceptors removes all interceptors.
func (w *WALInterceptor) ClearInterceptors() {
	w.mu.Lock()
	w.OnInitialState = nil
	w.OnEntries = nil
	w.OnTerm = nil
	w.OnLastIndex = nil
	w.OnFirstIndex = nil
	w.OnSnapshot = nil
	w.OnCreateSnapshot = nil
	w.OnCompact = nil
	w.OnAppend = nil
	w.OnApplySnapshot = nil
	w.OnClose = nil
	w.mu.Unlock()
}

// Ensure WALInterceptor implements WAL.
var _ WAL = (*WALInterceptor)(nil)
