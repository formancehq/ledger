package wal

import (
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Interceptor wraps a DefaultWAL and allows intercepting method calls.
// All interceptors are optional - if not set, the call passes through to the underlying DefaultWAL.
type Interceptor struct {
	delegate WAL
	mu       sync.RWMutex

	// Interceptors for raft.Storage methods
	OnInitialState func(delegate WAL) (raftpb.HardState, raftpb.ConfState, error)
	OnEntries      func(delegate WAL, lo, hi, maxSize uint64) ([]raftpb.Entry, error)
	OnTerm         func(delegate WAL, i uint64) (uint64, error)
	OnLastIndex    func(delegate WAL) (uint64, error)
	OnFirstIndex   func(delegate WAL) (uint64, error)
	OnSnapshot     func(delegate WAL) (raftpb.Snapshot, error)

	// Interceptors for DefaultWAL-specific methods
	OnCreateSnapshot          func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error
	OnUpdateSnapshotConfState func(delegate WAL, cs *raftpb.ConfState) error
	OnCompact                 func(delegate WAL, u uint64) error
	OnAppend                  func(delegate WAL, state raftpb.HardState, entries []raftpb.Entry) error
	OnApplySnapshot           func(delegate WAL, snapshot raftpb.Snapshot) error
	OnClose                   func(delegate WAL) error
}

// NewWALInterceptor creates a new Interceptor wrapping the given DefaultWAL.
func NewWALInterceptor(delegate WAL) *Interceptor {
	return &Interceptor{
		delegate: delegate,
	}
}

// Delegate returns the underlying DefaultWAL.
func (w *Interceptor) Delegate() WAL {
	return w.delegate
}

// InitialState implements raft.Storage.
func (w *Interceptor) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	w.mu.RLock()
	interceptor := w.OnInitialState
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.InitialState()
}

// Entries implements raft.Storage.
func (w *Interceptor) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	w.mu.RLock()
	interceptor := w.OnEntries
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, lo, hi, maxSize)
	}
	return w.delegate.Entries(lo, hi, maxSize)
}

// Term implements raft.Storage.
func (w *Interceptor) Term(i uint64) (uint64, error) {
	w.mu.RLock()
	interceptor := w.OnTerm
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, i)
	}
	return w.delegate.Term(i)
}

// LastIndex implements raft.Storage.
func (w *Interceptor) LastIndex() (uint64, error) {
	w.mu.RLock()
	interceptor := w.OnLastIndex
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.LastIndex()
}

// FirstIndex implements raft.Storage.
func (w *Interceptor) FirstIndex() (uint64, error) {
	w.mu.RLock()
	interceptor := w.OnFirstIndex
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.FirstIndex()
}

// Snapshot implements raft.Storage.
func (w *Interceptor) Snapshot() (raftpb.Snapshot, error) {
	w.mu.RLock()
	interceptor := w.OnSnapshot
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.Snapshot()
}

// CreateSnapshot implements DefaultWAL.
func (w *Interceptor) CreateSnapshot(i uint64, r *raftpb.ConfState, data []byte) error {
	w.mu.RLock()
	interceptor := w.OnCreateSnapshot
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, i, r, data)
	}
	return w.delegate.CreateSnapshot(i, r, data)
}

// UpdateSnapshotConfState implements WAL.
func (w *Interceptor) UpdateSnapshotConfState(cs *raftpb.ConfState) error {
	w.mu.RLock()
	interceptor := w.OnUpdateSnapshotConfState
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, cs)
	}
	return w.delegate.UpdateSnapshotConfState(cs)
}

// Compact implements DefaultWAL.
func (w *Interceptor) Compact(u uint64) error {
	w.mu.RLock()
	interceptor := w.OnCompact
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, u)
	}
	return w.delegate.Compact(u)
}

// Append implements DefaultWAL.
func (w *Interceptor) Append(state raftpb.HardState, entries []raftpb.Entry) error {
	w.mu.RLock()
	interceptor := w.OnAppend
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, state, entries)
	}
	return w.delegate.Append(state, entries)
}

// ApplySnapshot implements DefaultWAL.
func (w *Interceptor) ApplySnapshot(snapshot raftpb.Snapshot) error {
	w.mu.RLock()
	interceptor := w.OnApplySnapshot
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate, snapshot)
	}
	return w.delegate.ApplySnapshot(snapshot)
}

// Close implements DefaultWAL.
func (w *Interceptor) Close() error {
	w.mu.RLock()
	interceptor := w.OnClose
	w.mu.RUnlock()

	if interceptor != nil {
		return interceptor(w.delegate)
	}
	return w.delegate.Close()
}

// Setter methods for interceptors

func (w *Interceptor) SetInitialStateInterceptor(fn func(delegate WAL) (raftpb.HardState, raftpb.ConfState, error)) {
	w.mu.Lock()
	w.OnInitialState = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetEntriesInterceptor(fn func(delegate WAL, lo, hi, maxSize uint64) ([]raftpb.Entry, error)) {
	w.mu.Lock()
	w.OnEntries = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetTermInterceptor(fn func(delegate WAL, i uint64) (uint64, error)) {
	w.mu.Lock()
	w.OnTerm = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetLastIndexInterceptor(fn func(delegate WAL) (uint64, error)) {
	w.mu.Lock()
	w.OnLastIndex = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetFirstIndexInterceptor(fn func(delegate WAL) (uint64, error)) {
	w.mu.Lock()
	w.OnFirstIndex = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetSnapshotInterceptor(fn func(delegate WAL) (raftpb.Snapshot, error)) {
	w.mu.Lock()
	w.OnSnapshot = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetCreateSnapshotInterceptor(fn func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error) {
	w.mu.Lock()
	w.OnCreateSnapshot = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetUpdateSnapshotConfStateInterceptor(fn func(delegate WAL, cs *raftpb.ConfState) error) {
	w.mu.Lock()
	w.OnUpdateSnapshotConfState = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetCompactInterceptor(fn func(delegate WAL, u uint64) error) {
	w.mu.Lock()
	w.OnCompact = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetAppendInterceptor(fn func(delegate WAL, state raftpb.HardState, entries []raftpb.Entry) error) {
	w.mu.Lock()
	w.OnAppend = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetApplySnapshotInterceptor(fn func(delegate WAL, snapshot raftpb.Snapshot) error) {
	w.mu.Lock()
	w.OnApplySnapshot = fn
	w.mu.Unlock()
}

func (w *Interceptor) SetCloseInterceptor(fn func(delegate WAL) error) {
	w.mu.Lock()
	w.OnClose = fn
	w.mu.Unlock()
}

// ClearInterceptors removes all interceptors.
func (w *Interceptor) ClearInterceptors() {
	w.mu.Lock()
	w.OnInitialState = nil
	w.OnEntries = nil
	w.OnTerm = nil
	w.OnLastIndex = nil
	w.OnFirstIndex = nil
	w.OnSnapshot = nil
	w.OnCreateSnapshot = nil
	w.OnUpdateSnapshotConfState = nil
	w.OnCompact = nil
	w.OnAppend = nil
	w.OnApplySnapshot = nil
	w.OnClose = nil
	w.mu.Unlock()
}

// Ensure Interceptor implements DefaultWAL.
var _ WAL = (*Interceptor)(nil)
