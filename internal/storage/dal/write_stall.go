package dal

import "sync"

// WriteStallState tracks whether Pebble is currently in a write stall.
// It uses a channel-based pattern: a closed channel means "not stalled"
// (waiters proceed immediately), an open channel means "stalled" (waiters block).
// Thread-safe via sync.Mutex.
type WriteStallState struct {
	mu      sync.Mutex
	ch      chan struct{}
	stalled bool
}

// NewWriteStallState creates a WriteStallState initialized as "not stalled"
// (the channel is pre-closed so WaitCh returns immediately).
func NewWriteStallState() *WriteStallState {
	ch := make(chan struct{})
	close(ch)
	return &WriteStallState{ch: ch}
}

// OnStallBegin marks the state as stalled. Subsequent WaitCh callers will block
// until OnStallEnd is called.
func (s *WriteStallState) OnStallBegin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.stalled {
		s.stalled = true
		s.ch = make(chan struct{})
	}
}

// OnStallEnd marks the state as no longer stalled, unblocking all WaitCh waiters.
func (s *WriteStallState) OnStallEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stalled {
		s.stalled = false
		close(s.ch)
	}
}

// WaitCh returns a channel that blocks while a write stall is active.
// When not stalled, the returned channel is already closed (non-blocking).
func (s *WriteStallState) WaitCh() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ch
}

// IsStalled returns true if Pebble is currently in a write stall.
func (s *WriteStallState) IsStalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stalled
}
