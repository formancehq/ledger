package spool

import (
	"context"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Interceptor wraps a Spool and allows intercepting method calls.
// All interceptors are optional - if not set, the call passes through to the underlying Spool.
type Interceptor struct {
	delegate Spool
	mu       sync.RWMutex

	// Interceptors - set these to intercept specific method calls
	// Each interceptor receives the original arguments and the delegate.
	// Return values from interceptors replace the original return values.

	OnAppendCommittedEntries func(
		ctx context.Context,
		delegate Spool,
		entries []raftpb.Entry,
	) error

	OnEnd func(
		delegate Spool,
	) (*Position, error)

	OnReplayUntil func(
		ctx context.Context,
		delegate Spool,
		end Position,
		lastApplied uint64,
		applyFn func(raftpb.Entry) error,
	) error

	OnPrune func(
		delegate Spool,
		lastApplied uint64,
	) error

	OnClose func(delegate Spool) error
}

// NewInterceptor creates a new Interceptor wrapping the given Spool.
func NewInterceptor(delegate Spool) *Interceptor {
	return &Interceptor{
		delegate: delegate,
	}
}

// Delegate returns the underlying Spool.
func (s *Interceptor) Delegate() Spool {
	return s.delegate
}

// AppendCommittedEntries implements Spool.
func (s *Interceptor) AppendCommittedEntries(ctx context.Context, entries ...raftpb.Entry) error {
	s.mu.RLock()
	interceptor := s.OnAppendCommittedEntries
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(ctx, s.delegate, entries)
	}

	return s.delegate.AppendCommittedEntries(ctx, entries...)
}

// End implements Spool.
func (s *Interceptor) End() (*Position, error) {
	s.mu.RLock()
	interceptor := s.OnEnd
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(s.delegate)
	}

	return s.delegate.End()
}

// ReplayUntil implements Spool.
func (s *Interceptor) ReplayUntil(
	ctx context.Context,
	end Position,
	lastApplied uint64,
	applyFn func(raftpb.Entry) error,
) error {
	s.mu.RLock()
	interceptor := s.OnReplayUntil
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(ctx, s.delegate, end, lastApplied, applyFn)
	}

	return s.delegate.ReplayUntil(ctx, end, lastApplied, applyFn)
}

// Prune implements Spool.
func (s *Interceptor) Prune(lastApplied uint64) error {
	s.mu.RLock()
	interceptor := s.OnPrune
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(s.delegate, lastApplied)
	}

	return s.delegate.Prune(lastApplied)
}

// Close implements Spool.
func (s *Interceptor) Close() error {
	s.mu.RLock()
	interceptor := s.OnClose
	s.mu.RUnlock()

	if interceptor != nil {
		return interceptor(s.delegate)
	}

	return s.delegate.Close()
}

// SetAppendCommittedEntriesInterceptor sets the interceptor for AppendCommittedEntries.
func (s *Interceptor) SetAppendCommittedEntriesInterceptor(
	fn func(ctx context.Context, delegate Spool, entries []raftpb.Entry) error,
) {
	s.mu.Lock()
	s.OnAppendCommittedEntries = fn
	s.mu.Unlock()
}

// SetEndInterceptor sets the interceptor for End.
func (s *Interceptor) SetEndInterceptor(fn func(delegate Spool) (*Position, error)) {
	s.mu.Lock()
	s.OnEnd = fn
	s.mu.Unlock()
}

// SetReplayUntilInterceptor sets the interceptor for ReplayUntil.
func (s *Interceptor) SetReplayUntilInterceptor(
	fn func(ctx context.Context, delegate Spool, end Position, lastApplied uint64, applyFn func(raftpb.Entry) error) error,
) {
	s.mu.Lock()
	s.OnReplayUntil = fn
	s.mu.Unlock()
}

// SetPruneInterceptor sets the interceptor for Prune.
func (s *Interceptor) SetPruneInterceptor(fn func(delegate Spool, lastApplied uint64) error) {
	s.mu.Lock()
	s.OnPrune = fn
	s.mu.Unlock()
}

// SetCloseInterceptor sets the interceptor for Close.
func (s *Interceptor) SetCloseInterceptor(fn func(delegate Spool) error) {
	s.mu.Lock()
	s.OnClose = fn
	s.mu.Unlock()
}

// ClearInterceptors removes all interceptors.
func (s *Interceptor) ClearInterceptors() {
	s.mu.Lock()
	s.OnAppendCommittedEntries = nil
	s.OnEnd = nil
	s.OnReplayUntil = nil
	s.OnPrune = nil
	s.OnClose = nil
	s.mu.Unlock()
}

// Ensure Interceptor implements Spool.
var _ Spool = (*Interceptor)(nil)
