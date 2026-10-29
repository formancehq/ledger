package state

import "sync"

// SharedState holds shared runtime flags (maintenance mode, require signatures)
// that are read by admission and written by the FSM. It is thread-safe.
type SharedState struct {
	mu                sync.RWMutex
	maintenanceMode   bool
	drainEpoch        string
	drainIndex        uint64
	requireSignatures bool
}

// NewSharedState creates a new SharedState with default values.
func NewSharedState() *SharedState {
	return &SharedState{}
}

// MaintenanceMode returns whether maintenance mode is active.
func (s *SharedState) MaintenanceMode() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.maintenanceMode
}

// SetMaintenanceMode sets whether maintenance mode is active.
func (s *SharedState) SetMaintenanceMode(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.maintenanceMode = enabled
}

// Drain returns the applied shutdown marker. Zero index means no drain.
func (s *SharedState) Drain() (string, uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.drainEpoch, s.drainIndex
}

// SetDrain publishes the marker for admission and cluster status.
func (s *SharedState) SetDrain(epoch string, index uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.drainEpoch, s.drainIndex = epoch, index
}

// RequireSignatures returns whether all requests must be signed.
func (s *SharedState) RequireSignatures() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.requireSignatures
}

// SetRequireSignatures sets whether all requests must be signed.
func (s *SharedState) SetRequireSignatures(require bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requireSignatures = require
}

// Reset clears all shared state. Used during snapshot restore.
func (s *SharedState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.maintenanceMode = false
	s.drainEpoch, s.drainIndex = "", 0
	s.requireSignatures = false
}
