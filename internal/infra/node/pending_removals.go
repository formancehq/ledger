package node

import (
	"bytes"
	"sync"
)

// pendingRemovalSet retains each committed removal independently. A follower
// may observe several removals of one ordinal before any of them reach the FSM.
// The zero value is ready for use. No lock is held during apply or store access.
type pendingRemovalSet struct {
	mu     sync.RWMutex
	byNode map[uint64]map[uint64]*pendingRemoval
}

func (s *pendingRemovalSet) load(nodeID, index uint64) (*pendingRemoval, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pending, ok := s.byNode[nodeID][index]

	return pending, ok
}

func (s *pendingRemovalSet) loadOrStore(nodeID uint64, pending *pendingRemoval) (*pendingRemoval, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if actual, ok := s.byNode[nodeID][pending.committedIndex]; ok {
		return actual, true
	}
	if s.byNode == nil {
		s.byNode = make(map[uint64]map[uint64]*pendingRemoval)
	}
	if s.byNode[nodeID] == nil {
		s.byNode[nodeID] = make(map[uint64]*pendingRemoval)
	}
	s.byNode[nodeID][pending.committedIndex] = pending

	return pending, false
}

func (s *pendingRemovalSet) compareAndDelete(nodeID uint64, pending *pendingRemoval) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.byNode[nodeID]
	if events[pending.committedIndex] != pending {
		return false
	}
	delete(events, pending.committedIndex)
	if len(events) == 0 {
		delete(s.byNode, nodeID)
	}

	return true
}

func (s *pendingRemovalSet) contains(nodeID uint64, instanceID []byte) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, pending := range s.byNode[nodeID] {
		if bytes.Equal(pending.instanceID, instanceID) {
			return true
		}
	}

	return false
}
