package state

import (
	"sync/atomic"
)

// SyncProgress tracks the progress of a checkpoint fetch.
// All methods are safe for concurrent access.
type SyncProgress struct {
	bytesReceived atomic.Uint64
	bytesTotal    atomic.Uint64
	checkpointID  atomic.Uint64
}

// NewSyncProgress creates a new SyncProgress tracker.
func NewSyncProgress() *SyncProgress {
	return &SyncProgress{}
}

func (p *SyncProgress) SetCheckpointID(id uint64) { p.checkpointID.Store(id) }
func (p *SyncProgress) SetTotal(total uint64)     { p.bytesTotal.Store(total) }
func (p *SyncProgress) AddReceived(n uint64)      { p.bytesReceived.Add(n) }
func (p *SyncProgress) BytesReceived() uint64     { return p.bytesReceived.Load() }
func (p *SyncProgress) BytesTotal() uint64        { return p.bytesTotal.Load() }
func (p *SyncProgress) CheckpointID() uint64      { return p.checkpointID.Load() }
