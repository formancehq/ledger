package state

import (
	"sync/atomic"
)

// SyncProgress tracks the progress of a checkpoint fetch.
// All methods are safe for concurrent access.
type SyncProgress struct {
	bytesReceived  atomic.Uint64
	bytesTotal     atomic.Uint64
	filesCompleted atomic.Uint64
	filesTotal     atomic.Uint64
}

// NewSyncProgress creates a new SyncProgress tracker.
func NewSyncProgress() *SyncProgress {
	return &SyncProgress{}
}

func (p *SyncProgress) SetTotal(total uint64)  { p.bytesTotal.Store(total) }
func (p *SyncProgress) AddReceived(n uint64)   { p.bytesReceived.Add(n) }
func (p *SyncProgress) BytesReceived() uint64  { return p.bytesReceived.Load() }
func (p *SyncProgress) BytesTotal() uint64     { return p.bytesTotal.Load() }
func (p *SyncProgress) SetFilesTotal(n uint64) { p.filesTotal.Store(n) }
func (p *SyncProgress) AddFileCompleted()      { p.filesCompleted.Add(1) }
func (p *SyncProgress) FilesCompleted() uint64 { return p.filesCompleted.Load() }
func (p *SyncProgress) FilesTotal() uint64     { return p.filesTotal.Load() }
