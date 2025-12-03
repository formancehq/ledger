package raft

import (
	"io"

	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// FSM implements the raft.FSM interface
type FSM struct {
	logger *zap.Logger
}

func NewFSM(logger *zap.Logger) *FSM {
	return &FSM{
		logger: logger,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.logger.Debug("Applying log entry", zap.Uint64("index", log.Index))
	return nil
}

// Snapshot returns a snapshot of the FSM state
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.logger.Debug("Creating snapshot")
	return &snapshot{}, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(reader io.ReadCloser) error {
	f.logger.Debug("Restoring from snapshot")
	defer reader.Close()
	return nil
}

// snapshot implements raft.FSMSnapshot
type snapshot struct{}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (s *snapshot) Release() {}
