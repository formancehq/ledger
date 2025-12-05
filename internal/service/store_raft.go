package service

import (
	"context"
	"encoding/json"
	"fmt"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"go.etcd.io/etcd/raft/v3"
	"go.uber.org/zap"
)

// RaftLogWriter is a LogWriter implementation that writes logs via Raft
type RaftLogWriter struct {
	node   *raft.RawNode
	logger *zap.Logger
}

// NewRaftLogWriter creates a new RaftLogWriter
func NewRaftLogWriter(node *raft.RawNode, logger *zap.Logger) *RaftLogWriter {
	return &RaftLogWriter{
		node:   node,
		logger: logger,
	}
}

// InsertLogs writes logs to the Raft cluster
func (r *RaftLogWriter) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Create an insert logs command
	cmd, err := NewInsertLogsCommand(logs)
	if err != nil {
		return fmt.Errorf("creating insert logs command: %w", err)
	}

	// Serialize the command
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshaling command: %w", err)
	}

	// Propose the command via Raft (will be applied in readyLoop)
	if err := r.node.Propose(cmdData); err != nil {
		return fmt.Errorf("proposing command via raft: %w", err)
	}

	r.logger.Debug("Logs proposed via Raft successfully", zap.Int("count", len(logs)))
	return nil
}
