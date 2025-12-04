package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/hashicorp/raft"
	"go.uber.org/zap"
)

// RaftLogWriter is a LogWriter implementation that writes logs via Raft
type RaftLogWriter struct {
	raft   *raft.Raft
	logger *zap.Logger
}

// NewRaftLogWriter creates a new RaftLogWriter
func NewRaftLogWriter(raft *raft.Raft, logger *zap.Logger) *RaftLogWriter {
	return &RaftLogWriter{
		raft:   raft,
		logger: logger,
	}
}

// InsertLogs writes logs to the Raft cluster
func (r *RaftLogWriter) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Serialize the logs as an array (FSM expects an array of logs)
	logData, err := json.Marshal(logs)
	if err != nil {
		return fmt.Errorf("marshaling logs: %w", err)
	}

	// Apply the logs via Raft (FSM will persist them to the store)
	future := r.raft.Apply(logData, 10*time.Second)
	if err := future.Error(); err != nil {
		return fmt.Errorf("applying logs via raft: %w", err)
	}

	// Check if FSM returned an error
	if future.Response() != nil {
		if err, ok := future.Response().(error); ok {
			return fmt.Errorf("fsm error: %w", err)
		}
	}

	r.logger.Debug("Logs applied via Raft successfully", zap.Int("count", len(logs)))
	return nil
}

