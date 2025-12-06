package bucketfsm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// BucketFSM represents the finite state machine for a bucket Raft group
// It manages ledgers within a specific bucket
type BucketFSM struct {
	bucketName   string
	ledgers      map[string]service.LedgerInfo // Map of ledger name -> ledger info
	nextLedgerID uint64                        // Next sequential ledger ID
	logs         []ledger.Log                  // Logs stored in memory until snapshot
	logger       *zap.Logger
}

// NewBucketFSM creates a new bucket FSM
func NewBucketFSM(bucketName string, logger *zap.Logger) *BucketFSM {
	return &BucketFSM{
		bucketName:   bucketName,
		ledgers:      make(map[string]service.LedgerInfo),
		nextLedgerID: 1, // Start at 1, first ledger will have ID 1
		logs:         make([]ledger.Log, 0),
		logger:       logger.With(zap.String("bucket", bucketName), zap.String("component", "bucket-fsm")),
	}
}

// HandleCreateLedger handles the create ledger command for this bucket
func (f *BucketFSM) HandleCreateLedger(data json.RawMessage, index uint64) error {
	var createCmd service.CreateLedgerCommand
	if err := json.Unmarshal(data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create ledger command", zap.Error(err))
		return fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists in this bucket
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.Warn("Ledger already exists in bucket", zap.String("name", createCmd.Name), zap.String("bucket", f.bucketName))
		return fmt.Errorf("ledger already exists in bucket %s: %s", f.bucketName, createCmd.Name)
	}

	// Assign sequential ID to the ledger
	ledgerID := f.nextLedgerID
	f.nextLedgerID++

	// Create ledger info
	ledgerInfo := service.LedgerInfo{
		ID:        ledgerID,
		Name:      createCmd.Name,
		CreatedAt: time.Now().UTC(),
		Metadata:  createCmd.Metadata,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	f.logger.Info("Ledger created in bucket", zap.Uint64("index", index), zap.Uint64("id", ledgerID), zap.String("name", createCmd.Name), zap.String("bucket", f.bucketName))
	return nil
}

// HandleInsertLogs handles the insert logs command by storing logs in memory
func (f *BucketFSM) HandleInsertLogs(data json.RawMessage, index uint64) error {
	var insertCmd service.InsertLogsCommand
	if err := json.Unmarshal(data, &insertCmd); err != nil {
		f.logger.Error("Failed to unmarshal insert logs command", zap.Error(err))
		return fmt.Errorf("unmarshaling insert logs command: %w", err)
	}

	if len(insertCmd.Logs) == 0 {
		f.logger.Debug("Insert logs command with no logs, skipping")
		return nil
	}

	// Store logs in memory (will be written to store during snapshot)
	f.logs = append(f.logs, insertCmd.Logs...)

	f.logger.Info("Logs stored in memory", zap.Int("count", len(insertCmd.Logs)), zap.Int("totalLogs", len(f.logs)))
	return nil
}

// GetLedger returns the ledger info for a given name in this bucket
func (f *BucketFSM) GetLedger(name string) (service.LedgerInfo, bool) {
	info, ok := f.ledgers[name]
	return info, ok
}

// GetAllLedgers returns all ledgers in this bucket
func (f *BucketFSM) GetAllLedgers() map[string]service.LedgerInfo {
	// Return a copy to avoid external modifications
	result := make(map[string]service.LedgerInfo, len(f.ledgers))
	for k, v := range f.ledgers {
		result[k] = v
	}
	return result
}

// CreateSnapshot creates a snapshot of the bucket FSM state
// It writes logs to the log store and returns snapshot data
func (f *BucketFSM) CreateSnapshot(ctx context.Context, logStore service.LogWriter) ([]byte, error) {
	// Write logs to the store before creating snapshot
	if len(f.logs) > 0 {
		if err := logStore.InsertLogs(ctx, f.logs...); err != nil {
			return nil, fmt.Errorf("writing logs to store during snapshot: %w", err)
		}
		f.logger.Info("Logs written to store during snapshot", zap.Int("count", len(f.logs)))
		// Clear logs from memory after writing to store
		f.logs = make([]ledger.Log, 0)
	}

	snapshotData := map[string]interface{}{
		"ledgers":      f.ledgers,
		"nextLedgerID": f.nextLedgerID,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the bucket FSM from a snapshot
func (f *BucketFSM) RestoreSnapshot(data []byte) error {
	var snapshotData struct {
		Ledgers      map[string]service.LedgerInfo `json:"ledgers"`
		NextLedgerID uint64                        `json:"nextLedgerID"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.ledgers = snapshotData.Ledgers
	if f.ledgers == nil {
		f.ledgers = make(map[string]service.LedgerInfo)
	}

	// Restore nextLedgerID, or calculate from existing ledgers if not present
	if snapshotData.NextLedgerID == 0 {
		maxID := uint64(0)
		for _, ledger := range f.ledgers {
			if ledger.ID > maxID {
				maxID = ledger.ID
			}
		}
		f.nextLedgerID = maxID + 1
	} else {
		f.nextLedgerID = snapshotData.NextLedgerID
	}

	f.logger.Info("Bucket FSM restored from snapshot", zap.Int("ledgerCount", len(f.ledgers)), zap.Uint64("nextLedgerID", f.nextLedgerID))
	return nil
}
