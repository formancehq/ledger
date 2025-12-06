package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// FSM implements the raft.FSM interface
type FSM struct {
	logger       *zap.Logger
	store        service.LogWriter
	logReader    service.LogReader             // Needed for restore
	lastID       uint64                        // Last assigned log ID
	logs         []ledger.Log                  // In-memory logs waiting to be persisted
	ledgers      map[string]service.LedgerInfo // Map of ledger name -> ledger info
	buckets      map[string]service.BucketInfo // Map of bucket name -> bucket info
	nextBucketID uint64                        // Next sequential bucket ID
}

func NewFSM(logger *zap.Logger, store service.LogWriter, logReader service.LogReader) *FSM {
	return &FSM{
		logger:       logger,
		store:        store,
		logReader:    logReader,
		lastID:       0, // Start at 0, first log will be 1
		logs:         make([]ledger.Log, 0),
		ledgers:      make(map[string]service.LedgerInfo),
		buckets:      make(map[string]service.BucketInfo),
		nextBucketID: 1, // Start at 1, first bucket will have ID 1
	}
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Cluster.
// The handleInsertLogs and handleCreateLedger methods are called from Cluster.applyEntry.
// These methods are exported so they can be called from Cluster.

// HandleInsertLogs handles the insert logs command
func (f *FSM) HandleInsertLogs(data json.RawMessage, index uint64) error {
	var insertCmd service.InsertLogsCommand
	if err := json.Unmarshal(data, &insertCmd); err != nil {
		f.logger.Error("Failed to unmarshal insert logs command", zap.Error(err))
		return fmt.Errorf("unmarshaling insert logs command: %w", err)
	}

	// Assign IDs to each log and store them in memory
	for i := range insertCmd.Logs {
		f.lastID++
		insertCmd.Logs[i].ID = pointer.For(f.lastID)
		f.logs = append(f.logs, insertCmd.Logs[i])
	}

	f.logger.Debug("Logs stored in memory", zap.Uint64("index", index), zap.Int("count", len(insertCmd.Logs)), zap.Uint64("lastID", f.lastID), zap.Int("totalLogsInMemory", len(f.logs)))
	return nil
}

// HandleCreateLedger handles the create ledger command
func (f *FSM) HandleCreateLedger(data json.RawMessage, index uint64) error {
	var createCmd service.CreateLedgerCommand
	if err := json.Unmarshal(data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create ledger command", zap.Error(err))
		return fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Check if ledger already exists
	if _, exists := f.ledgers[createCmd.Name]; exists {
		f.logger.Warn("Ledger already exists", zap.String("name", createCmd.Name))
		return fmt.Errorf("ledger already exists: %s", createCmd.Name)
	}

	// Create ledger info
	ledgerInfo := service.LedgerInfo{
		Name:      createCmd.Name,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
		Metadata:  createCmd.Metadata,
	}

	// Store the ledger
	f.ledgers[createCmd.Name] = ledgerInfo

	f.logger.Info("Ledger created", zap.Uint64("index", index), zap.String("name", createCmd.Name))
	return nil
}

// GetLedger returns the ledger info for a given name
func (f *FSM) GetLedger(name string) (service.LedgerInfo, bool) {
	info, ok := f.ledgers[name]
	return info, ok
}

// GetAllLedgers returns all ledgers
func (f *FSM) GetAllLedgers() map[string]service.LedgerInfo {
	// Return a copy to avoid external modifications
	result := make(map[string]service.LedgerInfo, len(f.ledgers))
	for k, v := range f.ledgers {
		result[k] = v
	}
	return result
}

// HandleCreateBucket handles the create bucket command
func (f *FSM) HandleCreateBucket(data json.RawMessage, index uint64) error {
	var createCmd service.CreateBucketCommand
	if err := json.Unmarshal(data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create bucket command", zap.Error(err))
		return fmt.Errorf("unmarshaling create bucket command: %w", err)
	}

	// Check if bucket already exists
	if _, exists := f.buckets[createCmd.Name]; exists {
		f.logger.Warn("Bucket already exists", zap.String("name", createCmd.Name))
		return fmt.Errorf("bucket already exists: %s", createCmd.Name)
	}

	// Validate bucket configuration
	if err := service.ValidateBucketConfig(createCmd.Driver, createCmd.Config); err != nil {
		f.logger.Error("Invalid bucket configuration", zap.String("name", createCmd.Name), zap.String("driver", createCmd.Driver), zap.Error(err))
		return fmt.Errorf("invalid bucket configuration: %w", err)
	}

	// Assign sequential bucket ID
	bucketID := f.nextBucketID
	f.nextBucketID++

	// Create bucket info
	bucketInfo := service.BucketInfo{
		ID:        bucketID,
		Name:      createCmd.Name,
		Driver:    createCmd.Driver,
		Config:    createCmd.Config,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	// Store the bucket
	f.buckets[createCmd.Name] = bucketInfo

	f.logger.Info("Bucket created", zap.Uint64("index", index), zap.Uint64("bucketID", bucketID), zap.String("name", createCmd.Name), zap.String("driver", createCmd.Driver))
	return nil
}

// GetBucket returns the bucket info for a given name
func (f *FSM) GetBucket(name string) (service.BucketInfo, bool) {
	info, ok := f.buckets[name]
	return info, ok
}

// GetAllBuckets returns all buckets
func (f *FSM) GetAllBuckets() map[string]service.BucketInfo {
	// Return a copy to avoid external modifications
	result := make(map[string]service.BucketInfo, len(f.buckets))
	for k, v := range f.buckets {
		result[k] = v
	}
	return result
}

// HandleDeleteBucket handles the delete bucket command
func (f *FSM) HandleDeleteBucket(data json.RawMessage, index uint64) error {
	var deleteCmd service.DeleteBucketCommand
	if err := json.Unmarshal(data, &deleteCmd); err != nil {
		f.logger.Error("Failed to unmarshal delete bucket command", zap.Error(err))
		return fmt.Errorf("unmarshaling delete bucket command: %w", err)
	}

	// Check if bucket exists
	if _, exists := f.buckets[deleteCmd.Name]; !exists {
		f.logger.Warn("Bucket does not exist", zap.String("name", deleteCmd.Name))
		return fmt.Errorf("bucket does not exist: %s", deleteCmd.Name)
	}

	// Delete the bucket
	delete(f.buckets, deleteCmd.Name)

	f.logger.Info("Bucket deleted", zap.Uint64("index", index), zap.String("name", deleteCmd.Name))
	return nil
}

// Snapshot returns a snapshot of the FSM state
// CreateSnapshot creates a snapshot for etcd/raft
func (f *FSM) CreateSnapshot(index uint64) ([]byte, error) {
	f.logger.Info("FSM: Creating snapshot", zap.Uint64("index", index), zap.Uint64("lastID", f.lastID), zap.Int("logsToPersist", len(f.logs)))

	// Write all logs to the store before persisting the snapshot
	if len(f.logs) > 0 {
		f.logger.Info("FSM: Persisting logs to store", zap.Int("count", len(f.logs)))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := f.store.InsertLogs(ctx, f.logs...); err != nil {
			f.logger.Error("Failed to persist logs to store during snapshot", zap.Error(err))
			return nil, fmt.Errorf("persisting logs to store: %w", err)
		}

		f.logger.Info("FSM: Logs persisted to store during snapshot", zap.Int("count", len(f.logs)))
		// Clear in-memory logs after persisting
		f.logs = f.logs[:0]
	} else {
		f.logger.Debug("FSM: No logs to persist")
	}

	// Create snapshot data
	// Note: ledgers are now managed by bucket Raft groups, not the main FSM
	snapshotData := map[string]interface{}{
		"lastID":       f.lastID,
		"buckets":      f.buckets,
		"nextBucketID": f.nextBucketID,
	}

	// Marshal to JSON
	data, err := json.Marshal(snapshotData)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// Restore restores the FSM from a snapshot
func (f *FSM) Restore(reader io.ReadCloser) error {
	f.logger.Debug("Restoring from snapshot")
	defer reader.Close()

	// Read the last ID from the snapshot
	var lastID uint64
	if err := binary.Read(reader, binary.BigEndian, &lastID); err != nil {
		// If we can't read (empty snapshot or error), start from 0
		lastID = 0
		f.logger.Warn("Could not read lastID from snapshot, starting from 0", zap.Error(err))
	}

	f.lastID = lastID
	// Clear in-memory logs - they will be replayed from Raft logs after restore
	f.logs = make([]ledger.Log, 0)

	// Read buckets from snapshot
	decoder := json.NewDecoder(reader)
	var snapshotData struct {
		Buckets      map[string]service.BucketInfo `json:"buckets"`
		NextBucketID uint64                        `json:"nextBucketID"`
	}
	if err := decoder.Decode(&snapshotData); err != nil {
		return fmt.Errorf("decoding snapshot data: %w", err)
	}

	f.ledgers = make(map[string]service.LedgerInfo)
	if snapshotData.Buckets != nil {
		f.buckets = snapshotData.Buckets
		// Calculate nextBucketID from existing buckets if not present in snapshot
		if snapshotData.NextBucketID == 0 {
			maxID := uint64(0)
			for _, bucket := range f.buckets {
				if bucket.ID > maxID {
					maxID = bucket.ID
				}
			}
			f.nextBucketID = maxID + 1
		} else {
			f.nextBucketID = snapshotData.NextBucketID
		}
	} else {
		f.buckets = make(map[string]service.BucketInfo)
		f.nextBucketID = 1
	}

	f.logger.Info("FSM restored from snapshot", zap.Uint64("lastID", lastID), zap.Uint64("nextBucketID", f.nextBucketID), zap.Int("bucketsCount", len(f.buckets)))
	return nil
}

// RestoreSnapshot restores FSM state from snapshot data
func (f *FSM) RestoreSnapshot(data []byte) error {
	var snapshotData struct {
		LastID       uint64                        `json:"lastID"`
		Buckets      map[string]service.BucketInfo `json:"buckets"`
		NextBucketID uint64                        `json:"nextBucketID"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.lastID = snapshotData.LastID
	f.ledgers = make(map[string]service.LedgerInfo)
	if snapshotData.Buckets != nil {
		f.buckets = snapshotData.Buckets
		// Calculate nextBucketID from existing buckets if not present in snapshot
		if snapshotData.NextBucketID == 0 {
			maxID := uint64(0)
			for _, bucket := range f.buckets {
				if bucket.ID > maxID {
					maxID = bucket.ID
				}
			}
			f.nextBucketID = maxID + 1
		} else {
			f.nextBucketID = snapshotData.NextBucketID
		}
	} else {
		f.buckets = make(map[string]service.BucketInfo)
		f.nextBucketID = 1
	}
	f.logs = make([]ledger.Log, 0)

	f.logger.Info("FSM restored from snapshot", zap.Uint64("lastID", f.lastID), zap.Uint64("nextBucketID", f.nextBucketID), zap.Int("bucketsCount", len(f.buckets)))
	return nil
}
