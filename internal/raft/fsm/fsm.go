package fsm

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.uber.org/zap"
)

// FSM implements the raft.FSM interface
type FSM struct {
	logger       *zap.Logger
	buckets      map[string]service.BucketInfo // Map of bucket name -> bucket info
	nextBucketID uint64                        // Next sequential bucket ID
}

func NewFSM(logger *zap.Logger) *FSM {
	return &FSM{
		logger:       logger,
		buckets:      make(map[string]service.BucketInfo),
		nextBucketID: 1, // Start at 1, first bucket will have ID 1
	}
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Cluster.
// Ledgers and logs are now managed by bucket Raft groups, not the main FSM.

// HandleCreateBucket handles the create bucket command
func (f *FSM) HandleCreateBucket(data json.RawMessage, index uint64) error {
	var createCmd service.CreateBucketCommand
	if err := json.Unmarshal(data, &createCmd); err != nil {
		f.logger.Error("Failed to unmarshal create bucket command", zap.Error(err))
		return fmt.Errorf("unmarshaling create bucket command: %w", err)
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
		CreatedAt: time.Now().UTC(), // TODO: We MUST not use time.Now() in FSM as state must be deterministic
	}

	// Store the bucket
	f.buckets[createCmd.Name] = bucketInfo

	f.logger.Info("Bucket created", zap.Uint64("index", index), zap.Uint64("id", bucketID), zap.String("name", createCmd.Name))
	return nil
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

// GetAllBucketRaftGroups returns a map of bucket name -> bucket ID for all buckets
// This is used to reconstruct bucket Raft groups on startup
func (f *FSM) GetAllBucketRaftGroups() map[string]uint64 {
	result := make(map[string]uint64, len(f.buckets))
	for name, bucket := range f.buckets {
		result[name] = bucket.ID
	}
	return result
}

// CreateSnapshot creates a snapshot of the FSM state
func (f *FSM) CreateSnapshot() ([]byte, error) {
	snapshotData := map[string]interface{}{
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

// RestoreSnapshot restores the FSM from a snapshot
func (f *FSM) RestoreSnapshot(data []byte) error {
	var snapshotData struct {
		Buckets      map[string]service.BucketInfo `json:"buckets"`
		NextBucketID uint64                        `json:"nextBucketID"`
	}

	if err := json.Unmarshal(data, &snapshotData); err != nil {
		return fmt.Errorf("unmarshaling snapshot data: %w", err)
	}

	f.buckets = snapshotData.Buckets
	if f.buckets == nil {
		f.buckets = make(map[string]service.BucketInfo)
	}

	// Restore nextBucketID, or calculate from existing buckets if not present
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

	f.logger.Info("FSM restored from snapshot", zap.Int("bucketCount", len(f.buckets)), zap.Uint64("nextBucketID", f.nextBucketID))
	return nil
}
