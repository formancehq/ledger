package fsm

import (
	"encoding/json"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

// FSM implements the raft.FSM interface
type FSM struct {
	logger       logging.Logger
	buckets      map[string]service.BucketInfo // Map of bucket name -> bucket info
	nextBucketID uint64                        // Next sequential bucket ID
}

func NewFSM(logger logging.Logger) *FSM {
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
func (f *FSM) HandleCreateBucket(cmd service.Command, index uint64) error {
	var createCmd CreateBucketCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create bucket command")
		return fmt.Errorf("unmarshaling create bucket command: %w", err)
	}

	// Convert protobuf Struct to map[string]interface{}
	configMap := make(map[string]interface{})
	if createCmd.Config != nil {
		configMap = createCmd.Config.AsMap()
	}

	// Validate bucket configuration
	if err := service.ValidateBucketConfig(createCmd.Driver, configMap); err != nil {
		f.logger.WithFields(map[string]any{"name": createCmd.Name, "driver": createCmd.Driver, "error": err}).Errorf("Invalid bucket configuration")
		return fmt.Errorf("invalid bucket configuration: %w", err)
	}

	// Assign sequential bucket ID
	bucketID := f.nextBucketID
	f.nextBucketID++

	// Create bucket info using the command date
	bucketInfo := service.BucketInfo{
		ID:        bucketID,
		Name:      createCmd.Name,
		Driver:    createCmd.Driver,
		Config:    configMap,
		CreatedAt: cmd.Date,
	}

	// Store the bucket
	f.buckets[createCmd.Name] = bucketInfo

	f.logger.WithFields(map[string]any{"index": index, "id": bucketID, "name": createCmd.Name, "commandID": cmd.ID}).Infof("Bucket created")
	return nil
}

// HandleDeleteBucket handles the delete bucket command
func (f *FSM) HandleDeleteBucket(cmd service.Command, index uint64) error {
	var deleteCmd DeleteBucketCommand
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		f.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete bucket command")
		return fmt.Errorf("unmarshaling delete bucket command: %w", err)
	}

	// Check if bucket exists
	if _, exists := f.buckets[deleteCmd.Name]; !exists {
		f.logger.WithFields(map[string]any{"name": deleteCmd.Name}).Infof("WARN: Bucket does not exist")
		return fmt.Errorf("bucket does not exist: %s", deleteCmd.Name)
	}

	// Delete the bucket
	delete(f.buckets, deleteCmd.Name)

	f.logger.WithFields(map[string]any{"index": index, "name": deleteCmd.Name, "commandID": cmd.ID}).Infof("Bucket deleted")
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

	f.logger.WithFields(map[string]any{"bucketCount": len(f.buckets), "nextBucketID": f.nextBucketID}).Infof("FSM restored from snapshot")
	return nil
}
