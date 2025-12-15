package system

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/raft/bucket"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// State represents the state of the system FSM
type State struct {
	NextBucketID uint64                  // Next sequential bucket ID
	Buckets      map[string]*bucket.Node // Map of bucket name -> bucket node
}

// FSM implements the raft.FSM interface
type FSM struct {
	mu                   sync.RWMutex // Protects access to state
	state                State        // FSM state
	logger               logging.Logger
	raftConfig           Config
	multiplexedTransport *multiplexedTransport
}

func newFSM(
	logger logging.Logger,
	raftConfig Config,
	multiplexedTransport *multiplexedTransport,
) *FSM {
	return &FSM{
		state: State{
			Buckets:      make(map[string]*bucket.Node),
			NextBucketID: 1, // Start at 1, first bucket will have ID 1
		},
		logger:               logger,
		raftConfig:           raftConfig,
		multiplexedTransport: multiplexedTransport,
	}
}

// GetState returns a copy of the FSM state
func (fsm *FSM) GetState() ledger.SystemState {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Create a copy of the buckets map (shallow copy of the map, but nodes are pointers)
	bucketsCopy := make(map[string]ledger.BucketInfo, len(fsm.state.Buckets))
	for k, v := range fsm.state.Buckets {
		bucketsCopy[k] = v.Info()
	}

	return ledger.SystemState{
		NextBucketID: fsm.state.NextBucketID,
		Buckets:      bucketsCopy,
	}
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by bucket Raft groups, not the main FSM.

// handleCreateBucket handles the create bucket command
func (fsm *FSM) handleCreateBucket(cmd raft.Command) (*ledger.BucketInfo, error) {
	var createCmd CreateBucketCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create bucket command")
		return nil, fmt.Errorf("unmarshaling create bucket command: %w", err)
	}

	// Convert protobuf Struct to map[string]interface{} for validation
	configMap := make(map[string]interface{})
	if createCmd.Config != nil {
		configMap = createCmd.Config.AsMap()
	}

	// Validate bucket configuration
	if err := service.ValidateBucketConfig(createCmd.Driver, configMap); err != nil {
		fsm.logger.WithFields(map[string]any{"name": createCmd.Name, "driver": createCmd.Driver, "error": err}).Errorf("Invalid bucket configuration")
		return nil, fmt.Errorf("invalid bucket configuration: %w", err)
	}

	fsm.mu.Lock()
	if _, exists := fsm.state.Buckets[createCmd.Name]; exists {
		fsm.mu.Unlock()
		return nil, fmt.Errorf("bucket already exists: %s", createCmd.Name)
	}

	// Assign sequential bucket ID
	bucketID := fsm.state.NextBucketID
	fsm.state.NextBucketID++
	fsm.mu.Unlock()

	// Convert config to json.RawMessage
	configJSON, err := json.Marshal(configMap)
	if err != nil {
		return nil, fmt.Errorf("marshaling config to JSON: %w", err)
	}

	// Create bucket info using the command date
	bucketInfo := ledger.BucketInfo{
		ID:        bucketID,
		Name:      createCmd.Name,
		Driver:    createCmd.Driver,
		Config:    configJSON,
		CreatedAt: cmd.Date,
	}
	if createCmd.SnapshotThreshold > 0 {
		bucketInfo.SnapshotThreshold = createCmd.SnapshotThreshold
	}

	err = fsm.startBucketRaftGroupFromFSM(context.Background(), bucketInfo)
	if err != nil {
		return nil, err
	}

	fsm.logger.Infof("Bucket created")
	return &bucketInfo, nil
}

// handleDeleteBucket handles the delete bucket command
func (fsm *FSM) handleDeleteBucket(ctx context.Context, cmd raft.Command) error {
	var deleteCmd DeleteBucketCommand
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete bucket command")
		return fmt.Errorf("unmarshaling delete bucket command: %w", err)
	}

	if err := fsm.stopBucketRaftGroup(ctx, deleteCmd.Name); err != nil {
		return err
	}

	fsm.logger.Infof("BucketCluster deleted")
	return nil
}

func (fsm *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) []raft.ApplyResult {
	ret := make([]raft.ApplyResult, 0, len(commands))
	for _, cmd := range commands {
		switch cmd.Type {
		case CommandTypeCreateBucket:
			info, err := fsm.handleCreateBucket(cmd)
			if err != nil {
				ret = append(ret, raft.ApplyResult{
					Error: err,
				})
				continue
			}
			ret = append(ret, raft.ApplyResult{
				Result: info,
			})
		case CommandTypeDeleteBucket:
			ret = append(ret, raft.ApplyResult{
				Error: fsm.handleDeleteBucket(ctx, cmd),
			})
		default:
			ret = append(ret, raft.ApplyResult{
				Error: fmt.Errorf("unknown command type: %s", cmd.Type),
			})
		}
	}

	return ret
}

// GetBucket returns the bucket info for a given name
func (fsm *FSM) GetBucket(name string) (*bucket.Node, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	bucket, ok := fsm.state.Buckets[name]
	if !ok {
		return nil, fmt.Errorf("bucket does not exist: %s", name)
	}
	return bucket, nil
}

// GetAllBuckets returns all buckets
func (fsm *FSM) GetAllBuckets() map[string]ledger.BucketInfo {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Return a copy to avoid external modifications
	result := make(map[string]ledger.BucketInfo, len(fsm.state.Buckets))
	for k, v := range fsm.state.Buckets {
		result[k] = v.Info()
	}
	return result
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *FSM) CreateSnapshot(_ context.Context) ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Convert buckets to protobuf format
	bucketsProto := make(map[string]*BucketInfo, len(fsm.state.Buckets))
	for name, node := range fsm.state.Buckets {
		bucketInfo := node.Info()

		// Convert json.RawMessage to map[string]interface{} then to protobuf Struct
		var configStruct *structpb.Struct
		if len(bucketInfo.Config) > 0 {
			var configMap map[string]interface{}
			if err := json.Unmarshal(bucketInfo.Config, &configMap); err != nil {
				return nil, fmt.Errorf("unmarshaling bucket config: %w", err)
			}
			var err error
			configStruct, err = structpb.NewStruct(configMap)
			if err != nil {
				return nil, fmt.Errorf("converting bucket config to protobuf struct: %w", err)
			}
		}

		// Convert timestamp
		var createdAt *timestamppb.Timestamp
		if !bucketInfo.CreatedAt.IsZero() {
			createdAt = timestamppb.New(bucketInfo.CreatedAt.Time)
		}

		bucketsProto[name] = &BucketInfo{
			Id:                bucketInfo.ID,
			Name:              bucketInfo.Name,
			Driver:            bucketInfo.Driver,
			Config:            configStruct,
			CreatedAt:         createdAt,
			SnapshotThreshold: bucketInfo.SnapshotThreshold,
		}
	}

	snapshotProto := &SystemFSMSnapshot{
		Buckets:      bucketsProto,
		NextBucketId: fsm.state.NextBucketID,
	}

	// Marshal to protobuf
	data, err := proto.Marshal(snapshotProto)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the FSM from a snapshot
func (fsm *FSM) RestoreSnapshot(ctx context.Context, data []byte) {
	fsm.mu.Lock()
	bucketsToStop := make([]*bucket.Node, 0, len(fsm.state.Buckets))
	for _, node := range fsm.state.Buckets {
		bucketsToStop = append(bucketsToStop, node)
	}
	fsm.mu.Unlock()

	for _, node := range bucketsToStop {
		if err := node.Stop(ctx); err != nil {
			panic(err)
		}
	}

	// Unmarshal from protobuf
	var snapshotProto SystemFSMSnapshot
	if err := proto.Unmarshal(data, &snapshotProto); err != nil {
		panic(fmt.Errorf("unmarshaling snapshot data: %w", err))
	}

	// Convert protobuf buckets to ledger.BucketInfo
	buckets := make(map[string]ledger.BucketInfo, len(snapshotProto.Buckets))
	for name, bucketProto := range snapshotProto.Buckets {
		// Convert config Struct to json.RawMessage
		var configJSON json.RawMessage
		if bucketProto.Config != nil {
			configMap := bucketProto.Config.AsMap()
			var err error
			configJSON, err = json.Marshal(configMap)
			if err != nil {
				panic(fmt.Errorf("marshaling bucket config: %w", err))
			}
		}

		// Convert timestamp
		var createdAt time.Time
		if bucketProto.CreatedAt != nil {
			createdAt = time.New(bucketProto.CreatedAt.AsTime())
		}

		buckets[name] = ledger.BucketInfo{
			ID:                bucketProto.Id,
			Name:              bucketProto.Name,
			Driver:            bucketProto.Driver,
			Config:            configJSON,
			CreatedAt:         createdAt,
			SnapshotThreshold: bucketProto.SnapshotThreshold,
		}
	}

	fsm.mu.Lock()
	fsm.state.Buckets = make(map[string]*bucket.Node, len(buckets))
	fsm.mu.Unlock()

	for _, bucketInfo := range buckets {
		err := fsm.startBucketRaftGroupFromFSM(ctx, bucketInfo)
		if err != nil {
			panic(err)
		}
	}

	fsm.mu.Lock()
	fsm.state.NextBucketID = snapshotProto.NextBucketId
	fsm.mu.Unlock()

	fsm.logger.Infof("FSM restored from snapshot")
}

// stopBucketRaftGroup stops a Raft group for a bucket
func (fsm *FSM) stopBucketRaftGroup(ctx context.Context, bucketName string) error {
	fsm.mu.Lock()
	group, exists := fsm.state.Buckets[bucketName]
	if !exists {
		fsm.mu.Unlock()
		fsm.logger.WithFields(map[string]any{"bucket": bucketName}).Infof("WARN: Bucket Raft group does not exist")
		return nil
	}
	fsm.mu.Unlock()

	// Stop the group
	if err := group.Stop(ctx); err != nil {
		return fmt.Errorf("stopping bucket Raft group: %w", err)
	}

	// Remove from map
	fsm.mu.Lock()
	delete(fsm.state.Buckets, bucketName)
	fsm.mu.Unlock()

	fsm.logger.WithFields(map[string]any{"bucket": bucketName}).Infof("Bucket Raft group stopped")
	return nil
}

func (fsm *FSM) Stop(ctx context.Context) error {
	fsm.mu.Lock()
	bucketsToStop := make([]*bucket.Node, 0, len(fsm.state.Buckets))
	for _, group := range fsm.state.Buckets {
		bucketsToStop = append(bucketsToStop, group)
	}
	fsm.mu.Unlock()

	for _, group := range bucketsToStop {
		if err := group.Stop(ctx); err != nil {
			return fmt.Errorf("stopping bucket Raft group: %w", err)
		}
	}

	return nil
}

// startBucketRaftGroupFromFSM starts a Raft group for a bucket using information from the FSM
func (fsm *FSM) startBucketRaftGroupFromFSM(ctx context.Context, bucketInfo ledger.BucketInfo) error {

	logger := fsm.logger.WithFields(map[string]any{
		"bucket": bucketInfo.Name,
	})

	logger.Infof("Starting bucket Raft group...")

	logReader := service.NewLogReaderFn(func(ctx context.Context, from uint64, to uint64) (service.Cursor[ledger.Log], error) {
		fsm.mu.RLock()
		bucketNode := fsm.state.Buckets[bucketInfo.Name]
		fsm.mu.RUnlock()

		if bucketNode == nil {
			return nil, fmt.Errorf("bucket node not found: %s", bucketInfo.Name)
		}

		return service.
			NewBucketGrpcClient(bucketInfo.Name, service.NewBucketServiceClient(
				fsm.multiplexedTransport.GetPeerConnection(bucketNode.GetLeader()),
			)).
			GetAllLogs(ctx, from, to)
	})

	// Use bucket-specific snapshot threshold if set, otherwise use global config
	snapshotThreshold := fsm.raftConfig.SnapshotThreshold
	if bucketInfo.SnapshotThreshold > 0 {
		snapshotThreshold = bucketInfo.SnapshotThreshold
	}

	group, err := bucket.NewNode(
		bucketInfo,
		fsm.multiplexedTransport.NewBucketTransport(bucketInfo.ID),
		raft.NodeConfig{
			NodeID: nodeIDFromBucketAndRootNodeID(fsm.raftConfig.NodeID, bucketInfo),
			Peers: collectionutils.Map(fsm.raftConfig.Peers, func(from raft.Peer) raft.Peer {
				return raft.Peer{
					ID:      nodeIDFromBucketAndRootNodeID(from.ID, bucketInfo),
					Address: from.Address,
				}
			}),
			DataDir:           filepath.Join(fsm.raftConfig.DataDir, "buckets", bucketInfo.Name),
			SnapshotThreshold: snapshotThreshold,
			SnapshotInterval:  fsm.raftConfig.SnapshotInterval,
			ElectionTick:      fsm.raftConfig.ElectionTick,
			HeartbeatTick:     fsm.raftConfig.HeartbeatTick,
			MaxSizePerMsg:     fsm.raftConfig.MaxSizePerMsg,
			MaxInflightMsgs:   fsm.raftConfig.MaxInflightMsgs,
			TickInterval:      fsm.raftConfig.TickInterval,
		},
		logger,
		fsm.raftConfig.ExtraDataDir,
		logReader,
	)
	if err != nil {
		return fmt.Errorf("creating bucket Raft group: %w", err)
	}

	fsm.mu.Lock()
	fsm.state.Buckets[bucketInfo.Name] = group
	fsm.mu.Unlock()

	if err := group.Start(); err != nil {
		return fmt.Errorf("starting bucket Raft group: %w", err)
	}

	// Wait for leader to be elected
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
			if group.GetLeader() != 0 {
				return nil
			}
		}
	}
}

func bucketIDFromBucketNodeID(v uint64) uint64 {
	return (v & 0xFFFF0000) >> 16
}

func NodeIDFromBucketNodeID(bucketNodeID uint64) uint64 {
	return bucketNodeID & 0x0000FFFF
}

func nodeIDFromBucketAndRootNodeID(rootNodeID uint64, bucket ledger.BucketInfo) uint64 {
	return (bucket.ID << 16) | rootNodeID
}
