package system

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	ledgerraft "github.com/formancehq/ledger-v3-poc/internal/raft/ledger"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// State represents the state of the system FSM
type State struct {
	NextLedgerID uint64                      // Next sequential ledger ID
	Ledgers      map[string]*ledgerraft.Node // Map of ledger name -> ledger node
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
			Ledgers:      make(map[string]*ledgerraft.Node),
			NextLedgerID: 1, // Start at 1, first ledger will have ID 1
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

	// Create a copy of the ledgers map
	ledgersCopy := make(map[string]ledger.LedgerInfo, len(fsm.state.Ledgers))
	for k, v := range fsm.state.Ledgers {
		ledgersCopy[k] = v.Info()
	}

	return ledger.SystemState{
		NextLedgerID: fsm.state.NextLedgerID,
		Ledgers:      ledgersCopy,
	}
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by ledger Raft groups.

// handleCreateLedger handles the create ledger command
func (fsm *FSM) handleCreateLedger(ctx context.Context, cmd raft.Command) (*ledger.LedgerInfo, error) {
	var createCmd CreateLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create ledger command")
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	// Convert protobuf Struct to map[string]interface{} for validation
	configMap := make(map[string]interface{})
	if createCmd.Config != nil {
		configMap = createCmd.Config.AsMap()
	}

	// Validate ledger configuration
	if err := service.ValidateBucketConfig(createCmd.Driver, configMap); err != nil {
		fsm.logger.WithFields(map[string]any{"name": createCmd.Name, "driver": createCmd.Driver, "error": err}).Errorf("Invalid ledger configuration")
		return nil, fmt.Errorf("invalid ledger configuration: %w", err)
	}

	fsm.mu.Lock()
	if _, exists := fsm.state.Ledgers[createCmd.Name]; exists {
		fsm.mu.Unlock()
		return nil, fmt.Errorf("ledger already exists: %s", createCmd.Name)
	}

	// Assign sequential ledger ID
	ledgerID := fsm.state.NextLedgerID
	fsm.state.NextLedgerID++
	fsm.mu.Unlock()

	// Convert config to json.RawMessage
	configJSON, err := json.Marshal(configMap)
	if err != nil {
		return nil, fmt.Errorf("marshaling config to JSON: %w", err)
	}

	// Convert metadata
	var md metadata.Metadata
	if createCmd.Metadata != nil {
		mdMap := createCmd.Metadata.AsMap()
		md = make(metadata.Metadata)
		for k, v := range mdMap {
			if str, ok := v.(string); ok {
				md[k] = str
			}
		}
	}

	// Create ledger info using the command date
	ledgerInfo := ledger.LedgerInfo{
		ID:        ledgerID,
		Name:      createCmd.Name,
		Driver:    createCmd.Driver,
		Config:    configJSON,
		CreatedAt: cmd.Date,
		Metadata:  md,
	}
	if createCmd.SnapshotThreshold > 0 {
		ledgerInfo.SnapshotThreshold = createCmd.SnapshotThreshold
	}

	err = fsm.startLedgerRaftGroupFromFSM(ctx, ledgerInfo)
	if err != nil {
		return nil, err
	}

	fsm.logger.Infof("Ledger created")
	return &ledgerInfo, nil
}

// handleDeleteLedger handles the delete ledger command
func (fsm *FSM) handleDeleteLedger(ctx context.Context, cmd raft.Command) error {
	var deleteCmd DeleteLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	if err := fsm.stopLedgerRaftGroup(ctx, deleteCmd.Name); err != nil {
		return err
	}

	fsm.logger.Infof("Ledger deleted")
	return nil
}

func (fsm *FSM) ApplyEntries(ctx context.Context, commands ...raft.Command) ([]raft.ApplyResult, error) {
	ret := make([]raft.ApplyResult, 0, len(commands))
	for _, cmd := range commands {
		switch cmd.Type {
		case CommandTypeCreateLedger:
			info, err := fsm.handleCreateLedger(ctx, cmd)
			if err != nil {
				ret = append(ret, raft.ApplyResult{
					Error: err,
				})
				continue
			}
			ret = append(ret, raft.ApplyResult{
				Result: info,
			})
		case CommandTypeDeleteLedger:
			ret = append(ret, raft.ApplyResult{
				Error: fsm.handleDeleteLedger(ctx, cmd),
			})
		default:
			ret = append(ret, raft.ApplyResult{
				Error: fmt.Errorf("unknown command type: %s", cmd.Type),
			})
		}
	}

	return ret, nil
}

// GetLedger returns the ledger node for a given name
func (fsm *FSM) GetLedger(name string) (*ledgerraft.Node, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	ledgerNode, ok := fsm.state.Ledgers[name]
	if !ok {
		return nil, ledger.NewNotFoundError("ledger %s does not exist", name)
	}
	return ledgerNode, nil
}

// GetAllLedgers returns all ledgers
func (fsm *FSM) GetAllLedgers() map[string]ledger.LedgerInfo {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Return a copy to avoid external modifications
	result := make(map[string]ledger.LedgerInfo, len(fsm.state.Ledgers))
	for k, v := range fsm.state.Ledgers {
		result[k] = v.Info()
	}
	return result
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *FSM) CreateSnapshot(_ context.Context) ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Convert ledgers to protobuf format
	ledgersProto := make(map[string]*LedgerInfo, len(fsm.state.Ledgers))
	for name, node := range fsm.state.Ledgers {
		ledgerInfo := node.Info()

		// Convert json.RawMessage to map[string]interface{} then to protobuf Struct
		var configStruct *structpb.Struct
		if len(ledgerInfo.Config) > 0 {
			var configMap map[string]interface{}
			if err := json.Unmarshal(ledgerInfo.Config, &configMap); err != nil {
				return nil, fmt.Errorf("unmarshaling ledger config: %w", err)
			}
			var err error
			configStruct, err = structpb.NewStruct(configMap)
			if err != nil {
				return nil, fmt.Errorf("converting ledger config to protobuf struct: %w", err)
			}
		}

		// Convert metadata
		var metadataStruct *structpb.Struct
		if len(ledgerInfo.Metadata) > 0 {
			// Convert metadata.Metadata (map[string]string) to map[string]interface{}
			metadataMap := make(map[string]interface{})
			for k, v := range ledgerInfo.Metadata {
				metadataMap[k] = v
			}
			var err error
			metadataStruct, err = structpb.NewStruct(metadataMap)
			if err != nil {
				return nil, fmt.Errorf("converting ledger metadata to protobuf struct: %w", err)
			}
		}

		// Convert timestamp
		var createdAt *timestamppb.Timestamp
		if !ledgerInfo.CreatedAt.IsZero() {
			createdAt = timestamppb.New(ledgerInfo.CreatedAt.Time)
		}

		ledgersProto[name] = &LedgerInfo{
			Id:                ledgerInfo.ID,
			Name:              ledgerInfo.Name,
			Driver:            ledgerInfo.Driver,
			Config:            configStruct,
			Metadata:          metadataStruct,
			CreatedAt:         createdAt,
			SnapshotThreshold: ledgerInfo.SnapshotThreshold,
		}
	}

	snapshotProto := &SystemFSMSnapshot{
		Ledgers:      ledgersProto,
		NextLedgerId: fsm.state.NextLedgerID,
		Buckets:      nil, // Deprecated, kept for backward compatibility
		NextBucketId: 0,   // Deprecated, kept for backward compatibility
	}

	// Marshal to protobuf
	data, err := proto.Marshal(snapshotProto)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the FSM from a snapshot
func (fsm *FSM) RestoreSnapshot(ctx context.Context, _ uint64, snapshot raftpb.Snapshot) {
	fsm.mu.Lock()
	ledgersToStop := make([]*ledgerraft.Node, 0, len(fsm.state.Ledgers))
	for _, node := range fsm.state.Ledgers {
		ledgersToStop = append(ledgersToStop, node)
	}
	fsm.mu.Unlock()

	for _, node := range ledgersToStop {
		if err := node.Stop(ctx); err != nil {
			panic(err)
		}
	}

	// Unmarshal from protobuf
	var snapshotProto SystemFSMSnapshot
	if err := proto.Unmarshal(snapshot.Data, &snapshotProto); err != nil {
		panic(fmt.Errorf("unmarshaling snapshot data: %w", err))
	}

	// Convert protobuf ledgers to ledger.LedgerInfo
	ledgers := make(map[string]ledger.LedgerInfo, len(snapshotProto.Ledgers))
	for name, ledgerProto := range snapshotProto.Ledgers {
		// Convert config Struct to json.RawMessage
		var configJSON json.RawMessage
		if ledgerProto.Config != nil {
			configMap := ledgerProto.Config.AsMap()
			var err error
			configJSON, err = json.Marshal(configMap)
			if err != nil {
				panic(fmt.Errorf("marshaling ledger config: %w", err))
			}
		}

		// Convert metadata
		var md metadata.Metadata
		if ledgerProto.Metadata != nil {
			mdMap := ledgerProto.Metadata.AsMap()
			md = make(metadata.Metadata)
			for k, v := range mdMap {
				if str, ok := v.(string); ok {
					md[k] = str
				}
			}
		}

		// Convert timestamp
		var createdAt time.Time
		if ledgerProto.CreatedAt != nil {
			createdAt = time.New(ledgerProto.CreatedAt.AsTime())
		}

		ledgers[name] = ledger.LedgerInfo{
			ID:                ledgerProto.Id,
			Name:              ledgerProto.Name,
			Driver:            ledgerProto.Driver,
			Config:            configJSON,
			Metadata:          md,
			CreatedAt:         createdAt,
			SnapshotThreshold: ledgerProto.SnapshotThreshold,
		}
	}

	fsm.mu.Lock()
	fsm.state.Ledgers = make(map[string]*ledgerraft.Node, len(ledgers))
	fsm.mu.Unlock()

	for _, ledgerInfo := range ledgers {
		err := fsm.startLedgerRaftGroupFromFSM(ctx, ledgerInfo)
		if err != nil {
			panic(err)
		}
	}

	fsm.mu.Lock()
	fsm.state.NextLedgerID = snapshotProto.NextLedgerId
	fsm.mu.Unlock()

	fsm.logger.Infof("FSM restored from snapshot")
}

// stopLedgerRaftGroup stops a Raft group for a ledger
func (fsm *FSM) stopLedgerRaftGroup(ctx context.Context, ledgerName string) error {
	fsm.mu.Lock()
	group, exists := fsm.state.Ledgers[ledgerName]
	if !exists {
		fsm.mu.Unlock()
		fsm.logger.WithFields(map[string]any{"ledger": ledgerName}).Infof("WARN: Ledger Raft group does not exist")
		return nil
	}
	fsm.mu.Unlock()

	// Stop the group
	if err := group.Stop(ctx); err != nil {
		return fmt.Errorf("stopping ledger Raft group: %w", err)
	}

	// Remove from map
	fsm.mu.Lock()
	delete(fsm.state.Ledgers, ledgerName)
	fsm.mu.Unlock()

	fsm.logger.WithFields(map[string]any{"ledger": ledgerName}).Infof("Ledger Raft group stopped")
	return nil
}

func (fsm *FSM) Stop(ctx context.Context) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	for _, group := range fsm.state.Ledgers {
		fsm.logger.
			WithFields(map[string]any{"ledger": group.Info().Name}).
			Infof("Stopping ledger Raft group...")
		if err := group.Stop(ctx); err != nil {
			return fmt.Errorf("stopping ledger Raft group: %w", err)
		}
		fsm.logger.
			WithFields(map[string]any{"ledger": group.Info().Name}).
			Infof("Ledger Raft group stopped")
	}

	return nil
}

// startLedgerRaftGroupFromFSM starts a Raft group for a ledger using information from the FSM
func (fsm *FSM) startLedgerRaftGroupFromFSM(ctx context.Context, ledgerInfo ledger.LedgerInfo) error {

	logger := fsm.logger.WithFields(map[string]any{
		"ledger": ledgerInfo.Name,
	})

	logger.Infof("Creating ledger Raft group...")

	// Use ledger-specific snapshot threshold if set, otherwise use global config
	snapshotThreshold := fsm.raftConfig.SnapshotThreshold
	if ledgerInfo.SnapshotThreshold > 0 {
		snapshotThreshold = ledgerInfo.SnapshotThreshold
	}

	logger.Infof("Creating node...")
	group, err := ledgerraft.NewNode(
		ctx,
		ledgerInfo,
		fsm.multiplexedTransport.NewLedgerTransport(ledgerInfo.ID),
		raft.NodeConfig{
			NodeID: nodeIDFromLedgerAndRootNodeID(fsm.raftConfig.NodeID, ledgerInfo),
			Peers: collectionutils.Map(fsm.raftConfig.Peers, func(from raft.Peer) raft.Peer {
				return raft.Peer{
					ID:      nodeIDFromLedgerAndRootNodeID(from.ID, ledgerInfo),
					Address: from.Address,
				}
			}),
			DataDir:           filepath.Join(fsm.raftConfig.DataDir, "ledgers", ledgerInfo.Name),
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
		func(peerID uint64) service.LogReader {
			return service.NewLogReaderFn(func(ctx context.Context, from uint64, to uint64) (service.Cursor[ledger.Log], error) {

				conn := fsm.multiplexedTransport.GetPeerConnection(NodeIDFromLedgerNodeID(peerID))
				client := service.NewLedgerServiceClient(conn)
				streamLogs, err := client.StreamLogs(ctx, &service.StreamLogsRequest{
					Ledger:       ledgerInfo.Name,
					FromSequence: from,
					ToSequence:   to,
				})
				if err != nil {
					return nil, err
				}

				return service.NewGRPCStreamCursor(streamLogs, func(res service.StreamLogsResponse) (ledger.Log, error) {
					return service.LogFromLedgerProto(res.Log)
				}), nil
			})
		},
	)
	if err != nil {
		return fmt.Errorf("creating ledger Raft group: %w", err)
	}

	logger.Infof("Storing info...")
	fsm.mu.Lock()
	fsm.state.Ledgers[ledgerInfo.Name] = group
	fsm.mu.Unlock()

	logger.Infof("Starting ledger Raft group...")
	if err := group.Start(ctx); err != nil {
		return fmt.Errorf("starting ledger Raft group: %w", err)
	}

	return nil
}

func nodeIDFromLedgerAndRootNodeID(rootNodeID uint64, ledgerInfo ledger.LedgerInfo) uint64 {
	return (ledgerInfo.ID << 16) | rootNodeID
}
