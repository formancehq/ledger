package system

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/formancehq/go-libs/v3/collectionutils"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	ledgerraft "github.com/formancehq/ledger-v3-poc/internal/raft/ledger"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

// State represents the state of the system FSM
type State struct {
	NextLedgerID   uint64                          // Next sequential ledger ID
	Ledgers        map[string]*ledgerraft.Node     // Map of ledger name -> ledger node
	DeletedLedgers map[string]*ledgerpb.LedgerInfo // Map of deleted ledger name -> ledger info (for soft delete)
}

// FSM implements the raft.FSM interface
type FSM struct {
	mu                   sync.RWMutex // Protects access to state
	state                State        // FSM state
	logger               logging.Logger
	raftConfig           Config
	multiplexedTransport *multiplexedTransport
	meterProvider        metric.MeterProvider
}

func newFSM(
	logger logging.Logger,
	raftConfig Config,
	multiplexedTransport *multiplexedTransport,
	meterProvider metric.MeterProvider,
) *FSM {
	return &FSM{
		state: State{
			Ledgers:        make(map[string]*ledgerraft.Node),
			DeletedLedgers: make(map[string]*ledgerpb.LedgerInfo),
			NextLedgerID:   1, // Start at 1, first ledger will have ID 1
		},
		logger:               logger,
		raftConfig:           raftConfig,
		multiplexedTransport: multiplexedTransport,
		meterProvider:        meterProvider,
	}
}

// GetState returns a copy of the FSM state
func (fsm *FSM) GetState() ledgerpb.SystemState {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Create a copy of the ledgers map
	ledgersCopy := make(map[string]*ledgerpb.LedgerInfo, len(fsm.state.Ledgers))
	for k, v := range fsm.state.Ledgers {
		ledgersCopy[k] = v.Info()
	}

	return ledgerpb.SystemState{
		NextLedgerID: fsm.state.NextLedgerID,
		Ledgers:      ledgersCopy,
	}
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by ledger Raft groups.

// handleCreateLedger handles the create ledger command
func (fsm *FSM) handleCreateLedger(ctx context.Context, cmd raft.Command) (*ledgerpb.LedgerInfo, error) {
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
	// Check if ledger was previously deleted (soft delete)
	if deletedInfo, exists := fsm.state.DeletedLedgers[createCmd.Name]; exists {
		fsm.mu.Unlock()
		return nil, fmt.Errorf("ledger %s was deleted at %v and cannot be recreated", createCmd.Name, deletedInfo.DeletedAt)
	}

	// Assign sequential ledger ID
	ledgerID := fsm.state.NextLedgerID
	fsm.state.NextLedgerID++
	fsm.mu.Unlock()

	// Convert timestamp
	createdAt := ledgerpb.NewTimestamp(cmd.Date)

	// Create ledger info using protobuf types directly
	var metadata map[string]string
	if createCmd.Metadata != nil {
		metadata = ledgerpb.StructToMetadata(createCmd.Metadata)
	}
	ledgerInfo := &ledgerpb.LedgerInfo{
		Id:        ledgerID,
		Name:      createCmd.Name,
		Driver:    createCmd.Driver,
		Config:    createCmd.Config,
		Metadata:  metadata,
		CreatedAt: createdAt,
	}
	if createCmd.SnapshotThreshold > 0 {
		ledgerInfo.SnapshotThreshold = createCmd.SnapshotThreshold
	}

	if err := fsm.startLedgerRaftGroupFromFSM(ctx, ledgerInfo); err != nil {
		return nil, err
	}

	fsm.logger.Infof("Ledger created")
	return ledgerInfo, nil
}

// handleDeleteLedger handles the delete ledger command (soft delete)
func (fsm *FSM) handleDeleteLedger(ctx context.Context, cmd raft.Command) error {
	var deleteCmd DeleteLedgerCommand
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	fsm.mu.Lock()
	group, exists := fsm.state.Ledgers[deleteCmd.Name]
	if !exists {
		fsm.mu.Unlock()
		fsm.logger.WithFields(map[string]any{"ledger": deleteCmd.Name}).Infof("WARN: Ledger does not exist")
		return fmt.Errorf("ledger %s does not exist", deleteCmd.Name)
	}

	// Get ledger info and mark as deleted
	ledgerInfo := group.Info()
	if ledgerInfo.DeletedAt != nil {
		fsm.mu.Unlock()
		fsm.logger.WithFields(map[string]any{"ledger": deleteCmd.Name}).Infof("Ledger already deleted")
		return nil // Already deleted, no-op
	}

	// Set deleted_at timestamp
	ledgerInfo.DeletedAt = ledgerpb.NewTimestamp(cmd.Date)

	// Store deleted ledger info and remove from active ledgers
	fsm.state.DeletedLedgers[deleteCmd.Name] = ledgerInfo
	fsm.mu.Unlock()

	// Stop the Raft group
	if err := fsm.stopLedgerRaftGroupSoft(ctx, deleteCmd.Name); err != nil {
		return err
	}

	// Remove from active ledgers map
	fsm.mu.Lock()
	delete(fsm.state.Ledgers, deleteCmd.Name)
	fsm.mu.Unlock()

	fsm.logger.WithFields(map[string]any{"ledger": deleteCmd.Name}).Infof("Ledger soft-deleted")
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

// GetLedger returns the ledger node for a given name (including deleted ledgers)
func (fsm *FSM) GetLedger(name string) (*ledgerraft.Node, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	ledgerNode, ok := fsm.state.Ledgers[name]
	if !ok {
		// Check if ledger was deleted
		if _, deleted := fsm.state.DeletedLedgers[name]; deleted {
			return nil, ledgerpb.NewNotFoundError("ledger %s has been deleted", name)
		}
		return nil, ledgerpb.NewNotFoundError("ledger %s does not exist", name)
	}
	return ledgerNode, nil
}

// GetAllLedgers returns all ledgers (including deleted ones)
func (fsm *FSM) GetAllLedgers() map[string]*ledgerpb.LedgerInfo {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Return a copy to avoid external modifications
	// Include both active and deleted ledgers
	result := make(map[string]*ledgerpb.LedgerInfo, len(fsm.state.Ledgers)+len(fsm.state.DeletedLedgers))
	for k, v := range fsm.state.Ledgers {
		result[k] = v.Info()
	}
	// Add deleted ledgers
	for k, v := range fsm.state.DeletedLedgers {
		result[k] = v
	}
	return result
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *FSM) CreateSnapshot(_ context.Context) ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Ledgers are already in protobuf format
	// Include both active and deleted ledgers in snapshot
	ledgersProto := make(map[string]*LedgerInfo, len(fsm.state.Ledgers)+len(fsm.state.DeletedLedgers))
	for name, node := range fsm.state.Ledgers {
		ledgerInfo := node.Info()
		// Convert from ledgerpb.LedgerInfo to system.LedgerInfo (both are protobuf but different packages)
		var metadata *structpb.Struct
		if len(ledgerInfo.Metadata) > 0 {
			metadata, _ = ledgerpb.MetadataToStruct(ledgerInfo.Metadata)
		}
		ledgersProto[name] = &LedgerInfo{
			Id:                ledgerInfo.Id,
			Name:              ledgerInfo.Name,
			Driver:            ledgerInfo.Driver,
			Config:            ledgerInfo.Config,
			Metadata:          metadata,
			CreatedAt:         ledgerInfo.CreatedAt,
			SnapshotThreshold: ledgerInfo.SnapshotThreshold,
			DeletedAt:         ledgerInfo.DeletedAt,
		}
	}
	// Add deleted ledgers to snapshot
	for name, ledgerInfo := range fsm.state.DeletedLedgers {
		var metadata *structpb.Struct
		if len(ledgerInfo.Metadata) > 0 {
			metadata, _ = ledgerpb.MetadataToStruct(ledgerInfo.Metadata)
		}
		ledgersProto[name] = &LedgerInfo{
			Id:                ledgerInfo.Id,
			Name:              ledgerInfo.Name,
			Driver:            ledgerInfo.Driver,
			Config:            ledgerInfo.Config,
			Metadata:          metadata,
			CreatedAt:         ledgerInfo.CreatedAt,
			SnapshotThreshold: ledgerInfo.SnapshotThreshold,
			DeletedAt:         ledgerInfo.DeletedAt,
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

	// Convert system.LedgerInfo (from snapshot) to ledgerpb.LedgerInfo
	ledgers := make(map[string]*ledgerpb.LedgerInfo, len(snapshotProto.Ledgers))
	for name, ledgerProto := range snapshotProto.Ledgers {
		var metadata map[string]string
		if ledgerProto.Metadata != nil {
			metadata = ledgerpb.StructToMetadata(ledgerProto.Metadata)
		}
		ledgers[name] = &ledgerpb.LedgerInfo{
			Id:                ledgerProto.Id,
			Name:              ledgerProto.Name,
			Driver:            ledgerProto.Driver,
			Config:            ledgerProto.Config,
			Metadata:          metadata,
			CreatedAt:         ledgerProto.CreatedAt,
			SnapshotThreshold: ledgerProto.SnapshotThreshold,
			DeletedAt:         ledgerProto.DeletedAt,
		}
	}

	fsm.mu.Lock()
	fsm.state.Ledgers = make(map[string]*ledgerraft.Node, len(ledgers))
	fsm.mu.Unlock()

	fsm.mu.Lock()
	fsm.state.DeletedLedgers = make(map[string]*ledgerpb.LedgerInfo)
	fsm.mu.Unlock()

	for _, ledgerInfo := range ledgers {
		// Don't start Raft groups for deleted ledgers (soft delete)
		if ledgerInfo.DeletedAt != nil {
			fsm.logger.WithFields(map[string]any{"ledger": ledgerInfo.GetName()}).Infof("Skipping deleted ledger during snapshot restore")
			// Store deleted ledger info
			fsm.mu.Lock()
			fsm.state.DeletedLedgers[ledgerInfo.Name] = ledgerInfo
			fsm.mu.Unlock()
			continue
		}
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

// stopLedgerRaftGroupSoft stops a Raft group for a ledger (soft delete)
func (fsm *FSM) stopLedgerRaftGroupSoft(ctx context.Context, ledgerName string) error {
	fsm.mu.Lock()
	group, exists := fsm.state.Ledgers[ledgerName]
	if !exists {
		fsm.mu.Unlock()
		fsm.logger.WithFields(map[string]any{"ledger": ledgerName}).Infof("WARN: Ledger Raft group does not exist")
		return nil
	}
	fsm.mu.Unlock()

	// Stop the group but keep it in the map
	if err := group.Stop(ctx); err != nil {
		return fmt.Errorf("stopping ledger Raft group: %w", err)
	}

	fsm.logger.WithFields(map[string]any{"ledger": ledgerName}).Infof("Ledger Raft group stopped (soft delete)")
	return nil
}

func (fsm *FSM) Stop(ctx context.Context) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	for _, group := range fsm.state.Ledgers {
		fsm.logger.
			WithFields(map[string]any{"ledger": group.Info().GetName()}).
			Infof("Stopping ledger Raft group...")
		if err := group.Stop(ctx); err != nil {
			return fmt.Errorf("stopping ledger Raft group: %w", err)
		}
		fsm.logger.
			WithFields(map[string]any{"ledger": group.Info().GetName()}).
			Infof("Ledger Raft group stopped")
	}

	return nil
}

// startLedgerRaftGroupFromFSM starts a Raft group for a ledger using information from the FSM
func (fsm *FSM) startLedgerRaftGroupFromFSM(ctx context.Context, ledgerInfo *ledgerpb.LedgerInfo) error {

	logger := fsm.logger.WithFields(map[string]any{
		"ledger": ledgerInfo.GetName(),
	})

	logger.Infof("Creating ledger Raft group...")

	// Use ledger-specific snapshot threshold if set, otherwise use global config
	snapshotThreshold := fsm.raftConfig.SnapshotThreshold
	if ledgerInfo.SnapshotThreshold > 0 {
		snapshotThreshold = ledgerInfo.SnapshotThreshold
	}

	logger.Infof("Creating node...")
	ledgerDataDir := filepath.Join(fsm.raftConfig.DataDir, "ledgers", ledgerInfo.Name)

	// Create meter for this ledger node
	ledgerMeter := fsm.meterProvider.Meter(fmt.Sprintf("ledger.%s", ledgerInfo.Name), metric.WithInstrumentationVersion("1.0.0"))

	group, err := ledgerraft.NewNode(
		ctx,
		ledgerInfo,
		fsm.multiplexedTransport.NewLedgerTransport(ledgerInfo.GetId()),
		raft.NodeConfig{
			NodeID: nodeIDFromLedgerAndRootNodeID(fsm.raftConfig.NodeID, ledgerInfo),
			Peers: collectionutils.Map(fsm.raftConfig.Peers, func(from raft.Peer) raft.Peer {
				return raft.Peer{
					ID:      nodeIDFromLedgerAndRootNodeID(from.ID, ledgerInfo),
					Address: from.Address,
				}
			}),
			DataDir:           ledgerDataDir,
			SnapshotThreshold: snapshotThreshold,
			SnapshotInterval:  fsm.raftConfig.SnapshotInterval,
			ElectionTick:      fsm.raftConfig.ElectionTick,
			HeartbeatTick:     fsm.raftConfig.HeartbeatTick,
			MaxSizePerMsg:     fsm.raftConfig.MaxSizePerMsg,
			MaxInflightMsgs:   fsm.raftConfig.MaxInflightMsgs,
			TickInterval:      fsm.raftConfig.TickInterval,
		},
		logger,
		func(peerID uint64) service.LogReader {
			return service.LogReaderFn(func(ctx context.Context, from uint64, to uint64) (service.Cursor[*ledgerpb.Log], error) {

				conn := fsm.multiplexedTransport.GetPeerConnection(NodeIDFromLedgerNodeID(peerID))
				client := ledgerpb.NewLedgerServiceClient(conn)
				streamLogs, err := client.StreamLogs(ctx, &ledgerpb.StreamLogsRequest{
					Ledger: ledgerInfo.GetName(),
					FromId: from,
					ToId:   to,
				})
				if err != nil {
					return nil, err
				}

				return service.NewGRPCStreamCursor(streamLogs, func(res ledgerpb.StreamLogsResponse) (*ledgerpb.Log, error) {
					return res.Log, nil
				}), nil
			})
		},
		ledgerMeter,
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

func nodeIDFromLedgerAndRootNodeID(rootNodeID uint64, ledgerInfo *ledgerpb.LedgerInfo) uint64 {
	return (ledgerInfo.GetId() << 16) | rootNodeID
}
