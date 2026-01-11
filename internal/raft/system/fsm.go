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
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"google.golang.org/protobuf/proto"
)

// State represents the state of the system FSM
type State struct {
	NextLedgerID uint64                      // Next sequential ledger ID
	Nodes        map[string]*ledgerraft.Node // Map of ledger name -> ledger node
	Infos        map[string]*ledgerpb.LedgerInfo
}

// FSM implements the raft.FSM interface
type FSM struct {
	mu                   sync.RWMutex // Protects access to state
	state                State        // FSM state
	logger               logging.Logger
	raftConfig           NodeConfig
	multiplexedTransport *multiplexedTransport
	meterProvider        metric.MeterProvider
}

func newFSM(
	logger logging.Logger,
	raftConfig NodeConfig,
	multiplexedTransport *multiplexedTransport,
	meterProvider metric.MeterProvider,
) *FSM {
	return &FSM{
		state: State{
			Nodes:        make(map[string]*ledgerraft.Node),
			Infos:        make(map[string]*ledgerpb.LedgerInfo),
			NextLedgerID: 1, // Start at 1, first ledger will have ID 1
		},
		logger:               logger,
		raftConfig:           raftConfig,
		multiplexedTransport: multiplexedTransport,
		meterProvider:        meterProvider,
	}
}

// GetState returns a copy of the FSM state
func (fsm *FSM) GetState() *systempb.State {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	// Create a copy of the ledgers map
	ledgersCopy := make(map[string]*ledgerpb.LedgerInfo, len(fsm.state.Nodes))
	for k, v := range fsm.state.Nodes {
		ledgersCopy[k] = v.Info()
	}

	return &systempb.State{
		NextLedgerId: fsm.state.NextLedgerID,
		Ledgers:      ledgersCopy,
	}
}

// Note: With etcd/raft, we don't have an Apply method on FSM.
// The entries are applied directly in the readyLoop of Node.
// Ledgers and logs are now managed by ledger Raft groups.

// handleCreateLedger handles the create ledger command
func (fsm *FSM) handleCreateLedger(ctx context.Context, cmd *raft.Command) (*ledgerpb.LedgerInfo, error) {
	var createCmd systempb.CreateLedgerRequest
	if err := UnmarshalCommandData(cmd.Data, &createCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal create ledger command")
		return nil, fmt.Errorf("unmarshaling create ledger command: %w", err)
	}

	fsm.mu.Lock()
	if _, exists := fsm.state.Infos[createCmd.Name]; exists {
		fsm.mu.Unlock()
		return nil, fmt.Errorf("ledger already exists: %s", createCmd.Name)
	}

	// Assign sequential ledger ID
	ledgerID := fsm.state.NextLedgerID
	fsm.state.NextLedgerID++
	fsm.mu.Unlock()

	// Create ledger info using protobuf types directly
	ledgerInfo := &ledgerpb.LedgerInfo{
		Id:                ledgerID,
		Name:              createCmd.Name,
		StoreDriver:       createCmd.StoreDriver,
		StoreConfig:       createCmd.StoreConfig,
		Metadata:          createCmd.Metadata,
		CreatedAt:         cmd.Date,
		SnapshotThreshold: createCmd.SnapshotThreshold,
	}
	fsm.state.Infos[ledgerInfo.Name] = ledgerInfo

	if err := fsm.startLedgerRaftGroupFromFSM(ctx, ledgerInfo); err != nil {
		return nil, err
	}

	fsm.logger.Infof("Ledger created")
	return ledgerInfo, nil
}

// handleDeleteLedger handles the delete ledger command (hard delete)
func (fsm *FSM) handleDeleteLedger(ctx context.Context, cmd *raft.Command) error {
	var deleteCmd systempb.DeleteLedgerRequest
	if err := UnmarshalCommandData(cmd.Data, &deleteCmd); err != nil {
		fsm.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal delete ledger command")
		return fmt.Errorf("unmarshaling delete ledger command: %w", err)
	}

	fsm.mu.Lock()
	// Check if ledger exists
	_, ok := fsm.state.Infos[deleteCmd.Name]
	if !ok {
		fsm.mu.Unlock()
		return ledgerpb.NewNotFoundError("ledger %s does not exist", deleteCmd.Name)
	}

	group, exists := fsm.state.Nodes[deleteCmd.Name]
	if exists {
		// Stop the Raft group and delete stores (hard delete)
		if err := fsm.stopLedgerRaftGroupHard(ctx, group); err != nil {
			fsm.mu.Unlock()
			return err
		}
		delete(fsm.state.Nodes, deleteCmd.Name)
	}

	// Remove ledger info completely (hard delete)
	delete(fsm.state.Infos, deleteCmd.Name)

	fsm.mu.Unlock()

	fsm.logger.WithFields(map[string]any{"ledger": deleteCmd.Name}).Infof("Ledger hard-deleted")
	return nil
}

func (fsm *FSM) ApplyEntries(ctx context.Context, commands ...*raft.Command) ([]raft.ApplyResult, error) {
	ret := make([]raft.ApplyResult, 0, len(commands))
	for _, cmd := range commands {
		switch cmd.Type {
		case raft.CommandType_CreateLedger:
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
		case raft.CommandType_DeleteLedger:
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

	ledgerNode, ok := fsm.state.Nodes[name]
	if !ok {
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
	result := make(map[string]*ledgerpb.LedgerInfo, len(fsm.state.Infos))
	for k, v := range fsm.state.Infos {
		result[k] = v
	}

	return result
}

// CreateSnapshot creates a snapshot of the FSM state
func (fsm *FSM) CreateSnapshot(_ context.Context) ([]byte, error) {
	fsm.mu.RLock()
	defer fsm.mu.RUnlock()

	snapshotProto := &systempb.SystemFSMSnapshot{
		Ledgers:      fsm.state.Infos,
		NextLedgerId: fsm.state.NextLedgerID,
	}

	// Marshal to protobuf
	data, err := proto.Marshal(snapshotProto)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot data: %w", err)
	}

	return data, nil
}

// RestoreSnapshot restores the FSM from a snapshot
func (fsm *FSM) RestoreSnapshot(ctx context.Context, _ uint64, snapshot raftpb.Snapshot) error {
	// Unmarshal from protobuf
	var snapshotProto systempb.SystemFSMSnapshot
	if err := proto.Unmarshal(snapshot.Data, &snapshotProto); err != nil {
		return err
	}

l1:
	for existingLedgerName := range fsm.state.Infos {
		for expectedLedgerName := range snapshotProto.Ledgers {
			if existingLedgerName == expectedLedgerName {
				continue l1
			}
		}
		delete(fsm.state.Infos, existingLedgerName)

		if node, ok := fsm.state.Nodes[existingLedgerName]; ok {
			if err := node.Stop(ctx); err != nil {
				return err
			}
			delete(fsm.state.Nodes, existingLedgerName)
		}
	}

l2:
	for expectedLedgerName, expectedLedgerInfo := range snapshotProto.Ledgers {
		for existingLedgerName := range fsm.state.Nodes {
			if existingLedgerName == expectedLedgerName {
				continue l2
			}
		}

		fsm.state.Infos[expectedLedgerName] = expectedLedgerInfo
		if err := fsm.startLedgerRaftGroupFromFSM(ctx, expectedLedgerInfo); err != nil {
			return err
		}
	}

	fsm.state.NextLedgerID = snapshotProto.NextLedgerId

	fsm.logger.Infof("FSM restored from snapshot")

	return nil
}

// stopLedgerRaftGroupHard stops a Raft group for a ledger and deletes all associated stores (hard delete)
func (fsm *FSM) stopLedgerRaftGroupHard(ctx context.Context, group *ledgerraft.Node) error {
	if group == nil {
		fsm.logger.Infof("WARN: Ledger Raft group is nil")
		return nil
	}

	ledgerName := group.Info().GetName()

	// Stop the Raft group first
	if err := group.Stop(ctx); err != nil {
		return fmt.Errorf("stopping ledger Raft group: %w", err)
	}

	// Delete store files and data directory
	if err := group.DeleteStoreFiles(); err != nil {
		return fmt.Errorf("deleting ledger store files: %w", err)
	}

	fsm.logger.WithFields(map[string]any{"ledger": ledgerName}).Infof("Ledger Raft group stopped and stores deleted (hard delete)")
	return nil
}

func (fsm *FSM) Stop(ctx context.Context) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	for _, group := range fsm.state.Nodes {
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
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

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
			DataDir:              ledgerDataDir,
			SnapshotThreshold:    snapshotThreshold,
			SnapshotInterval:     fsm.raftConfig.SnapshotInterval,
			ElectionTick:         fsm.raftConfig.ElectionTick,
			HeartbeatTick:        fsm.raftConfig.HeartbeatTick,
			MaxSizePerMsg:        fsm.raftConfig.MaxSizePerMsg,
			MaxInflightMsgs:      fsm.raftConfig.MaxInflightMsgs,
			TickInterval:         fsm.raftConfig.TickInterval,
			CompactionMargin:     fsm.raftConfig.CompactionMargin,
			ProposeQueueCapacity: fsm.raftConfig.ProposeQueueCapacity,
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

				return service.NewGRPCStreamCursor(streamLogs, func(res *ledgerpb.StreamLogsResponse) (*ledgerpb.Log, error) {
					return res.Log, nil
				}), nil
			})
		},
		fsm.meterProvider,
	)
	if err != nil {
		return fmt.Errorf("creating ledger Raft group: %w", err)
	}

	logger.Infof("Storing info...")
	fsm.state.Nodes[ledgerInfo.Name] = group

	logger.Infof("Starting ledger Raft group...")
	if err := group.Start(ctx); err != nil {
		return fmt.Errorf("starting ledger Raft group: %w", err)
	}

	return nil
}

func nodeIDFromLedgerAndRootNodeID(rootNodeID uint64, ledgerInfo *ledgerpb.LedgerInfo) uint64 {
	return (ledgerInfo.GetId() << 16) | rootNodeID
}
