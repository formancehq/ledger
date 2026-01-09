package system

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	ledgerraft "github.com/formancehq/ledger-v3-poc/internal/raft/ledger"
	"go.opentelemetry.io/otel/metric"
)

type Node struct {
	*raft.Node[ledgerpb.SystemState, *FSM]
	raftConfig NodeConfig
	logger     logging.Logger
	multiplexedTransport *multiplexedTransport
	meterProvider        metric.MeterProvider
}

func NewNode(
	config NodeConfig,
	logger logging.Logger,
	transport *raft.GRPCTransport,
	meterProvider metric.MeterProvider,
) (*Node, error) {
	// meterProvider can be nil if metrics are not enabled
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	// Create storage for etcd/etcdraft
	storage, err := raft.NewWALStorage(config.DataDir, logger)
	if err != nil {
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	multiplexedTransport := newMultiplexedTransport(
		logger.WithFields(map[string]any{
			"cmp": "multiplexer",
		}),
		transport,
		meterProvider,
		config.MultiplexedTransportConfig,
	)

	// Create FSM (Finite State Machine)
	fsm := newFSM(logger, config, multiplexedTransport, meterProvider)

	// Create meter for system node
	systemMeter := meterProvider.Meter("raft.node.system")

	node, err := raft.NewNode(config.NodeConfig, storage, multiplexedTransport.MainTransport(), fsm, logger, systemMeter)
	if err != nil {
		return nil, fmt.Errorf("creating node wrapper: %w", err)
	}

	// Add peers to transport
	// Peers are in format "<id>/<address>", parse them
	for _, peerEntry := range config.Peers {
		logger := logger.WithFields(map[string]any{"peer": peerEntry})
		logger.Debugf("Adding peer to transport")
		transport.AddPeer(peerEntry.ID, peerEntry.Address)
	}

	return &Node{
		Node:                 node,
		logger:               logger,
		raftConfig:           config,
		multiplexedTransport: multiplexedTransport,
		meterProvider:        meterProvider,
	}, nil
}

func (node *Node) Start(ctx context.Context) error {
	go node.multiplexedTransport.Start()

	return node.Node.Start(ctx)
}

// CreateLedger creates a new ledger via a FSM command
func (node *Node) CreateLedger(ctx context.Context, name string, logStoreConfig, runtimeStoreConfig map[string]interface{}, metadata map[string]string, snapshotThreshold *uint64, logStoreDriver, runtimeStoreDriver string) (*ledgerpb.LedgerInfo, error) {
	// Create the command
	cmd, err := NewCreateLedgerCommand(name, logStoreConfig, runtimeStoreConfig, metadata, snapshotThreshold, logStoreDriver, runtimeStoreDriver)
	if err != nil {
		return nil, fmt.Errorf("creating create ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, ret, err := node.Apply(cmd, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

	// Wait for leader to be elected
	node.logger.Infof("Waiting leader election...")
	ledgerGroup, err := node.Node.Inner().GetLedger(name)
	if err != nil {
		return nil, fmt.Errorf("getting ledger group: %w", err)
	}

l:
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			if leader := ledgerGroup.GetLeader(); leader != 0 {
				node.logger.Infof("Ledger Raft group started, leader: %x", leader)
				break l
			}
		}
	}

	node.logger.
		WithFields(map[string]any{"name": name, "log_store_driver": logStoreDriver, "runtime_store_driver": runtimeStoreDriver, "commandID": cmd.ID}).
		Infof("Ledger created on leader")

	// ledgerInfo is already *ledgerpb.LedgerInfo
	ledgerInfo := ret.(*ledgerpb.LedgerInfo)
	return ledgerInfo, nil
}

// GetLedgerNode returns the ledger node for a given name (only if not deleted)
func (node *Node) GetLedgerNode(ctx context.Context, name string) (*ledgerraft.Node, error) {
	return node.Inner().GetLedger(name)
}

func (node *Node) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	// GetLedger returns an error for deleted ledgers, so we need to check GetAllLedgers
	allLedgers := node.Inner().GetAllLedgers()
	ledgerInfo, ok := allLedgers[name]
	if !ok {
		return nil, ledgerpb.NewNotFoundError("Ledger not found: %s", name)
	}
	return ledgerInfo, nil
}

// GetAllLedgersInfo returns all ledgers
func (node *Node) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	allLedgers := node.Inner().GetAllLedgers()
	result := make(map[string]*ledgerpb.LedgerInfo, len(allLedgers))
	for name, info := range allLedgers {
		result[name] = info
	}
	return result, nil
}

// DeleteLedger deletes a ledger via a FSM command
func (node *Node) DeleteLedger(ctx context.Context, name string) error {
	// Create the command
	cmd, err := NewDeleteLedgerCommand(name)
	if err != nil {
		return fmt.Errorf("creating delete ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, _, err = node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command '%s' via etcdraft: %w", cmd, err)
	}

	node.logger.WithFields(map[string]any{"name": name, "commandID": cmd.ID}).Infof("Ledger deleted via Raft")
	return nil
}

func (node *Node) ResolveLedger(ctx context.Context, name string) (string, uint64, error) {
	allLedgers := node.Inner().GetAllLedgers()
	ledgerInfo, ok := allLedgers[name]
	if !ok {
		return "", 0, ledgerpb.NewNotFoundError("Ledger not found: %s", name)
	}

	// With hard delete, if the ledger is in the map, it's active
	return ledgerInfo.GetName(), ledgerInfo.GetId(), nil
}

func (node *Node) ResolveLedgerLeader(ctx context.Context, name string) (uint64, error) {
	ledgerNode, err := node.Inner().GetLedger(name)
	if err != nil {
		return 0, err
	}

	return ledgerNode.GetLeader(), nil
}

func (node *Node) Stop(ctx context.Context) error {
	node.logger.Info("Stopping system FSM...")
	if err := node.Inner().Stop(ctx); err != nil {
		return nil
	}

	node.logger.Info("Stopping raft node...")
	if err := node.Node.Stop(ctx); err != nil {
		return err
	}

	node.logger.Info("Stopping multiplexed transport...")
	node.multiplexedTransport.Stop()

	return nil
}
