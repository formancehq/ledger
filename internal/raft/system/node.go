package system

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

type Node struct {
	*raft.Node[ledger.SystemState, *FSM]
	raftConfig           Config
	logger               logging.Logger
	multiplexedTransport *multiplexedTransport
}

func NewNode(config Config, logger logging.Logger, transport *raft.GRPCTransport) (*Node, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(config.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	// Create storage for etcd/etcdraft
	storage, err := raft.NewWALStorage(config.DataDir, logger)
	if err != nil {
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	multiplexedTransport := newMultiplexedTransport(logger.WithFields(map[string]any{
		"cmp": "multiplexer",
	}), transport)

	// Create FSM (Finite State Machine)
	fsm := newFSM(logger, config, multiplexedTransport)

	node, err := raft.NewNode(config.NodeConfig, storage, multiplexedTransport.MainTransport(), fsm, logger)
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
	}, nil
}

func (node *Node) Start() error {
	go node.multiplexedTransport.Start()

	return node.Node.Start()
}

// CreateBucket creates a new bucket via a FSM command
func (node *Node) CreateBucket(ctx context.Context, name, driver string, config map[string]interface{}, snapshotThreshold *uint64) (*ledger.BucketInfo, error) {
	// Create the command
	cmd, err := NewCreateBucketCommand(name, driver, config, snapshotThreshold)
	if err != nil {
		return nil, fmt.Errorf("creating create bucket command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, ret, err := node.Apply(cmd, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("applying command via etcdraft: %w", err)
	}

	node.logger.
		WithFields(map[string]any{"name": name, "driver": driver, "commandID": cmd.ID}).
		Infof("Bucket created via Raft")
	return ret.(*ledger.BucketInfo), nil
}

// GetBucket returns the bucket info for a given name
func (node *Node) GetBucketCluster(ctx context.Context, name string) (service.BucketCluster, error) {
	return node.Inner().GetBucket(name)
}

func (node *Node) GetBucketInfo(ctx context.Context, name string) (*ledger.BucketInfo, error) {
	bucketCluster, err := node.Inner().GetBucket(name)
	if err != nil {
		return nil, ledger.NewNotFoundError("Bucket not found: " + name)
	}
	return pointer.For(bucketCluster.Info()), nil
}

// GetAllBucketsInfo returns all buckets
func (node *Node) GetAllBucketsInfo(ctx context.Context) map[string]ledger.BucketInfo {
	return node.Inner().GetAllBuckets()
}

// DeleteBucket deletes a bucket via a FSM command
func (node *Node) DeleteBucket(ctx context.Context, name string) error {
	// Create the command
	cmd, err := NewDeleteBucketCommand(name)
	if err != nil {
		return fmt.Errorf("creating delete bucket command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, _, err = node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command via etcdraft: %w", err)
	}

	node.logger.WithFields(map[string]any{"name": name, "commandID": cmd.ID}).Infof("Bucket deleted via Raft")
	return nil
}

func (node *Node) ResolveLedger(ctx context.Context, name string) (string, uint64, error) {
	allBuckets := node.Inner().GetAllBuckets()
	for bucketName := range allBuckets {
		bucketCluster, err := node.Inner().GetBucket(bucketName)
		if err != nil {
			continue
		}
		ledgers, err := bucketCluster.GetLedgers(context.Background())
		if err != nil {
			// todo: better error handling
			panic(err)
		}
		for _, info := range ledgers {
			if info.Name == name {
				return bucketCluster.Info().Name, bucketCluster.Info().ID, nil
			}
		}
	}

	return "", 0, ledger.NewNotFoundError("Ledger not found: " + name)
}

func (node *Node) Stop(ctx context.Context) error {
	node.logger.Info("Stopping multiplexed transport")
	node.multiplexedTransport.Stop()

	node.logger.Info("Stopping FSM")
	if err := node.Inner().Stop(ctx); err != nil {
		return nil
	}

	node.logger.Info("Stopping raft node")
	defer func() {
		node.logger.Info("Raft node stopped")
	}()
	return node.Node.Stop(ctx)
}
