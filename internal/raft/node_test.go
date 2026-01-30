package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	storepkg "github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/ledger-v3-poc/internal/store/pebble"
	"github.com/formancehq/ledger-v3-poc/internal/wal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
)

// listLedgerContains checks if a ledger with the given name exists in the store
func listLedgerContains(ctx context.Context, s storepkg.Store, name string) bool {
	cursor, err := s.ListLedgers(ctx)
	if err != nil {
		return false
	}
	defer func() { _ = cursor.Close() }()

	for {
		ledger, err := cursor.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return false
		}
		if ledger.Name == name {
			return true
		}
	}
	return false
}

// ClusterNode represents a single node in the test cluster
type ClusterNode struct {
	ID        uint64
	Node      *Node
	Transport *ChannelTransport

	// Underlying implementations
	Store storepkg.Store
	WAL   WAL
	Spool Spool

	// Interceptors - use these to intercept/modify behavior during tests
	StoreInterceptor *storepkg.StoreInterceptor
	WALInterceptor   *WALInterceptor
	SpoolInterceptor *SpoolInterceptor

	// Directory paths for restart capability
	walDir   string
	dataDir  string
	spoolDir string
}

// Cluster represents a test cluster of Raft nodes
type Cluster struct {
	t      *testing.T
	nodes  []*ClusterNode
	logger logging.Logger
	mu     sync.RWMutex
}

// storeLogStreamerWrapper wraps a store.LogStreamer and adds a Close() method
// to implement the raft.LogStreamer interface.
type storeLogStreamerWrapper struct {
	storepkg.LogStreamer
}

func (w *storeLogStreamerWrapper) Close() error {
	return nil
}

// ClusterLogStreamerProvider provides LogStreamer access to peer stores within a cluster.
// This is used for tests where we don't have gRPC connections between nodes.
type ClusterLogStreamerProvider struct {
	cluster *Cluster
}

func (p *ClusterLogStreamerProvider) GetForPeer(id uint64) (LogStreamer, error) {
	node := p.cluster.GetNodeByID(id)
	if node == nil {
		return nil, fmt.Errorf("peer %d not found in cluster", id)
	}
	// Return the StoreInterceptor (not the underlying Store) so interceptors work
	// Wrap it to add Close() method required by LogStreamer interface
	return &storeLogStreamerWrapper{node.StoreInterceptor}, nil
}

// ClusterConfig holds configuration for creating a test cluster
type ClusterConfig struct {
	// SnapshotThreshold is the number of entries before creating a snapshot
	SnapshotThreshold uint64
}

// DefaultClusterConfig returns default cluster configuration
func DefaultClusterConfig() ClusterConfig {
	return ClusterConfig{
		SnapshotThreshold: 1000,
	}
}

// NewCluster creates a new test cluster with the specified number of nodes
func NewCluster(t *testing.T, numNodes int, config ClusterConfig) *Cluster {
	t.Helper()

	if numNodes < 1 {
		t.Fatalf("cluster must have at least 1 node, got %d", numNodes)
	}

	logger := logging.Testing()
	meter := noop.Meter{}

	cluster := &Cluster{
		t:      t,
		nodes:  make([]*ClusterNode, numNodes),
		logger: logger,
	}

	// Create LogStreamerProvider that accesses peer stores via the cluster
	logStreamerProvider := &ClusterLogStreamerProvider{cluster: cluster}

	// Create transports first
	transports := make([]*ChannelTransport, numNodes)
	for i := range numNodes {
		nodeID := uint64(i + 1)
		transports[i] = NewChannelTransport(nodeID, DefaultChannelTransportConfig())
	}

	// Connect all transports in a full mesh
	for i := range numNodes {
		for j := i + 1; j < numNodes; j++ {
			transports[i].Connect(transports[j])
		}
	}

	// Build peer list for each node
	allPeers := make([]Peer, numNodes)
	for i := range numNodes {
		allPeers[i] = Peer{
			ID: uint64(i + 1),
		}
	}

	// Create each node
	for i := range numNodes {
		nodeID := uint64(i + 1)

		// Create temporary directories
		walDir := t.TempDir()
		dataDir := t.TempDir()
		spoolDir := t.TempDir()

		// Create WAL
		w, err := wal.New(walDir, logger, meter)
		require.NoError(t, err)

		// Create spool
		spool, err := NewDefaultSpool(DefaultSpoolConfig{
			Dir: spoolDir,
		})
		require.NoError(t, err)

		// Create store
		store, err := pebble.NewStore(dataDir, logger, meter, pebble.DefaultConfig())
		require.NoError(t, err)

		// Wrap with interceptors
		walInterceptor := NewWALInterceptor(w)
		spoolInterceptor := NewSpoolInterceptor(spool)
		storeInterceptor := storepkg.NewStoreInterceptor(store)

		// Build peer list excluding self
		peers := make([]Peer, 0, numNodes-1)
		for _, peer := range allPeers {
			if peer.ID != nodeID {
				peers = append(peers, peer)
			}
		}

		// Create node using interceptors
		node, err := NewNode(
			NodeConfig{
				NodeID:            nodeID,
				SnapshotThreshold: config.SnapshotThreshold,
				Peers:             peers,
				ElectionTick:      20,
			},
			transports[i],
			storeInterceptor,
			logger.WithFields(map[string]any{"node": nodeID}),
			meter,
			spoolInterceptor,
			walInterceptor,
			logStreamerProvider,
		)
		require.NoError(t, err)

		cluster.nodes[i] = &ClusterNode{
			ID:               nodeID,
			Node:             node,
			Transport:        transports[i],
			Store:            store,
			WAL:              w,
			Spool:            spool,
			StoreInterceptor: storeInterceptor,
			WALInterceptor:   walInterceptor,
			SpoolInterceptor: spoolInterceptor,
			walDir:           walDir,
			dataDir:          dataDir,
			spoolDir:         spoolDir,
		}
	}

	// Register cleanup
	t.Cleanup(func() {
		cluster.Stop()
	})

	return cluster
}

// Start starts all nodes in the cluster
func (c *Cluster) Start(ctx context.Context) []chan error {
	c.mu.Lock()
	defer c.mu.Unlock()

	errorChans := make([]chan error, len(c.nodes))
	for i, clusterNode := range c.nodes {
		errorChans[i] = make(chan error, 1)
		go func(node *Node, errCh chan error) {
			defer func() {
				if e := recover(); e != nil {
					switch e := e.(type) {
					case error:
						errCh <- fmt.Errorf("node panic: %w", e)
					default:
						errCh <- fmt.Errorf("node panic: %v", e)
					}
				}
			}()
			errCh <- node.Run(ctx)
		}(clusterNode.Node, errorChans[i])
	}

	return errorChans
}

// Stop stops all nodes and cleans up resources
func (c *Cluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, clusterNode := range c.nodes {
		if clusterNode.Node != nil {
			_ = clusterNode.Node.Stop(ctx)
		}
		if clusterNode.Store != nil {
			_ = clusterNode.Store.Close(ctx)
		}
		if clusterNode.Spool != nil {
			_ = clusterNode.Spool.Close()
		}
		if clusterNode.WAL != nil {
			_ = clusterNode.WAL.Close()
		}
		if clusterNode.Transport != nil {
			clusterNode.Transport.Close()
		}
	}
}

// GetNode returns the cluster node at the given index (0-based)
func (c *Cluster) GetNode(index int) *ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if index < 0 || index >= len(c.nodes) {
		return nil
	}
	return c.nodes[index]
}

// GetNodeByID returns the cluster node with the given ID
func (c *Cluster) GetNodeByID(id uint64) *ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, node := range c.nodes {
		if node.ID == id {
			return node
		}
	}
	return nil
}

// WaitForLeader waits for a leader to be elected and returns its ID
func (c *Cluster) WaitForLeader(timeout time.Duration) (uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, clusterNode := range c.nodes {
			if clusterNode.Node.IsLeader() {
				return clusterNode.ID, nil
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	return 0, fmt.Errorf("no leader elected within %v", timeout)
}

// GetLeader returns the current leader node, or nil if no leader
func (c *Cluster) GetLeader() *ClusterNode {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, clusterNode := range c.nodes {
		if clusterNode.Node.IsLeader() {
			return clusterNode
		}
	}
	return nil
}

// Size returns the number of nodes in the cluster
func (c *Cluster) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.nodes)
}

// DisconnectNode disconnects a node from all other nodes
func (c *Cluster) DisconnectNode(nodeID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the node directly (don't call GetNodeByID to avoid deadlock)
	var node *ClusterNode
	for _, n := range c.nodes {
		if n.ID == nodeID {
			node = n
			break
		}
	}
	if node == nil {
		return
	}

	for _, other := range c.nodes {
		if other.ID != nodeID {
			node.Transport.Disconnect(other.ID)
		}
	}
}

// ReconnectNode reconnects a node to all other nodes
func (c *Cluster) ReconnectNode(nodeID uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the node directly (don't call GetNodeByID to avoid deadlock)
	var node *ClusterNode
	for _, n := range c.nodes {
		if n.ID == nodeID {
			node = n
			break
		}
	}
	if node == nil {
		return
	}

	for _, other := range c.nodes {
		if other.ID != nodeID {
			node.Transport.Connect(other.Transport)
		}
	}
}

// RestartNode stops a node's resources and restarts it from the same data directories.
// This simulates a node crash and restart scenario.
func (c *Cluster) RestartNode(ctx context.Context, nodeID uint64, config ClusterConfig) (chan error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find the node
	var nodeIndex = -1
	var clusterNode *ClusterNode
	for i, n := range c.nodes {
		if n.ID == nodeID {
			nodeIndex = i
			clusterNode = n
			break
		}
	}
	if clusterNode == nil {
		return nil, fmt.Errorf("node %d not found", nodeID)
	}

	// Try to stop the old node first (with a short timeout since it might have already crashed)
	if clusterNode.Node != nil {
		stopCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		_ = clusterNode.Node.Stop(stopCtx)
		cancel()
	}

	// Close current resources
	if clusterNode.Store != nil {
		_ = clusterNode.Store.Close(ctx)
	}
	if clusterNode.Spool != nil {
		_ = clusterNode.Spool.Close()
	}
	if clusterNode.WAL != nil {
		_ = clusterNode.WAL.Close()
	}

	// Recreate resources from the same directories
	w, err := wal.New(clusterNode.walDir, c.logger, noop.Meter{})
	if err != nil {
		return nil, fmt.Errorf("recreating WAL: %w", err)
	}

	spool, err := NewDefaultSpool(DefaultSpoolConfig{
		Dir: clusterNode.spoolDir,
	})
	if err != nil {
		return nil, fmt.Errorf("recreating spool: %w", err)
	}

	store, err := pebble.NewStore(clusterNode.dataDir, c.logger, noop.Meter{}, pebble.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("recreating store: %w", err)
	}

	// Create new interceptors
	walInterceptor := NewWALInterceptor(w)
	spoolInterceptor := NewSpoolInterceptor(spool)
	storeInterceptor := storepkg.NewStoreInterceptor(store)

	// Create LogStreamerProvider that accesses peer stores via the cluster
	logStreamerProvider := &ClusterLogStreamerProvider{cluster: c}

	// Build peer list excluding self
	peers := make([]Peer, 0, len(c.nodes)-1)
	for _, other := range c.nodes {
		if other.ID != nodeID {
			peers = append(peers, Peer{ID: other.ID})
		}
	}

	// Create new node
	node, err := NewNode(
		NodeConfig{
			NodeID:            nodeID,
			SnapshotThreshold: config.SnapshotThreshold,
			Peers:             peers,
		},
		clusterNode.Transport,
		storeInterceptor,
		c.logger.WithFields(map[string]any{"node": nodeID}),
		noop.Meter{},
		spoolInterceptor,
		walInterceptor,
		logStreamerProvider,
	)
	if err != nil {
		return nil, fmt.Errorf("creating node: %w", err)
	}

	// Update cluster node
	c.nodes[nodeIndex] = &ClusterNode{
		ID:               nodeID,
		Node:             node,
		Transport:        clusterNode.Transport,
		Store:            store,
		WAL:              w,
		Spool:            spool,
		StoreInterceptor: storeInterceptor,
		WALInterceptor:   walInterceptor,
		SpoolInterceptor: spoolInterceptor,
		walDir:           clusterNode.walDir,
		dataDir:          clusterNode.dataDir,
		spoolDir:         clusterNode.spoolDir,
	}

	// Start the node and return error channel
	errCh := make(chan error, 1)
	go func() {
		errCh <- node.Run(ctx)
	}()

	return errCh, nil
}

// createLedger is a test helper that creates a ledger via the node's Apply method
func createLedger(ctx context.Context, node *Node, name string) (*commonpb.LedgerInfo, error) {
	action := NewCreateLedgerAction(&raftcmdpb.CreateLedgerCommand{
		Name: name,
	})
	logs, err := node.Apply(ctx, action)
	if err != nil {
		return nil, err
	}

	return logs[0].Payload.GetCreateLedger().GetInfo(), nil
}

func TestClusterBasic(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster
	cluster := NewCluster(t, 3, DefaultClusterConfig())

	// Start all nodes
	_ = cluster.Start(ctx)

	// Wait for a leader to be elected
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	require.NotZero(t, leaderID)

	t.Logf("Leader elected: node %d", leaderID)

	// Get the leader node
	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Create a ledger via the leader
	ledgerInfo, err := createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)
	require.Equal(t, "test-ledger", ledgerInfo.Name)

	t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)

	// Create a transaction
	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerInfo.Id,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	// Apply a few transactions
	for i := range 5 {
		_, err := leader.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
		t.Logf("Transaction %d applied", i+1)
	}

	// Verify balances on the leader's store
	balances, err := leader.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
		"world": {"USD"},
		"bank":  {"USD"},
	})
	require.NoError(t, err)
	require.Equal(t, big.NewInt(-500), balances["world"]["USD"])
	require.Equal(t, big.NewInt(500), balances["bank"]["USD"])
}

func TestNodeFailureBetweenStoreSnapshotAndWalSnapshot(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a single-node cluster with snapshot threshold of 10
	config := ClusterConfig{SnapshotThreshold: 10}
	cluster := NewCluster(t, 1, config)

	// Get the node and its WAL interceptor
	clusterNode := cluster.GetNode(0)

	// Start the cluster
	nodeErrors := cluster.Start(ctx)
	require.Len(t, nodeErrors, 1)
	nodeError := nodeErrors[0]

	// Wait for leader election
	_, err := cluster.WaitForLeader(4 * time.Second)
	require.NoError(t, err)

	// Track CreateSnapshot calls to fail on the second one
	var errUnexpected = errors.New("unexpected error")
	clusterNode.WALInterceptor.SetCreateSnapshotInterceptor(func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error {
		return errUnexpected
	})

	// Create a ledger
	_, err = createLedger(ctx, clusterNode.Node, "default")
	require.NoError(t, err)

	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: 1,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	// Should not trigger any snapshotting at this point (7 transactions + 1 CreateLedger = 8 entries)
	for range 7 {
		_, err := clusterNode.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
	}

	// Now should trigger the snapshotting (and the interceptor will fail)
	go func() {
		_, _ = clusterNode.Node.Apply(ctx, createTransaction())
	}()

	select {
	case <-time.After(5 * time.Second):
		require.Fail(t, "node did not fail and it should")
	case err := <-nodeError:
		require.Error(t, err)
		require.ErrorIs(t, err, errUnexpected)
	}

	// Restart the node (simulates crash recovery)
	t.Log("Restarting the node after simulated crash")
	_, err = cluster.RestartNode(ctx, clusterNode.ID, config)
	require.NoError(t, err)

	// Get the new node reference after restart
	clusterNode = cluster.GetNode(0)

	// Wait for leader election
	_, err = cluster.WaitForLeader(2 * time.Second)
	require.NoError(t, err)

	// Verify balances recovered correctly (8 transactions * 100 = 800)
	require.Eventually(t, func() bool {
		balances, err := clusterNode.Store.GetBalances(ctx, 1, map[string][]string{
			"world": {"USD"},
		})
		require.NoError(t, err)

		worldBalance := balances["world"]["USD"]
		return worldBalance.Cmp(big.NewInt(-800)) == 0
	}, 2*time.Second, 100*time.Millisecond)
}

func TestFollowerResyncViaSnapshot(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster with a low snapshot threshold to trigger snapshots quickly
	config := ClusterConfig{SnapshotThreshold: 5}
	cluster := NewCluster(t, 3, config)

	// Start the cluster
	_ = cluster.Start(ctx)

	// Wait for leader election
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: node %d", leaderID)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Find a follower to disconnect
	var follower *ClusterNode
	for _, node := range []*ClusterNode{cluster.GetNode(0), cluster.GetNode(1), cluster.GetNode(2)} {
		if node.ID != leaderID {
			follower = node
			break
		}
	}
	require.NotNil(t, follower, "should have a follower")
	t.Logf("Selected follower to disconnect: node %d", follower.ID)

	// Create a ledger before disconnecting the follower
	ledgerInfo, err := createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)
	t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)

	// Wait for the ledger creation to replicate to the follower
	require.Eventually(t, func() bool {
		return listLedgerContains(ctx, follower.Store, "test-ledger")
	}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Disconnect the follower
	t.Logf("Disconnecting follower node %d", follower.ID)
	cluster.DisconnectNode(follower.ID)

	// Track when the follower receives a snapshot via ApplySnapshot
	snapshotReceived := make(chan struct{}, 1)
	follower.WALInterceptor.SetApplySnapshotInterceptor(func(delegate WAL, snapshot raftpb.Snapshot) error {
		t.Logf("Follower %d received snapshot at index %d", follower.ID, snapshot.Metadata.Index)
		select {
		case snapshotReceived <- struct{}{}:
		default:
		}
		return delegate.ApplySnapshot(snapshot)
	})

	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerInfo.Id,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	// Apply enough transactions to trigger multiple snapshots and WAL compaction
	// With threshold of 5, we need to apply more than 5 entries to trigger a snapshot,
	// then the leader will compact the WAL. When the follower reconnects,
	// it will be too far behind and need to receive a snapshot.
	numTransactions := 20
	t.Logf("Applying %d transactions while follower is disconnected", numTransactions)
	for i := range numTransactions {
		_, err := leader.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
		if (i+1)%5 == 0 {
			t.Logf("Applied %d transactions", i+1)
		}
	}

	// Verify leader has the expected balance
	leaderBalances, err := leader.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
		"world": {"USD"},
		"bank":  {"USD"},
	})
	require.NoError(t, err)
	expectedBalance := big.NewInt(int64(numTransactions * 100))
	require.Equal(t, new(big.Int).Neg(expectedBalance), leaderBalances["world"]["USD"])
	require.Equal(t, expectedBalance, leaderBalances["bank"]["USD"])
	t.Logf("Leader balance verified: world=%s, bank=%s", leaderBalances["world"]["USD"], leaderBalances["bank"]["USD"])

	// Reconnect the follower
	t.Logf("Reconnecting follower node %d", follower.ID)
	cluster.ReconnectNode(follower.ID)

	// Wait for the follower to receive the snapshot
	select {
	case <-snapshotReceived:
		t.Logf("Follower received snapshot successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for follower to receive snapshot")
	}

	// Wait for the follower to sync and verify its data matches the leader
	t.Logf("Waiting for follower to sync data...")
	require.Eventually(t, func() bool {
		followerBalances, err := follower.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
		})
		if err != nil {
			t.Logf("Follower GetBalances error: %v", err)
			return false
		}

		worldMatch := followerBalances["world"]["USD"].Cmp(leaderBalances["world"]["USD"]) == 0
		bankMatch := followerBalances["bank"]["USD"].Cmp(leaderBalances["bank"]["USD"]) == 0

		if worldMatch && bankMatch {
			t.Logf("Follower synced: world=%s, bank=%s", followerBalances["world"]["USD"], followerBalances["bank"]["USD"])
			return true
		}
		return false
	}, 3*time.Second, 100*time.Millisecond)

	t.Log("Test passed: follower successfully resynced via snapshot")
}

func TestFollowerSpoolDuringSyncFromLeader(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster with a low snapshot threshold
	config := ClusterConfig{SnapshotThreshold: 5}
	cluster := NewCluster(t, 5, config)

	// Start the cluster
	_ = cluster.Start(ctx)

	// Wait for leader election
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: node %d", leaderID)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Find a follower to disconnect
	var follower *ClusterNode
	for _, node := range []*ClusterNode{
		cluster.GetNode(0),
		cluster.GetNode(1),
		cluster.GetNode(2),
	} {
		if node.ID != leaderID {
			follower = node
			break
		}
	}
	require.NotNil(t, follower, "should have a follower")
	t.Logf("Selected follower: node %d", follower.ID)

	// Create a ledger before disconnecting the follower
	ledgerInfo, err := createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)
	t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)

	// Wait for the ledger creation to replicate to the follower
	require.Eventually(t, func() bool {
		return listLedgerContains(ctx, follower.Store, "test-ledger")
	}, 10*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Disconnect the follower
	t.Logf("Disconnecting follower node %d", follower.ID)
	cluster.DisconnectNode(follower.ID)

	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerInfo.Id,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	// Apply transactions to trigger snapshot while follower is disconnected
	numInitialTransactions := 20
	t.Logf("Applying %d transactions while follower is disconnected", numInitialTransactions)
	for i := range numInitialTransactions {
		_, err := leader.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
		if (i+1)%5 == 0 {
			t.Logf("Applied %d transactions", i+1)
		}
	}

	// Set up a blocker for the sync process
	// We intercept GetAllLogs on the leader's store to block when follower tries to sync
	syncStarted := make(chan struct{}, 1)
	syncBlocked := make(chan struct{})
	leader.StoreInterceptor.SetGetAllLogsInterceptor(func(ctx context.Context, delegate storepkg.Store, from, to uint64) (storepkg.Cursor[*commonpb.Log], error) {
		// Signal that sync has started
		select {
		case syncStarted <- struct{}{}:
		default:
		}
		// Block until we're told to continue
		t.Logf("Leader GetAllLogs called (from=%d, to=%d) - BLOCKING", from, to)
		<-syncBlocked
		t.Logf("Leader GetAllLogs UNBLOCKED")
		return delegate.GetAllLogs(ctx, from, to)
	})

	// Track when the follower receives a snapshot
	snapshotReceived := make(chan struct{}, 1)
	follower.WALInterceptor.SetApplySnapshotInterceptor(func(delegate WAL, snapshot raftpb.Snapshot) error {
		t.Logf("Follower %d received snapshot at index %d", follower.ID, snapshot.Metadata.Index)
		select {
		case snapshotReceived <- struct{}{}:
		default:
		}
		return delegate.ApplySnapshot(snapshot)
	})

	// Reconnect the follower - it will start syncing
	t.Logf("Reconnecting follower node %d", follower.ID)
	cluster.ReconnectNode(follower.ID)

	// Wait for the follower to receive the snapshot
	select {
	case <-snapshotReceived:
		t.Logf("Follower received snapshot successfully")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for follower to receive snapshot")
	}

	// Wait for sync to start (GetAllLogs called)
	select {
	case <-syncStarted:
		t.Logf("Sync started - follower is now blocked waiting for logs")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for sync to start")
	}

	// While sync is blocked, apply more transactions
	// These should be spooled on the follower since it's still syncing
	numSpooledTransactions := 10
	t.Logf("Applying %d more transactions while follower sync is blocked (should be spooled)", numSpooledTransactions)
	for i := range numSpooledTransactions {
		_, err := leader.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
		t.Logf("Applied spooled transaction %d", i+1)
	}

	// Now unblock the sync
	t.Logf("Unblocking sync...")
	close(syncBlocked)

	// Clear the interceptor to allow normal operation
	leader.StoreInterceptor.SetGetAllLogsInterceptor(nil)

	// Calculate expected final balance
	totalTransactions := numInitialTransactions + numSpooledTransactions
	expectedBalance := big.NewInt(int64(totalTransactions * 100))
	t.Logf("Expected final balance: world=%s, bank=%s", new(big.Int).Neg(expectedBalance), expectedBalance)

	// Wait for the follower to fully sync (including spooled entries)
	t.Logf("Waiting for follower to sync all data (including spooled entries)...")
	require.Eventually(t, func() bool {
		followerBalances, err := follower.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
		})
		if err != nil {
			t.Logf("Follower GetBalances error: %v", err)
			return false
		}

		worldBalance := followerBalances["world"]["USD"]
		bankBalance := followerBalances["bank"]["USD"]

		expectedWorld := new(big.Int).Neg(expectedBalance)
		worldMatch := worldBalance != nil && worldBalance.Cmp(expectedWorld) == 0
		bankMatch := bankBalance != nil && bankBalance.Cmp(expectedBalance) == 0

		if worldMatch && bankMatch {
			t.Logf("Follower fully synced: world=%s, bank=%s", worldBalance, bankBalance)
			return true
		}

		t.Logf("Follower balances: world=%v, bank=%v (expected: world=%s, bank=%s)",
			worldBalance, bankBalance, expectedWorld, expectedBalance)
		return false
	}, 10*time.Second, 200*time.Millisecond)

	// Verify leader has the same balance
	leaderBalances, err := leader.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
		"world": {"USD"},
		"bank":  {"USD"},
	})
	require.NoError(t, err)
	require.Equal(t, new(big.Int).Neg(expectedBalance), leaderBalances["world"]["USD"])
	require.Equal(t, expectedBalance, leaderBalances["bank"]["USD"])

	t.Log("Test passed: follower correctly handled spool during sync and recovered all data")
}

func TestNodeRecoveryAfterFSMSyncFailure(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster with a low snapshot threshold
	config := ClusterConfig{SnapshotThreshold: 5}
	cluster := NewCluster(t, 3, config)

	// Start the cluster
	nodeErrors := cluster.Start(ctx)

	// Wait for leader election
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected: node %d", leaderID)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Find a follower to test
	var follower *ClusterNode
	var followerIndex int
	for i, node := range []*ClusterNode{cluster.GetNode(0), cluster.GetNode(1), cluster.GetNode(2)} {
		if node.ID != leaderID {
			follower = node
			followerIndex = i
			break
		}
	}
	require.NotNil(t, follower, "should have a follower")
	t.Logf("Selected follower: node %d (index %d)", follower.ID, followerIndex)

	// Create a ledger before disconnecting the follower
	ledgerInfo, err := createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)
	t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)

	// Wait for the ledger creation to replicate to the follower
	require.Eventually(t, func() bool {
		return listLedgerContains(ctx, follower.Store, "test-ledger")
	}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Disconnect the follower
	t.Logf("Disconnecting follower node %d", follower.ID)
	cluster.DisconnectNode(follower.ID)

	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerInfo.Id,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	// Apply transactions to trigger snapshot while follower is disconnected
	numTransactions := 20
	t.Logf("Applying %d transactions while follower is disconnected", numTransactions)
	for i := range numTransactions {
		_, err := leader.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
		if (i+1)%5 == 0 {
			t.Logf("Applied %d transactions", i+1)
		}
	}

	// Track when the follower receives a snapshot
	snapshotAppliedToWAL := make(chan struct{}, 1)
	follower.WALInterceptor.SetApplySnapshotInterceptor(func(delegate WAL, snapshot raftpb.Snapshot) error {
		t.Logf("Follower %d: ApplySnapshot to WAL at index %d", follower.ID, snapshot.Metadata.Index)
		err := delegate.ApplySnapshot(snapshot)
		if err == nil {
			select {
			case snapshotAppliedToWAL <- struct{}{}:
			default:
			}
		}
		return err
	})

	// Set up interceptor to make FSM sync fail (GetAllLogs returns error)
	var errSyncFailed = errors.New("simulated FSM sync failure")
	leader.StoreInterceptor.SetGetAllLogsInterceptor(func(ctx context.Context, delegate storepkg.Store, from, to uint64) (storepkg.Cursor[*commonpb.Log], error) {
		t.Logf("Leader GetAllLogs called - returning error to simulate sync failure")
		return nil, errSyncFailed
	})

	// Reconnect the follower - it will receive snapshot but FSM sync will fail
	t.Logf("Reconnecting follower node %d", follower.ID)
	cluster.ReconnectNode(follower.ID)

	// Wait for the snapshot to be applied to WAL
	select {
	case <-snapshotAppliedToWAL:
		t.Logf("Snapshot applied to WAL successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for snapshot to be applied to WAL")
	}

	// Wait for the follower node to fail due to sync error
	followerNodeError := nodeErrors[followerIndex]
	select {
	case err := <-followerNodeError:
		require.Error(t, err)
		t.Logf("Follower node failed as expected: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for follower to fail")
	}

	// Clear the interceptor on leader so sync can succeed on restart
	leader.StoreInterceptor.SetGetAllLogsInterceptor(nil)

	// Restart the follower node
	t.Logf("Restarting follower node %d", follower.ID)
	newNodeError, err := cluster.RestartNode(ctx, follower.ID, config)
	require.NoError(t, err)

	// Get the new follower reference
	follower = cluster.GetNodeByID(follower.ID)
	require.NotNil(t, follower)

	// Wait for leader election (might change after restart)
	_, err = cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected after restart")

	// Calculate expected balance
	expectedBalance := big.NewInt(int64(numTransactions * 100))

	// Wait for the follower to sync and verify its data matches
	t.Logf("Waiting for follower to sync data after restart...")
	require.Eventually(t, func() bool {
		followerBalances, err := follower.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
		})
		if err != nil {
			t.Logf("Follower GetBalances error: %v", err)
			return false
		}

		worldBalance := followerBalances["world"]["USD"]
		bankBalance := followerBalances["bank"]["USD"]

		expectedWorld := new(big.Int).Neg(expectedBalance)
		worldMatch := worldBalance != nil && worldBalance.Cmp(expectedWorld) == 0
		bankMatch := bankBalance != nil && bankBalance.Cmp(expectedBalance) == 0

		if worldMatch && bankMatch {
			t.Logf("Follower synced after restart: world=%s, bank=%s", worldBalance, bankBalance)
			return true
		}

		t.Logf("Follower balances: world=%v, bank=%v (expected: world=%s, bank=%s)",
			worldBalance, bankBalance, expectedWorld, expectedBalance)
		return false
	}, 10*time.Second, 200*time.Millisecond)

	// Verify leader still has correct balance
	leaderBalances, err := leader.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
		"world": {"USD"},
		"bank":  {"USD"},
	})
	require.NoError(t, err)
	require.Equal(t, new(big.Int).Neg(expectedBalance), leaderBalances["world"]["USD"])
	require.Equal(t, expectedBalance, leaderBalances["bank"]["USD"])

	// Make sure node is still running without errors
	select {
	case err := <-newNodeError:
		t.Fatalf("Restarted node failed unexpectedly: %v", err)
	default:
		// Node is still running, good
	}

	t.Log("Test passed: node recovered correctly after FSM sync failure")
}

func TestFollowerRestartLeaderStability(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster
	config := DefaultClusterConfig()
	cluster := NewCluster(t, 3, config)

	// Start the cluster
	_ = cluster.Start(ctx)

	// Wait for leader election
	leaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Initial leader elected: node %d", leaderID)

	leader := cluster.GetLeader()
	require.NotNil(t, leader)

	// Find a follower to restart
	var follower *ClusterNode
	for _, node := range []*ClusterNode{
		cluster.GetNode(0),
		cluster.GetNode(1),
		cluster.GetNode(2),
	} {
		if node.ID != leaderID {
			follower = node
			break
		}
	}
	require.NotNil(t, follower, "should have a follower")
	t.Logf("Selected follower to restart: node %d", follower.ID)

	// Create a ledger to verify cluster is working
	ledgerInfo, err := createLedger(ctx, leader.Node, "test-ledger")
	require.NoError(t, err)
	t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)

	// Wait for the ledger creation to replicate to the follower
	require.Eventually(t, func() bool {
		return listLedgerContains(ctx, follower.Store, "test-ledger")
	}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Stop and restart the follower
	t.Logf("Stopping follower node %d", follower.ID)
	err = follower.Node.Stop(ctx)
	require.NoError(t, err)

	t.Logf("Restarting follower node %d", follower.ID)
	_, err = cluster.RestartNode(ctx, follower.ID, config)
	require.NoError(t, err)

	// Get the new follower reference after restart
	follower = cluster.GetNodeByID(follower.ID)
	require.NotNil(t, follower)

	// Wait for cluster to stabilize and verify leader is still the same
	require.Eventually(t, func() bool {
		currentLeader := cluster.GetLeader()
		if currentLeader == nil {
			t.Logf("No leader currently")
			return false
		}
		return currentLeader.ID == leaderID
	}, 5*time.Second, 100*time.Millisecond, "leader should remain the same after follower restart")

	currentLeaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	require.Equal(t, leaderID, currentLeaderID, "leader should remain the same after follower restart")
	t.Logf("Leader after follower restart: node %d (unchanged)", currentLeaderID)

	// Verify the restarted follower can still receive replication
	// Create another transaction on the leader
	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerInfo.Id,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	_, err = leader.Node.Apply(ctx, createTransaction())
	require.NoError(t, err)
	t.Logf("Applied transaction on leader")

	// Verify the transaction is replicated to the restarted follower
	expectedBalance := big.NewInt(100)
	require.Eventually(t, func() bool {
		balances, err := follower.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
			"bank": {"USD"},
		})
		if err != nil {
			t.Logf("Follower GetBalances error: %v", err)
			return false
		}
		bankBalance := balances["bank"]["USD"]
		if bankBalance != nil && bankBalance.Cmp(expectedBalance) == 0 {
			t.Logf("Follower synced: bank=%s", bankBalance)
			return true
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "transaction should be replicated to restarted follower")

	t.Log("Test passed: follower restarted successfully and leader remained stable")
}

func TestLocalSnapshotWALFailureRecovery(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a single-node cluster with a low snapshot threshold
	config := ClusterConfig{SnapshotThreshold: 10}
	cluster := NewCluster(t, 1, config)

	// Get the node
	node := cluster.GetNode(0)

	// Start the cluster
	nodeErrors := cluster.Start(ctx)
	require.Len(t, nodeErrors, 1)
	nodeError := nodeErrors[0]

	// Wait for leader election
	_, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Node %d became leader", node.ID)

	// Create a ledger
	ledgerInfo, err := createLedger(ctx, node.Node, "test-ledger")
	require.NoError(t, err)
	t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)

	createTransaction := func() *raftcmdpb.Action {
		return NewCreateLedgerLogAction(&raftcmdpb.CreateLedgerLogCommand{
			LedgerId: ledgerInfo.Id,
			Command: &raftcmdpb.CreateLedgerLogCommand_AppendTransaction{
				AppendTransaction: &raftcmdpb.AppendTransactionCommand{
					Postings: []*commonpb.Posting{{
						Source:      "world",
						Destination: "bank",
						Amount:      commonpb.NewBigInt(big.NewInt(100)),
						Asset:       "USD",
					}},
				},
			},
		})
	}

	// Apply some transactions but not enough to trigger snapshot yet
	// Threshold is 10, so we apply 7 (plus 1 for CreateLedger = 8 entries)
	numInitialTransactions := 7
	t.Logf("Applying %d initial transactions (not triggering snapshot yet)", numInitialTransactions)
	for range numInitialTransactions {
		_, err := node.Node.Apply(ctx, createTransaction())
		require.NoError(t, err)
	}

	// Verify initial balance
	initialBalance := big.NewInt(int64(numInitialTransactions * 100))
	balances, err := node.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
		"world": {"USD"},
		"bank":  {"USD"},
	})
	require.NoError(t, err)
	require.Equal(t, new(big.Int).Neg(initialBalance), balances["world"]["USD"])
	require.Equal(t, initialBalance, balances["bank"]["USD"])
	t.Logf("Initial balance verified: world=%s, bank=%s", balances["world"]["USD"], balances["bank"]["USD"])

	// Set up interceptor to make WAL.CreateSnapshot fail
	var errSnapshotFailed = errors.New("simulated WAL snapshot failure")
	node.WALInterceptor.SetCreateSnapshotInterceptor(func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error {
		t.Logf("WAL.CreateSnapshot called at index %d - returning error", i)
		return errSnapshotFailed
	})

	// Now apply more transactions to trigger snapshot (need 2 more to reach threshold of 10)
	numTriggerTransactions := 2
	t.Logf("Applying %d more transactions to trigger snapshot", numTriggerTransactions)
	for range numTriggerTransactions {
		go func() {
			_, _ = node.Node.Apply(ctx, createTransaction())
		}()
	}

	// Wait for the node to fail due to snapshot error
	select {
	case err := <-nodeError:
		require.Error(t, err)
		require.ErrorIs(t, err, errSnapshotFailed)
		t.Logf("Node failed as expected: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for node to fail")
	}

	// Calculate expected final balance after all transactions
	totalTransactions := numInitialTransactions + numTriggerTransactions
	expectedBalance := big.NewInt(int64(totalTransactions * 100))

	// Restart the node
	t.Logf("Restarting node %d", node.ID)
	newNodeError, err := cluster.RestartNode(ctx, node.ID, config)
	require.NoError(t, err)

	// Get the new node reference
	node = cluster.GetNode(0)
	require.NotNil(t, node)

	// Wait for leader election
	_, err = cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Got leader after restart")

	// Verify the state is consistent after replay
	// Since the snapshot was not saved, all entries should be replayed
	t.Logf("Verifying state after restart...")
	require.Eventually(t, func() bool {
		balances, err := node.Store.GetBalances(ctx, ledgerInfo.Id, map[string][]string{
			"world": {"USD"},
			"bank":  {"USD"},
		})
		if err != nil {
			t.Logf("GetBalances error: %v", err)
			return false
		}

		worldBalance := balances["world"]["USD"]
		bankBalance := balances["bank"]["USD"]

		expectedWorld := new(big.Int).Neg(expectedBalance)
		worldMatch := worldBalance != nil && worldBalance.Cmp(expectedWorld) == 0
		bankMatch := bankBalance != nil && bankBalance.Cmp(expectedBalance) == 0

		if worldMatch && bankMatch {
			t.Logf("State verified after restart: world=%s, bank=%s", worldBalance, bankBalance)
			return true
		}

		t.Logf("Current balances: world=%v, bank=%v (expected: world=%s, bank=%s)",
			worldBalance, bankBalance, expectedWorld, expectedBalance)
		return false
	}, 5*time.Second, 200*time.Millisecond)

	// Make sure node is still running without errors
	select {
	case err := <-newNodeError:
		t.Fatalf("Restarted node failed unexpectedly: %v", err)
	default:
		// Node is still running, good
	}

	t.Log("Test passed: node recovered correctly after WAL snapshot failure, entries replayed correctly")
}
