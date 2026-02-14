package node

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/cache"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"
)

// listLedgerContains checks if a ledger with the given name exists in the store
func listLedgerContains(s *data.Store, name string) bool {
	cursor, err := s.ListLedgers()
	if err != nil {
		return false
	}
	defer func() { _ = cursor.Close() }()

	for {
		ledger, err := cursor.Next()
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
	Store *data.Store
	WAL   wal.WAL
	Spool spool.Spool
	Cache *cache.Cache

	// Interceptors - use these to intercept/modify behavior during tests
	StoreInterceptor *data.StoreInterceptor
	WALInterceptor   *wal.Interceptor
	SpoolInterceptor *spool.Interceptor

	// Directory paths for restart capability
	walDir   string
	dataDir  string
	spoolDir string
}

// Cluster represents a test cluster of Raft nodes
type Cluster struct {
	t                       *testing.T
	nodes                   []*ClusterNode
	logger                  logging.Logger
	mu                      sync.RWMutex
	snapshotFetcherProvider *ClusterSnapshotFetcherProvider
}

// SnapshotFetcherInterceptorFunc is a function that intercepts FetchSnapshot calls.
// If it returns an error, the error is returned immediately.
// If it returns nil, the actual FetchSnapshot is called.
type SnapshotFetcherInterceptorFunc func(ctx context.Context, snapshotID uint64, targetDir string) error

// ClusterSnapshotFetcherProvider provides SnapshotFetcher access to peer stores within a cluster.
// This is used for tests where we don't have gRPC connections between nodes.
type ClusterSnapshotFetcherProvider struct {
	cluster     *Cluster
	interceptor SnapshotFetcherInterceptorFunc
	mu          sync.RWMutex
}

// SetFetchSnapshotInterceptor sets an interceptor function that is called before each FetchSnapshot.
// Pass nil to remove the interceptor.
func (p *ClusterSnapshotFetcherProvider) SetFetchSnapshotInterceptor(fn SnapshotFetcherInterceptorFunc) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.interceptor = fn
}

func (p *ClusterSnapshotFetcherProvider) getInterceptor() SnapshotFetcherInterceptorFunc {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.interceptor
}

func (p *ClusterSnapshotFetcherProvider) GetForPeer(id uint64) (state.SnapshotFetcher, error) {
	node := p.cluster.GetNodeByID(id)
	if node == nil {
		return nil, fmt.Errorf("peer %d not found in cluster", id)
	}
	return &clusterSnapshotFetcher{store: node.Store, provider: p}, nil
}

// clusterSnapshotFetcher fetches snapshots directly from a peer's store.
type clusterSnapshotFetcher struct {
	store    *data.Store
	provider *ClusterSnapshotFetcherProvider
}

func (f *clusterSnapshotFetcher) FetchSnapshot(ctx context.Context, snapshotID uint64, targetDir string) (uint64, string, error) {
	// Call interceptor if set
	if interceptor := f.provider.getInterceptor(); interceptor != nil {
		if err := interceptor(ctx, snapshotID, targetDir); err != nil {
			return 0, "", err
		}
	}

	// Get the checkpoint path from the source store
	srcPath, err := f.store.GetCheckpointPath(snapshotID)
	if err != nil {
		return 0, "", fmt.Errorf("getting checkpoint path: %w", err)
	}

	// Copy files from source to target
	var totalSize uint64
	err = filepath.Walk(srcPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(srcPath, path)
		if err != nil {
			return err
		}

		targetPath := filepath.Join(targetDir, relPath)

		if info.IsDir() {
			return os.MkdirAll(targetPath, info.Mode())
		}

		// Copy file
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer func() {
			_ = src.Close()
		}()

		dst, err := os.Create(targetPath)
		if err != nil {
			return err
		}
		defer func() {
			_ = dst.Close()
		}()

		n, err := io.Copy(dst, src)
		if err != nil {
			return err
		}
		totalSize += uint64(n)

		return nil
	})
	if err != nil {
		return 0, "", fmt.Errorf("copying checkpoint: %w", err)
	}

	return totalSize, "", nil
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

	// Create SnapshotFetcherProvider
	snapshotFetcherProvider := &ClusterSnapshotFetcherProvider{cluster: cluster}
	cluster.snapshotFetcherProvider = snapshotFetcherProvider

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

		// Create DefaultWAL
		w, err := wal.New(walDir, logger, meter)
		require.NoError(t, err)

		// Create spool
		defaultSpool, err := spool.NewDefault(spool.DefaultSpoolConfig{
			Dir: spoolDir,
		})
		require.NoError(t, err)

		// Create store
		pebbleStore, err := data.NewStore(dataDir, logger, meter, data.DefaultConfig())
		require.NoError(t, err)

		// Wrap with interceptors
		walInterceptor := wal.NewWALInterceptor(w)
		spoolInterceptor := spool.NewInterceptor(defaultSpool)
		storeInterceptor := data.NewStoreInterceptor(pebbleStore)

		// Build peer list excluding self
		peers := make([]Peer, 0, numNodes-1)
		for _, peer := range allPeers {
			if peer.ID != nodeID {
				peers = append(peers, peer)
			}
		}

		// Create node using interceptors
		nodeConfig := NodeConfig{
			NodeID:            nodeID,
			SnapshotThreshold: config.SnapshotThreshold,
			Peers:             peers,
			ElectionTick:      20,
			Bootstrap:         true,
		}
		nodeConfig.SetDefaults()
		nodeCache, err := cache.New(nodeConfig.RotationThreshold, nil)
		require.NoError(t, err)

		node, err := NewNode(
			nodeConfig,
			transports[i],
			pebbleStore,
			logger.WithFields(map[string]any{"node": nodeID}),
			meter,
			spoolInterceptor,
			walInterceptor,
			snapshotFetcherProvider,
			nodeCache,
			attributes.New(),
			true, // audit enabled
		)
		require.NoError(t, err)

		cluster.nodes[i] = &ClusterNode{
			ID:               nodeID,
			Node:             node,
			Transport:        transports[i],
			Store:            pebbleStore,
			WAL:              w,
			Spool:            defaultSpool,
			Cache:            nodeCache,
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

	errorChannels := make([]chan error, len(c.nodes))
	for i, clusterNode := range c.nodes {
		errorChannels[i] = make(chan error, 1)
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
			errCh <- node.Run(ctx, make(chan struct{}))
		}(clusterNode.Node, errorChannels[i])
	}

	return errorChannels
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
			_ = clusterNode.Store.Close()
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

// GetSnapshotFetcherProvider returns the cluster's snapshot fetcher provider.
func (c *Cluster) GetSnapshotFetcherProvider() *ClusterSnapshotFetcherProvider {
	return c.snapshotFetcherProvider
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
		_ = clusterNode.Store.Close()
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
		return nil, fmt.Errorf("recreating DefaultWAL: %w", err)
	}

	defaultSpool, err := spool.NewDefault(spool.DefaultSpoolConfig{
		Dir: clusterNode.spoolDir,
	})
	if err != nil {
		return nil, fmt.Errorf("recreating spool: %w", err)
	}

	newStore, err := data.NewStore(clusterNode.dataDir, c.logger, noop.Meter{}, data.DefaultConfig())
	if err != nil {
		return nil, fmt.Errorf("recreating store: %w", err)
	}

	// Create new interceptors
	walInterceptor := wal.NewWALInterceptor(w)
	spoolInterceptor := spool.NewInterceptor(defaultSpool)
	storeInterceptor := data.NewStoreInterceptor(newStore)

	// Use the cluster's snapshot fetcher provider
	snapshotFetcherProvider := c.snapshotFetcherProvider

	// Build peer list excluding self
	peers := make([]Peer, 0, len(c.nodes)-1)
	for _, other := range c.nodes {
		if other.ID != nodeID {
			peers = append(peers, Peer{ID: other.ID})
		}
	}

	// Create new node
	nodeConfig := NodeConfig{
		NodeID:            nodeID,
		SnapshotThreshold: config.SnapshotThreshold,
		Peers:             peers,
	}
	nodeConfig.SetDefaults()
	nodeCache, err := cache.New(nodeConfig.RotationThreshold, nil)
	if err != nil {
		return nil, fmt.Errorf("creating cache: %w", err)
	}

	node, err := NewNode(
		nodeConfig,
		clusterNode.Transport,
		newStore,
		c.logger.WithFields(map[string]any{"node": nodeID}),
		noop.Meter{},
		spoolInterceptor,
		walInterceptor,
		snapshotFetcherProvider,
		nodeCache,
		attributes.New(),
		true, // audit enabled
	)
	if err != nil {
		return nil, fmt.Errorf("creating node: %w", err)
	}

	// Update cluster node
	c.nodes[nodeIndex] = &ClusterNode{
		ID:               nodeID,
		Node:             node,
		Transport:        clusterNode.Transport,
		Store:            newStore,
		WAL:              w,
		Spool:            defaultSpool,
		Cache:            nodeCache,
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
		errCh <- node.Run(ctx, make(chan struct{}))
	}()

	return errCh, nil
}

// generateProposalID generates a random proposal ID for tests
func generateProposalID() uint64 {
	var buf [8]byte
	_, _ = rand.Read(buf[:])
	return uint64(buf[0]) | uint64(buf[1])<<8 | uint64(buf[2])<<16 | uint64(buf[3])<<24 |
		uint64(buf[4])<<32 | uint64(buf[5])<<40 | uint64(buf[6])<<48 | uint64(buf[7])<<56
}

// proposeAndWait proposes a command to the node and waits for the result
func proposeAndWait(node *Node, proposal *raftcmdpb.Proposal) ([]*commonpb.Log, error) {
	if len(proposal.Orders) == 0 {
		return nil, fmt.Errorf("proposal has no orders")
	}

	cmdData, err := proto.Marshal(proposal)
	if err != nil {
		return nil, fmt.Errorf("marshaling proposal: %w", err)
	}

	p := NewProposal(proposal.Id, cmdData)
	fsmFuture, err := node.Propose(p)
	if err != nil {
		return nil, fmt.Errorf("proposing command: %w", err)
	}

	if _, err := p.Wait(); err != nil {
		return nil, fmt.Errorf("waiting for raft consensus: %w", err)
	}

	logs, err := fsmFuture.Wait()
	if err != nil {
		return nil, fmt.Errorf("waiting for fsm result: %w", err)
	}

	if len(logs) == 0 {
		return nil, fmt.Errorf("fsm returned empty logs")
	}

	return logs, nil
}

// nowTimestamp returns the current time as a commonpb.Timestamp
func nowTimestamp() *commonpb.Timestamp {
	return &commonpb.Timestamp{Data: uint64(time.Now().UnixNano())}
}

// createLedger is a test helper that creates a ledger via direct proposal to the node
func createLedger(ctx context.Context, node *Node, name string) (*commonpb.LedgerInfo, error) {
	proposal := &raftcmdpb.Proposal{
		Id:   generateProposalID(),
		Date: nowTimestamp(),
		Orders: []*raftcmdpb.Order{{
			Type: &raftcmdpb.Order_CreateLedger{
				CreateLedger: &raftcmdpb.CreateLedgerOrder{
					Name: name,
				},
			},
		}},
	}

	logs, err := proposeAndWait(node, proposal)
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

	// Create multiple ledgers via the leader to test consensus replication
	for i := range 5 {
		ledgerName := fmt.Sprintf("test-ledger-%d", i)
		ledgerInfo, err := createLedger(ctx, leader.Node, ledgerName)
		require.NoError(t, err)
		require.Equal(t, ledgerName, ledgerInfo.Name)
		t.Logf("Created ledger: %s (ID: %d)", ledgerInfo.Name, ledgerInfo.Id)
	}

	// Verify all ledgers are replicated to all nodes
	for _, node := range cluster.nodes {
		require.Eventually(t, func() bool {
			for i := range 5 {
				if !listLedgerContains(node.Store, fmt.Sprintf("test-ledger-%d", i)) {
					return false
				}
			}
			return true
		}, 5*time.Second, 100*time.Millisecond, "all ledgers should be replicated to node %d", node.ID)
	}

	t.Log("Test passed: all ledgers replicated to all nodes")
}

func TestNodeFailureBetweenStoreSnapshotAndWalSnapshot(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a single-node cluster with snapshot threshold of 10
	config := ClusterConfig{SnapshotThreshold: 10}
	cluster := NewCluster(t, 1, config)

	// Get the node and its DefaultWAL interceptor
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
	clusterNode.WALInterceptor.SetCreateSnapshotInterceptor(func(delegate wal.WAL, i uint64, r *raftpb.ConfState, data []byte) error {
		return errUnexpected
	})

	// Should not trigger any snapshotting at this point (8 ledger creates < threshold of 10)
	for i := range 8 {
		_, err := createLedger(ctx, clusterNode.Node, fmt.Sprintf("ledger-%d", i))
		require.NoError(t, err)
	}

	// Now should trigger the snapshotting (and the interceptor will fail)
	go func() {
		_, _ = createLedger(ctx, clusterNode.Node, "trigger-ledger")
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

	// Verify ledgers recovered correctly
	require.Eventually(t, func() bool {
		for i := range 8 {
			if !listLedgerContains(clusterNode.Store, fmt.Sprintf("ledger-%d", i)) {
				return false
			}
		}
		return true
	}, 2*time.Second, 100*time.Millisecond)
}

func TestFollowerResyncViaSnapshot(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster with a low snapshot threshold to trigger snapshots quickly
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
		return listLedgerContains(follower.Store, "test-ledger")
	}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Disconnect the follower
	t.Logf("Disconnecting follower node %d", follower.ID)
	cluster.DisconnectNode(follower.ID)

	// Track when the follower receives a snapshot via ApplySnapshot
	snapshotReceived := make(chan struct{}, 1)
	follower.WALInterceptor.SetApplySnapshotInterceptor(func(delegate wal.WAL, snapshot raftpb.Snapshot) error {
		t.Logf("Follower %d received snapshot at index %d", follower.ID, snapshot.Metadata.Index)
		select {
		case snapshotReceived <- struct{}{}:
		default:
		}
		return delegate.ApplySnapshot(snapshot)
	})

	// Apply enough ledger creates to trigger multiple snapshots and WAL compaction
	// With threshold of 5, we need to apply more than 5 entries to trigger a snapshot,
	// then the leader will compact the WAL. When the follower reconnects,
	// it will be too far behind and need to receive a snapshot.
	numLedgers := 20
	t.Logf("Creating %d ledgers while follower is disconnected", numLedgers)
	for i := range numLedgers {
		_, err := createLedger(ctx, leader.Node, fmt.Sprintf("ledger-%d", i))
		require.NoError(t, err)
		if (i+1)%5 == 0 {
			t.Logf("Created %d ledgers", i+1)
		}
	}

	// Verify leader has all the ledgers
	for i := range numLedgers {
		require.True(t, listLedgerContains(leader.Store, fmt.Sprintf("ledger-%d", i)),
			"leader should have ledger-%d", i)
	}
	t.Logf("Leader verified: has all %d ledgers", numLedgers)

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
	require.Eventually(t, func() (result bool) {
		// Recover from panic if database is being restored
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Follower store access panicked (likely during restore): %v", r)
				result = false
			}
		}()

		// Check if follower has all ledgers
		for i := range numLedgers {
			if !listLedgerContains(follower.Store, fmt.Sprintf("ledger-%d", i)) {
				return false
			}
		}
		t.Logf("Follower synced: has all %d ledgers", numLedgers)
		return true
	}, 10*time.Second, 100*time.Millisecond)

	t.Log("Test passed: follower successfully resynced via snapshot")
}

func TestFollowerSpoolDuringSyncFromLeader(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster with a low snapshot threshold
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
		return listLedgerContains(follower.Store, "test-ledger")
	}, 10*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Disconnect the follower
	t.Logf("Disconnecting follower node %d", follower.ID)
	cluster.DisconnectNode(follower.ID)

	// Create ledgers to trigger snapshot while follower is disconnected
	numInitialLedgers := 20
	t.Logf("Creating %d ledgers while follower is disconnected", numInitialLedgers)
	for i := range numInitialLedgers {
		_, err := createLedger(ctx, leader.Node, fmt.Sprintf("ledger-%d", i))
		require.NoError(t, err)
		if (i+1)%5 == 0 {
			t.Logf("Created %d ledgers", i+1)
		}
	}

	// Wait for the leader's snapshot to be created and logs to be compacted.
	// This is important because snapshot creation is async (runMaintenanceTask).
	// Without this wait, the leader might still have all entries and would
	// replicate them instead of sending a snapshot to the follower.
	t.Log("Waiting for leader snapshot to be created and logs compacted...")
	require.Eventually(t, func() bool {
		snapshot, err := leader.WAL.Snapshot()
		if err != nil {
			return false
		}
		// Snapshot should be at an index close to the last entry
		// With compaction margin, we just need to ensure a recent snapshot exists
		hasSnapshot := snapshot.Metadata.Index > 0
		if hasSnapshot {
			t.Logf("Leader snapshot at index %d", snapshot.Metadata.Index)
		}
		return hasSnapshot
	}, 10*time.Second, 100*time.Millisecond, "leader should have created a snapshot")

	// Set up a blocker for the checkpoint restore process
	// We intercept FetchSnapshot on the snapshot fetcher to block when follower tries to restore
	syncStarted := make(chan struct{}, 1)
	syncBlocked := make(chan struct{})
	snapshotFetcherProvider := cluster.GetSnapshotFetcherProvider()
	require.NotNil(t, snapshotFetcherProvider, "snapshot fetcher provider should be available")

	snapshotFetcherProvider.SetFetchSnapshotInterceptor(func(ctx context.Context, snapshotID uint64, targetDir string) error {
		// Signal that sync has started
		select {
		case syncStarted <- struct{}{}:
		default:
		}
		// Block until we're told to continue
		t.Logf("FetchSnapshot called (snapshotID=%d, targetDir=%s) - BLOCKING", snapshotID, targetDir)
		<-syncBlocked
		t.Logf("FetchSnapshot UNBLOCKED")
		return nil // Return nil to proceed with the actual fetch
	})

	// Track when the follower receives a snapshot
	snapshotReceived := make(chan struct{}, 1)
	follower.WALInterceptor.SetApplySnapshotInterceptor(func(delegate wal.WAL, snapshot raftpb.Snapshot) error {
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

	// Wait for sync (FetchSnapshot) to start
	select {
	case <-syncStarted:
		t.Logf("Checkpoint restore started - follower is now blocked waiting for snapshot")
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for checkpoint restore to start")
	}

	// While sync is blocked, create more ledgers
	// These should be spooled on the follower since it's still syncing
	numSpooledLedgers := 10
	t.Logf("Creating %d more ledgers while follower sync is blocked (should be spooled)", numSpooledLedgers)
	for i := range numSpooledLedgers {
		_, err := createLedger(ctx, leader.Node, fmt.Sprintf("spooled-ledger-%d", i))
		require.NoError(t, err)
		t.Logf("Created spooled ledger %d", i+1)
	}

	// Now unblock the sync
	t.Logf("Unblocking checkpoint restore...")
	close(syncBlocked)

	// Clear the interceptor to allow normal operation
	snapshotFetcherProvider.SetFetchSnapshotInterceptor(nil)

	// Calculate expected total ledgers (initial + spooled + test-ledger)
	totalLedgers := numInitialLedgers + numSpooledLedgers + 1

	// Wait for the follower to fully sync (including spooled entries)
	t.Logf("Waiting for follower to sync all data (including spooled entries)...")
	require.Eventually(t, func() bool {
		defer func() {
			// Handle transient "pebble: closed" panic during checkpoint restore
			if r := recover(); r != nil {
				t.Logf("Recovered from panic during ledger check: %v", r)
			}
		}()

		// Check initial ledgers
		for i := range numInitialLedgers {
			if !listLedgerContains(follower.Store, fmt.Sprintf("ledger-%d", i)) {
				return false
			}
		}
		// Check spooled ledgers
		for i := range numSpooledLedgers {
			if !listLedgerContains(follower.Store, fmt.Sprintf("spooled-ledger-%d", i)) {
				return false
			}
		}
		t.Logf("Follower fully synced: has all %d ledgers", totalLedgers)
		return true
	}, 15*time.Second, 200*time.Millisecond)

	// Verify leader has the same ledgers
	for i := range numInitialLedgers {
		require.True(t, listLedgerContains(leader.Store, fmt.Sprintf("ledger-%d", i)),
			"leader should have ledger-%d", i)
	}
	for i := range numSpooledLedgers {
		require.True(t, listLedgerContains(leader.Store, fmt.Sprintf("spooled-ledger-%d", i)),
			"leader should have spooled-ledger-%d", i)
	}

	t.Log("Test passed: follower correctly handled spool during checkpoint restore and recovered all data")
}

func TestNodeRecoveryAfterFSMSyncFailure(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	// Create a 3-node cluster with a low snapshot threshold
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
		return listLedgerContains(follower.Store, "test-ledger")
	}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to follower")

	// Disconnect the follower
	t.Logf("Disconnecting follower node %d", follower.ID)
	cluster.DisconnectNode(follower.ID)

	// Create ledgers to trigger snapshot while follower is disconnected
	numLedgers := 20
	t.Logf("Creating %d ledgers while follower is disconnected", numLedgers)
	for i := range numLedgers {
		_, err := createLedger(ctx, leader.Node, fmt.Sprintf("ledger-%d", i))
		require.NoError(t, err)
		if (i+1)%5 == 0 {
			t.Logf("Created %d ledgers", i+1)
		}
	}

	// Wait for the leader's snapshot to be created and include all ledgers.
	// This is critical because snapshot creation is async (runMaintenanceTask).
	// Without this wait, the leader might send an incomplete snapshot.
	t.Log("Waiting for leader snapshot to include all ledgers...")
	require.Eventually(t, func() bool {
		snapshot, err := leader.WAL.Snapshot()
		if err != nil {
			return false
		}
		// Snapshot should be at index >= numLedgers + 1 (test-ledger creation)
		hasCompleteSnapshot := snapshot.Metadata.Index >= uint64(numLedgers)
		if hasCompleteSnapshot {
			t.Logf("Leader snapshot at index %d (includes all %d ledgers)", snapshot.Metadata.Index, numLedgers)
		}
		return hasCompleteSnapshot
	}, 5*time.Second, 50*time.Millisecond, "leader should have created a complete snapshot")

	// Track when the follower receives a snapshot
	snapshotAppliedToWAL := make(chan struct{}, 1)
	follower.WALInterceptor.SetApplySnapshotInterceptor(func(delegate wal.WAL, snapshot raftpb.Snapshot) error {
		t.Logf("Follower %d: ApplySnapshot to DefaultWAL at index %d", follower.ID, snapshot.Metadata.Index)
		err := delegate.ApplySnapshot(snapshot)
		if err == nil {
			select {
			case snapshotAppliedToWAL <- struct{}{}:
			default:
			}
		}
		return err
	})

	// Set up interceptor to make checkpoint restore fail (FetchSnapshot returns error)
	var errSyncFailed = errors.New("simulated checkpoint restore failure")
	snapshotFetcherProvider := cluster.GetSnapshotFetcherProvider()
	require.NotNil(t, snapshotFetcherProvider, "snapshot fetcher provider should be available")

	snapshotFetcherProvider.SetFetchSnapshotInterceptor(func(ctx context.Context, snapshotID uint64, targetDir string) error {
		t.Logf("FetchSnapshot called (snapshotID=%d) - returning error to simulate failure", snapshotID)
		return errSyncFailed
	})

	// Reconnect the follower - it will receive snapshot but checkpoint restore will fail
	t.Logf("Reconnecting follower node %d", follower.ID)
	cluster.ReconnectNode(follower.ID)

	// Wait for the snapshot to be applied to DefaultWAL
	select {
	case <-snapshotAppliedToWAL:
		t.Logf("Snapshot applied to DefaultWAL successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for snapshot to be applied to DefaultWAL")
	}

	// Wait for the follower node to fail due to checkpoint restore error
	followerNodeError := nodeErrors[followerIndex]
	select {
	case err := <-followerNodeError:
		require.Error(t, err)
		t.Logf("Follower node failed as expected: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for follower to fail")
	}

	// Clear the interceptor so sync can succeed on restart
	snapshotFetcherProvider.SetFetchSnapshotInterceptor(nil)

	// Restart the follower node
	t.Logf("Restarting follower node %d", follower.ID)
	newNodeError, err := cluster.RestartNode(ctx, follower.ID, config)
	require.NoError(t, err)

	// Get the new follower reference
	follower = cluster.GetNodeByID(follower.ID)
	require.NotNil(t, follower)

	// Wait for leader election (might change after restart)
	newLeaderID, err := cluster.WaitForLeader(5 * time.Second)
	require.NoError(t, err)
	t.Logf("Leader elected after restart: node %d", newLeaderID)

	// Refresh leader reference (leader might have changed after restart)
	leader = cluster.GetLeader()
	require.NotNil(t, leader)

	// Wait for the follower to sync and verify its data matches
	t.Logf("Waiting for follower to sync data after restart...")
	require.Eventually(t, func() bool {
		defer func() {
			// Handle transient "pebble: closed" panic during checkpoint restore
			if r := recover(); r != nil {
				t.Logf("Recovered from panic during ledger check: %v", r)
			}
		}()

		// Check all ledgers
		for i := range numLedgers {
			if !listLedgerContains(follower.Store, fmt.Sprintf("ledger-%d", i)) {
				return false
			}
		}
		t.Logf("Follower synced after restart: has all %d ledgers", numLedgers)
		return true
	}, 5*time.Second, 50*time.Millisecond)

	// Verify leader still has all ledgers (using refreshed leader reference)
	for i := range numLedgers {
		require.True(t, listLedgerContains(leader.Store, fmt.Sprintf("ledger-%d", i)),
			"leader should have ledger-%d", i)
	}

	// Make sure node is still running without errors
	select {
	case err := <-newNodeError:
		t.Fatalf("Restarted node failed unexpectedly: %v", err)
	default:
		// Node is still running, good
	}

	t.Log("Test passed: node recovered correctly after checkpoint restore failure")
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
		return listLedgerContains(follower.Store, "test-ledger")
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
	// Create another ledger on the leader
	_, err = createLedger(ctx, leader.Node, "another-ledger")
	require.NoError(t, err)
	t.Logf("Created another ledger on leader")

	// Verify the ledger is replicated to the restarted follower
	require.Eventually(t, func() bool {
		if listLedgerContains(follower.Store, "another-ledger") {
			t.Logf("Follower synced: has another-ledger")
			return true
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "ledger should be replicated to restarted follower")

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

	// Apply some ledgers but not enough to trigger snapshot yet
	// Threshold is 10, so we apply 8 ledgers
	numInitialLedgers := 8
	t.Logf("Creating %d initial ledgers (not triggering snapshot yet)", numInitialLedgers)
	for i := range numInitialLedgers {
		_, err := createLedger(ctx, node.Node, fmt.Sprintf("ledger-%d", i))
		require.NoError(t, err)
	}

	// Verify initial ledgers exist
	for i := range numInitialLedgers {
		require.True(t, listLedgerContains(node.Store, fmt.Sprintf("ledger-%d", i)), "ledger-%d should exist", i)
	}
	t.Logf("Initial %d ledgers verified", numInitialLedgers)

	// Set up interceptor to make DefaultWAL.CreateSnapshot fail
	var errSnapshotFailed = errors.New("simulated DefaultWAL snapshot failure")
	node.WALInterceptor.SetCreateSnapshotInterceptor(func(delegate wal.WAL, i uint64, r *raftpb.ConfState, data []byte) error {
		t.Logf("DefaultWAL.CreateSnapshot called at index %d - returning error", i)
		return errSnapshotFailed
	})

	// Now apply more ledgers to trigger snapshot (need 2 more to reach threshold of 10)
	numTriggerLedgers := 2
	t.Logf("Creating %d more ledgers to trigger snapshot", numTriggerLedgers)
	for i := range numTriggerLedgers {
		go func(idx int) {
			_, _ = createLedger(ctx, node.Node, fmt.Sprintf("trigger-ledger-%d", idx))
		}(i)
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

	// Calculate expected total ledgers after all operations
	totalLedgers := numInitialLedgers + numTriggerLedgers

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
		// Check all initial ledgers exist
		for i := range numInitialLedgers {
			if !listLedgerContains(node.Store, fmt.Sprintf("ledger-%d", i)) {
				t.Logf("ledger-%d not found yet", i)
				return false
			}
		}
		// Check all trigger ledgers exist
		for i := range numTriggerLedgers {
			if !listLedgerContains(node.Store, fmt.Sprintf("trigger-ledger-%d", i)) {
				t.Logf("trigger-ledger-%d not found yet", i)
				return false
			}
		}
		t.Logf("State verified after restart: %d ledgers exist", totalLedgers)
		return true
	}, 5*time.Second, 200*time.Millisecond)

	// Make sure node is still running without errors
	select {
	case err := <-newNodeError:
		t.Fatalf("Restarted node failed unexpectedly: %v", err)
	default:
		// Node is still running, good
	}

	t.Log("Test passed: node recovered correctly after DefaultWAL snapshot failure, entries replayed correctly")
}
