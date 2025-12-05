package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type Cluster struct {
	node          *raft.RawNode
	fsm           *FSM
	storage       *Storage
	transport     *Transport
	config        *config.Config
	logger        *zap.Logger
	grpcServer    *grpc.Server
	grpcClient    *grpc.Client
	defaultLedger *service.DefaultLedger
	logStore      service.LogStore // Can be SQLiteLogStore or FileLogStore
	closeStore    func() error     // Function to close the store
	ctx           context.Context
	cancel        context.CancelFunc
	nodeID        uint64
}

func NewRaftCluster(parentCtx context.Context, cfg *config.Config, logger *zap.Logger) (*Cluster, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	ctx, cancel := context.WithCancel(parentCtx)

	// Convert node ID to uint64 (simple hash for now, you might want to use a proper ID system)
	nodeID := uint64(0)
	for _, c := range cfg.NodeID {
		nodeID = nodeID*31 + uint64(c)
	}

	// Create storage for etcd/raft
	storage, err := NewStorage(cfg.DataDir, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	// Create transport
	transport := NewTransport(nodeID, cfg.BindAddr, logger)
	if err := transport.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("starting transport: %w", err)
	}

	// Create Raft configuration
	raftConfig := &raft.Config{
		ID:              nodeID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
		// Logger is optional in etcd/raft
	}

	// Configure snapshot parameters
	if cfg.SnapshotThreshold > 0 {
		// etcd/raft doesn't have SnapshotThreshold, we'll handle it manually
	}

	// Create RawNode
	node, err := raft.NewRawNode(raftConfig)
	if err != nil {
		cancel()
		transport.Stop()
		return nil, fmt.Errorf("creating raw node: %w", err)
	}

	// Build peers list if bootstrap and storage is empty
	if cfg.Bootstrap {
		// Only bootstrap if storage is empty
		if !storage.IsEmpty() {
			logger.Info("Storage is not empty, skipping bootstrap")
		} else {
			peers := make([]raft.Peer, 0, len(cfg.Peers)+1)
			peers = append(peers, raft.Peer{ID: nodeID})

			// Add peers if provided
			for _, peerAddr := range cfg.Peers {
				// Extract hostname from address (e.g., "node-1:8888" -> "node-1")
				host, _, err := net.SplitHostPort(peerAddr)
				if err != nil {
					logger.Warn("Invalid peer address format, skipping", zap.String("peer", peerAddr), zap.Error(err))
					continue
				}
				// Convert hostname to uint64 (simple hash)
				peerID := uint64(0)
				for _, c := range host {
					peerID = peerID*31 + uint64(c)
				}
				peers = append(peers, raft.Peer{ID: peerID})
			}

			// Bootstrap the cluster
			if err := node.Bootstrap(peers); err != nil {
				cancel()
				transport.Stop()
				return nil, fmt.Errorf("bootstrapping cluster: %w", err)
			}
			logger.Info("Cluster bootstrapped", zap.Int("peers", len(peers)))
		}
	}

	// Create application log store based on storage type
	var appLogStore service.LogStore
	var closeStore func() error

	switch cfg.StorageType {
	case "sqlite":
		sqliteStore, err := service.NewSQLiteLogStore(ctx, cfg.SQLiteDSN, logger)
		if err != nil {
			cancel() // Clean up context on error
			transport.Stop()
			return nil, fmt.Errorf("creating sqlite store: %w", err)
		}
		appLogStore = sqliteStore
		closeStore = func() error {
			return sqliteStore.Close()
		}
	case "file":
		fileStore, err := service.NewFileLogStore(cfg.StorageFilePath, logger)
		if err != nil {
			cancel() // Clean up context on error
			transport.Stop()
			return nil, fmt.Errorf("creating file store: %w", err)
		}
		appLogStore = fileStore
		closeStore = func() error {
			return fileStore.Close()
		}
	default:
		cancel() // Clean up context on error
		transport.Stop()
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.StorageType)
	}

	// Create FSM (Finite State Machine) with application log store
	fsm := NewFSM(logger, appLogStore, appLogStore)

	// Create RaftLogWriter for writing logs via Raft (using node instead of raft)
	raftLogWriter := service.NewRaftLogWriter(node, logger)

	// Create reconstructed volumes store (reconstructs volumes from logs)
	volumesStore := service.NewReconstructedVolumesStore(appLogStore)

	// Wrap volumes store with locked volumes store for concurrent access control
	lockedVolumesStore := service.NewDefaultLockedVolumesStore(volumesStore)

	// Create ledger service (will use RaftLogWriter to persist logs via Raft)
	// appLogStore implements LogReader, lockedVolumesStore implements LockedVolumesStore
	defaultLedger := service.NewDefaultLedger(raftLogWriter, lockedVolumesStore, appLogStore, logger)

	cluster := &Cluster{
		node:          node,
		fsm:           fsm,
		storage:       storage,
		transport:     transport,
		config:        cfg,
		logger:        logger,
		grpcServer:    grpc.NewServer(cfg.GRPCPort, logger, defaultLedger),
		grpcClient:    grpc.NewClient(logger),
		defaultLedger: defaultLedger,
		logStore:      appLogStore,
		ctx:           ctx,
		cancel:        cancel,
		nodeID:        nodeID,
	}

	// Store close function for shutdown
	cluster.closeStore = closeStore

	// Add peers to transport
	for _, peerAddr := range cfg.Peers {
		host, _, err := net.SplitHostPort(peerAddr)
		if err != nil {
			continue
		}
		peerID := uint64(0)
		for _, c := range host {
			peerID = peerID*31 + uint64(c)
		}
		transport.AddPeer(peerID, peerAddr)
	}

	return cluster, nil
}

func (r *Cluster) Start() error {
	// Start the Ready loop
	go r.readyLoop()

	// Start leader monitoring
	go r.monitorLeader()

	return nil
}

// readyLoop processes Ready structures from etcd/raft
func (r *Cluster) readyLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			r.node.Tick()
		case msg := <-r.transport.Recv():
			// Process incoming messages from transport
			r.node.Step(msg)
		case peerID := <-r.transport.Unreachable():
			// Report unreachable peer to Raft
			r.logger.Info("Reporting peer as unreachable", zap.Uint64("peer", peerID))
			r.node.ReportUnreachable(peerID)
			// Check if we should trigger an election
			status := r.node.Status()
			if status.RaftState == raft.StateFollower && status.Lead == peerID {
				r.logger.Info("Leader is unreachable, checking if election should be triggered", zap.Uint64("leader", peerID))
			}
		}

		// Process Ready structures
		for r.node.HasReady() {
			rd := r.node.Ready()

			// Save HardState, Entries and Snapshot to storage
			if !raft.IsEmptyHardState(rd.HardState) {
				r.storage.SetHardState(rd.HardState)
			}

			if len(rd.Entries) > 0 {
				if err := r.storage.Append(rd.Entries); err != nil {
					r.logger.Error("Failed to append entries", zap.Error(err))
					continue
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				// Apply snapshot to storage
				if err := r.storage.ApplySnapshot(rd.Snapshot); err != nil {
					r.logger.Error("Failed to apply snapshot to storage", zap.Error(err))
					continue
				}
				// Restore FSM from snapshot
				if err := r.fsm.RestoreSnapshot(rd.Snapshot.Data); err != nil {
					r.logger.Error("Failed to restore FSM from snapshot", zap.Error(err))
					continue
				}
			}

			// Send messages via transport
			for _, msg := range rd.Messages {
				r.transport.Send(msg)
			}

			// Apply committed entries to FSM
			for _, entry := range rd.CommittedEntries {
				// Configuration change entries must be applied to update the ConfState
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						r.logger.Error("Failed to unmarshal ConfChange", zap.Error(err))
						continue
					}
					r.logger.Info("Applying configuration change",
						zap.String("type", cc.Type.String()),
						zap.Uint64("nodeID", cc.NodeID))
					// Apply the conf change to update the ConfState
					r.node.ApplyConfChange(cc)
					continue
				}
				// Skip other non-normal entries
				if entry.Type != raftpb.EntryNormal {
					r.logger.Debug("Skipping non-normal entry", zap.Uint64("index", entry.Index), zap.Uint64("type", uint64(entry.Type)))
					continue
				}
				// Skip empty entries (they might be used for heartbeat or other Raft internal purposes)
				if len(entry.Data) == 0 {
					r.logger.Debug("Skipping empty entry", zap.Uint64("index", entry.Index))
					continue
				}
				if err := r.applyEntry(entry); err != nil {
					r.logger.Error("Failed to apply entry", zap.Uint64("index", entry.Index), zap.Error(err))
				}
			}

			// Advance the node
			r.node.Advance(rd)
		}
	}
}

// applyEntry applies a Raft log entry to the FSM
func (r *Cluster) applyEntry(entry raftpb.Entry) error {
	// Decode the command from the Raft log data
	var cmd service.Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshaling command: %w", err)
	}

	// Route to the appropriate command handler in FSM
	switch cmd.Type {
	case service.CommandTypeInsertLogs:
		return r.fsm.HandleInsertLogs(cmd.Data, entry.Index)
	case service.CommandTypeCreateLedger:
		return r.fsm.HandleCreateLedger(cmd.Data, entry.Index)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

func (r *Cluster) monitorLeader() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastLeaderID uint64

	for {
		select {
		case <-r.ctx.Done():
			r.logger.Info("Context cancelled, stopping leader monitoring")
			return
		case <-ticker.C:
			status := r.node.Status()
			leaderID := status.Lead

			// Check if leader changed
			if leaderID != lastLeaderID {
				r.logger.Info("Leader changed", zap.Uint64("old", lastLeaderID), zap.Uint64("new", leaderID))
				lastLeaderID = leaderID
				r.handleLeaderChange(leaderID)
			}

			// If we're a follower and have a leader, periodically check if leader is reachable
			if status.RaftState == raft.StateFollower && leaderID != 0 && leaderID != raft.None {
				leaderAddr := r.findPeerAddress(leaderID)
				if leaderAddr != "" {
					// Try a simple TCP connection to verify leader is reachable
					conn, err := net.DialTimeout("tcp", leaderAddr, 500*time.Millisecond)
					if err != nil {
						r.logger.Info("Leader appears unreachable, reporting", zap.Uint64("leader", leaderID), zap.String("addr", leaderAddr), zap.Error(err))
						r.node.ReportUnreachable(leaderID)
					} else {
						conn.Close()
					}
				}
			}
		}
	}
}

func (r *Cluster) handleLeaderChange(leaderID uint64) {
	// Check if we are the leader
	isLeader := leaderID == r.nodeID

	if isLeader {
		r.logger.Info("Became leader, starting gRPC server")
		// Stop client if running
		r.grpcClient.Close()

		// Start gRPC server
		go func() {
			if err := r.grpcServer.Start(r.ctx); err != nil {
				r.logger.Error("gRPC server error", zap.Error(err))
			}
		}()
	} else if leaderID != 0 {
		r.logger.Info("Leader changed, connecting to new leader", zap.Uint64("leader", leaderID))
		// Stop server if running
		r.grpcServer.Stop()

		// Find leader address from transport or config
		leaderAddr := r.findPeerAddress(leaderID)
		if leaderAddr == "" {
			r.logger.Error("Failed to find leader address", zap.Uint64("leaderID", leaderID))
			return
		}

		// Extract hostname from leader address and construct gRPC address
		host, _, err := net.SplitHostPort(leaderAddr)
		if err != nil {
			r.logger.Error("Failed to parse leader address", zap.String("address", leaderAddr), zap.Error(err))
			return
		}

		// Connect to leader's gRPC server (assume same host, different port)
		grpcAddr := fmt.Sprintf("%s:%d", host, r.config.GRPCPort)

		// Retry connection with exponential backoff
		maxRetries := 5
		var lastErr error
		for i := 0; i < maxRetries; i++ {
			if err := r.grpcClient.Connect(r.ctx, grpcAddr); err != nil {
				lastErr = err
				if i < maxRetries-1 {
					backoff := time.Duration(i+1) * 500 * time.Millisecond
					r.logger.Warn("Failed to connect to leader gRPC, retrying",
						zap.String("address", grpcAddr),
						zap.Error(err),
						zap.Int("attempt", i+1),
						zap.Duration("backoff", backoff))
					time.Sleep(backoff)
					continue
				}
			} else {
				r.logger.Info("Successfully connected to leader gRPC", zap.String("address", grpcAddr))
				return
			}
		}
		r.logger.Error("Failed to connect to leader gRPC after retries",
			zap.String("address", grpcAddr),
			zap.Error(lastErr),
			zap.Int("retries", maxRetries))
	} else {
		r.logger.Debug("No leader available")
		// Stop both server and client
		r.grpcServer.Stop()
		r.grpcClient.Close()
	}
}

// findPeerAddress finds the address of a peer by ID
func (r *Cluster) findPeerAddress(peerID uint64) string {
	// First, check if it's the local node
	if peerID == r.nodeID {
		return r.config.AdvertiseAddr
	}

	// Try to find it in the transport (which has the actual peer addresses)
	r.transport.mu.RLock()
	if addr, ok := r.transport.peers[peerID]; ok {
		r.transport.mu.RUnlock()
		return addr
	}
	r.transport.mu.RUnlock()

	// Fallback: try to find it in the config peers
	for _, peerAddr := range r.config.Peers {
		host, _, err := net.SplitHostPort(peerAddr)
		if err != nil {
			continue
		}
		id := uint64(0)
		for _, c := range host {
			id = id*31 + uint64(c)
		}
		if id == peerID {
			return peerAddr
		}
	}

	return ""
}

func (r *Cluster) Shutdown() error {
	r.logger.Info("Shutting down Raft cluster")

	// Cancel context to stop monitoring
	r.cancel()

	// Stop gRPC server and client
	r.grpcServer.Stop()
	r.grpcClient.Close()

	// Stop transport
	r.transport.Stop()

	// Note: etcd/raft RawNode doesn't have a Stop method
	// The node will be stopped when the context is cancelled

	// Close log store
	if r.closeStore != nil {
		if err := r.closeStore(); err != nil {
			return fmt.Errorf("closing log store: %w", err)
		}
	}

	return nil
}

func (r *Cluster) GetRaft() *raft.RawNode {
	return r.node
}

func (r *Cluster) GetGRPCClient() service.GRPCClient {
	return r.grpcClient
}

// GetDefaultLedger returns the default ledger service
func (r *Cluster) GetDefaultLedger() *service.DefaultLedger {
	return r.defaultLedger
}

// Snapshot forces a snapshot of the Raft cluster
func (r *Cluster) Snapshot() error {
	r.logger.Info("Snapshot request received")

	// Check if we are the leader (only leader can create snapshots)
	status := r.node.Status()
	if status.RaftState != raft.StateLeader {
		r.logger.Warn("Snapshot requested but not leader", zap.String("state", status.RaftState.String()))
		return fmt.Errorf("only leader can create snapshots, current state: %v", status.RaftState)
	}

	r.logger.Info("Creating snapshot", zap.Uint64("applied", status.Applied))

	// Trigger snapshot creation
	// In etcd/raft, snapshots are created automatically when needed
	// We can trigger one manually by checking the status
	if status.Applied > 0 {
		r.logger.Debug("Creating snapshot data via FSM", zap.Uint64("applied", status.Applied))
		// Create snapshot data via FSM
		snapshotData, err := r.fsm.CreateSnapshot(status.Applied)
		if err != nil {
			r.logger.Error("Failed to create snapshot data", zap.Error(err))
			return fmt.Errorf("creating snapshot data: %w", err)
		}
		r.logger.Debug("Snapshot data created", zap.Int("size", len(snapshotData)))

		// Get current configuration state from storage
		r.logger.Debug("Getting initial state from storage")
		_, confState, err := r.storage.InitialState()
		if err != nil {
			r.logger.Error("Failed to get initial state", zap.Error(err))
			return fmt.Errorf("getting initial state: %w", err)
		}

		// Create snapshot via storage
		r.logger.Debug("Creating snapshot in storage", zap.Uint64("index", status.Applied))
		_, err = r.storage.CreateSnapshot(status.Applied, &confState, snapshotData)
		if err != nil {
			r.logger.Error("Failed to create snapshot in storage", zap.Error(err))
			return fmt.Errorf("creating snapshot: %w", err)
		}

		r.logger.Info("Snapshot created successfully", zap.Uint64("applied", status.Applied))
	} else {
		r.logger.Warn("No applied entries to snapshot", zap.Uint64("applied", status.Applied))
	}
	return nil
}

// IsHealthy returns true if the node is connected to the cluster (leader or follower)
func (r *Cluster) IsHealthy() bool {
	status := r.node.Status()
	// Node is healthy if it's a leader or follower
	return status.RaftState == raft.StateLeader || status.RaftState == raft.StateFollower
}

// GetClusterState returns the current state of the Raft cluster
func (r *Cluster) GetClusterState() (*http.ClusterState, error) {
	status := r.node.Status()

	// Get leader
	leaderID := status.Lead
	leader := ""
	if leaderID != 0 {
		leader = strconv.FormatUint(leaderID, 10)
	}

	// Convert state to string
	stateStr := "Unknown"
	switch status.RaftState {
	case raft.StateLeader:
		stateStr = "Leader"
	case raft.StateFollower:
		stateStr = "Follower"
	case raft.StateCandidate:
		stateStr = "Candidate"
	case raft.StatePreCandidate:
		stateStr = "PreCandidate"
	}

	// Build nodes list from progress
	nodes := make([]http.NodeInfo, 0)
	for id, progress := range status.Progress {
		suffrage := "Voter"
		if !progress.IsLearner {
			// In etcd/raft, all nodes in Progress are voters unless they're learners
			// We don't have a direct way to check, so assume voters
		}
		addr := r.findPeerAddress(id)
		if addr == "" {
			addr = strconv.FormatUint(id, 10)
		}
		nodes = append(nodes, http.NodeInfo{
			ID:       strconv.FormatUint(id, 10),
			Address:  addr,
			Suffrage: suffrage,
		})
	}

	return &http.ClusterState{
		State:     stateStr,
		Leader:    leader,
		Nodes:     nodes,
		LocalNode: r.config.NodeID,
	}, nil
}

// CreateLedger creates a new ledger via a FSM command
func (r *Cluster) CreateLedger(name string, metadata map[string]string) error {
	// Create the command
	cmd, err := service.NewCreateLedgerCommand(name, metadata)
	if err != nil {
		return fmt.Errorf("creating create ledger command: %w", err)
	}

	// Serialize the command
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("marshaling command: %w", err)
	}

	// Propose the command via Raft (will be applied in readyLoop)
	if err := r.node.Propose(cmdData); err != nil {
		return fmt.Errorf("proposing command via raft: %w", err)
	}

	r.logger.Info("Ledger creation proposed via Raft", zap.String("name", name))
	return nil
}
