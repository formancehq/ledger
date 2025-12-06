package raft

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft/fsm"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type Cluster struct {
	node          *NodeWrapper
	fsm           *fsm.FSM
	storage       *Storage
	transport     *Transport
	config        *config.Config
	logger        *zap.Logger
	grpcServer    *grpc.Server
	grpcClient    *grpc.Client
	ledgerService service.Ledger // Routed ledger service that routes to bucket Raft groups
	ctx           context.Context
	cancel        context.CancelFunc
	nodeID        uint64
	bucketGroups  map[string]*BucketRaftGroup // Map of bucket name -> bucket Raft group
	muGroups      sync.RWMutex                // Mutex for bucketGroups map
}

func NewRaftCluster(parentCtx context.Context, cfg *config.Config, logger *zap.Logger) (*Cluster, error) {
	// Create data directory if it doesn't exist
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("creating data directory: %w", err)
	}

	ctx, cancel := context.WithCancel(parentCtx)

	// Use numeric node ID directly from config
	nodeID := cfg.NodeID

	// Create storage for etcd/raft
	storage, err := NewStorage(cfg.DataDir, logger)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating storage: %w", err)
	}

	// Create transport
	transport := NewTransport(nodeID, cfg.BindAddr, logger)
	// Note: transport.Start() is not called here because we use a unified gRPC server
	// The transport will be registered on the unified server instead

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
	rawNode, err := raft.NewRawNode(raftConfig)
	if err != nil {
		cancel()
		transport.Stop()
		return nil, fmt.Errorf("creating raw node: %w", err)
	}

	// Wrap the RawNode with our wrapper
	node := NewNodeWrapper(rawNode, logger)

	// Build peers list if bootstrap and storage is empty
	if cfg.Bootstrap {
		// Only bootstrap if storage is empty
		if !storage.IsEmpty() {
			logger.Info("Storage is not empty, skipping bootstrap")
		} else {
			peers := make([]raft.Peer, 0, len(cfg.Peers)+1)
			peers = append(peers, raft.Peer{ID: nodeID})

			// Add peers if provided
			// Peers are in format "<id>/<address>", parse them
			for _, peerEntry := range cfg.Peers {
				parts := strings.SplitN(peerEntry, "/", 2)
				if len(parts) != 2 {
					logger.Warn("Invalid peer format, skipping", zap.String("peer", peerEntry))
					continue
				}
				peerIDStr := parts[0]

				peerID, err := strconv.ParseUint(peerIDStr, 10, 64)
				if err != nil {
					logger.Warn("Invalid peer ID, skipping", zap.String("peer", peerEntry), zap.Error(err))
					continue
				}

				peers = append(peers, raft.Peer{ID: peerID})
			}

			// Bootstrap the cluster
			if err := node.RawNode().Bootstrap(peers); err != nil {
				cancel()
				transport.Stop()
				return nil, fmt.Errorf("bootstrapping cluster: %w", err)
			}
			logger.Info("Cluster bootstrapped", zap.Int("peers", len(peers)))
		}
	}

	// Create FSM (Finite State Machine)
	mainFSM := fsm.NewFSM(logger)

	// Extract port from BindAddr for the unified gRPC server
	// The unified server listens on the same port as Raft transport (BindAddr)
	_, raftPort, err := net.SplitHostPort(cfg.BindAddr)
	if err != nil {
		cancel()
		transport.Stop()
		return nil, fmt.Errorf("invalid bind address format: %w", err)
	}
	grpcPort, err := strconv.Atoi(raftPort)
	if err != nil {
		cancel()
		transport.Stop()
		return nil, fmt.Errorf("invalid port in bind address: %w", err)
	}

	cluster := &Cluster{
		node:         node,
		fsm:          mainFSM,
		storage:      storage,
		transport:    transport,
		config:       cfg,
		logger:       logger,
		grpcClient:   grpc.NewClient(logger),
		ctx:          ctx,
		cancel:       cancel,
		nodeID:       nodeID,
		bucketGroups: make(map[string]*BucketRaftGroup),
	}

	// Create a routed ledger service that routes to the appropriate bucket
	routedLedger := service.NewRoutedLedger(cluster, &bucketLedgerRouter{cluster: cluster}, logger)
	cluster.ledgerService = routedLedger
	cluster.grpcServer = grpc.NewServer(grpcPort, logger, routedLedger, transport, cluster)

	// Add peers to transport
	// Peers are in format "<id>/<address>", parse them
	for _, peerEntry := range cfg.Peers {
		parts := strings.SplitN(peerEntry, "/", 2)
		if len(parts) != 2 {
			logger.Warn("Invalid peer format, skipping", zap.String("peer", peerEntry))
			continue
		}
		peerIDStr := parts[0]
		peerAddr := parts[1]

		peerID, err := strconv.ParseUint(peerIDStr, 10, 64)
		if err != nil {
			logger.Warn("Invalid peer ID, skipping", zap.String("peer", peerEntry), zap.Error(err))
			continue
		}

		transport.AddPeer(peerID, peerAddr)
	}

	return cluster, nil
}

func (r *Cluster) Start() error {
	// Start the unified gRPC server (always running to receive Raft messages)
	go func() {
		if err := r.grpcServer.Start(r.ctx); err != nil {
			r.logger.Error("Unified gRPC server error", zap.Error(err))
		}
	}()

	// Start the Ready loop - it will receive all messages and route them appropriately
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
			r.node.RawNode().Tick()
		case msg := <-r.transport.Recv():
			// Route messages: if node ID >= 0x10000, it's for a bucket group
			// Otherwise, it's for the main cluster
			if msg.To >= 0x10000 || msg.From >= 0x10000 {
				// Route to appropriate bucket group
				r.logger.Debug("Received message for bucket group", zap.String("from", fmt.Sprintf("%x", msg.From)), zap.String("to", fmt.Sprintf("%x", msg.To)))
				r.routeMessageToBucketGroup(msg)
			} else {
				r.logger.Debug("Received message for main cluster", zap.String("from", fmt.Sprintf("%x", msg.From)), zap.String("to", fmt.Sprintf("%x", msg.To)))
				// Process message for main cluster
				r.node.RawNode().Step(msg)
			}
		case peerID := <-r.transport.Unreachable():
			// Report unreachable peer to Raft
			// If peerID >= 0x10000, it's for a bucket group, route it there
			if peerID >= 0x10000 {
				r.logger.Debug("Received unreachable message for bucket group", zap.String("peer", fmt.Sprintf("%x", peerID)))
				r.routeUnreachableToBucketGroup(peerID)
			} else {
				// Report to main cluster
				r.logger.Debug("Received unreachable message for main cluster", zap.String("peer", fmt.Sprintf("%x", peerID)))
				r.node.RawNode().ReportUnreachable(peerID)
			}
		}

		// Process Ready structures
		for r.node.RawNode().HasReady() {
			rd := r.node.RawNode().Ready()

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
				// Start Raft groups for existing buckets from FSM after snapshot restoration
				// This ensures that buckets created before this node started have their Raft groups running
				r.startBucketRaftGroupsFromFSM()
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
						zap.String("nodeID", fmt.Sprintf("%x", cc.NodeID)))
					// Apply the conf change to update the ConfState
					r.node.RawNode().ApplyConfChange(cc)
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
				// Decode the command to get its ID
				var cmd service.Command
				if err := cmd.UnmarshalBinary(entry.Data); err != nil {
					r.logger.Error("Failed to unmarshal command for notification", zap.Uint64("index", entry.Index), zap.Error(err))
					continue
				}

				applyErr := r.applyEntry(entry)
				// Notify the wrapper that this command has been applied using its ID
				r.node.NotifyApplied(cmd.ID, entry.Index, applyErr)
				if applyErr != nil {
					r.logger.Error("Failed to apply entry", zap.Uint64("index", entry.Index), zap.Uint64("commandID", cmd.ID), zap.Error(applyErr))
				}
			}

			// Advance the node
			r.node.RawNode().Advance(rd)
		}
	}
}

// applyEntry applies a Raft log entry to the FSM
func (r *Cluster) applyEntry(entry raftpb.Entry) error {
	// Decode the command from the Raft log data
	var cmd service.Command
	if err := cmd.UnmarshalBinary(entry.Data); err != nil {
		return fmt.Errorf("unmarshaling command: %w", err)
	}

	// Route to the appropriate command handler in FSM
	var err error
	switch cmd.Type {
	// Note: InsertLogs and CreateLedger are now handled by bucket Raft groups, not the main cluster
	case fsm.CommandTypeCreateBucket:
		err = r.fsm.HandleCreateBucket(cmd, entry.Index)
		if err == nil {
			// Start Raft group for the bucket after successful creation
			// The bucket Raft group info is now stored in the FSM, so we retrieve it from there
			var createCmd fsm.CreateBucketCommand
			if unmarshalErr := fsm.UnmarshalCommandData(cmd.Data, &createCmd); unmarshalErr == nil {
				bucketInfo, exists := r.fsm.GetBucket(createCmd.Name)
				if exists {
					bucketRaftGroups := r.fsm.GetAllBucketRaftGroups()
					bucketID, hasGroup := bucketRaftGroups[createCmd.Name]
					if hasGroup {
						if startErr := r.startBucketRaftGroupFromFSM(createCmd.Name, bucketID, bucketInfo); startErr != nil {
							r.logger.Error("Failed to start bucket Raft group", zap.String("bucket", createCmd.Name), zap.Error(startErr))
							// Don't fail the entry application, just log the error
						}
					}
				}
			}
		}
	case fsm.CommandTypeDeleteBucket:
		err = r.fsm.HandleDeleteBucket(cmd, entry.Index)
		if err == nil {
			// Stop Raft group for the bucket after successful deletion
			var deleteCmd fsm.DeleteBucketCommand
			if unmarshalErr := fsm.UnmarshalCommandData(cmd.Data, &deleteCmd); unmarshalErr == nil {
				if stopErr := r.stopBucketRaftGroup(deleteCmd.Name); stopErr != nil {
					r.logger.Error("Failed to stop bucket Raft group", zap.String("bucket", deleteCmd.Name), zap.Error(stopErr))
					// Don't fail the entry application, just log the error
				}
			}
		}
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	return err
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
			status := r.node.RawNode().Status()
			leaderID := status.Lead

			// Check if leader changed
			if leaderID != lastLeaderID {
				oldLeaderAddr := r.findPeerAddress(lastLeaderID)
				if oldLeaderAddr == "" && lastLeaderID != 0 && lastLeaderID != raft.None {
					oldLeaderAddr = fmt.Sprintf("%x", lastLeaderID)
				}
				newLeaderAddr := r.findPeerAddress(leaderID)
				if newLeaderAddr == "" && leaderID != 0 && leaderID != raft.None {
					newLeaderAddr = fmt.Sprintf("%x", leaderID)
				}
				r.logger.Info("Leader changed",
					zap.String("oldLeaderID", fmt.Sprintf("%x", lastLeaderID)),
					zap.String("old", oldLeaderAddr),
					zap.String("newLeaderID", fmt.Sprintf("%x", leaderID)),
					zap.String("new", newLeaderAddr))
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
						r.logger.Info("Leader appears unreachable, reporting", zap.String("leader", leaderAddr), zap.Error(err))
						r.node.RawNode().ReportUnreachable(leaderID)
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
		r.logger.Info("Became leader")
		// Stop client if running (we are now the leader, don't need to connect to ourselves)
		r.grpcClient.Close()
		// Note: The unified gRPC server is already running from Start()
	} else if leaderID != 0 {
		// Find leader address from transport or config
		leaderAddr := r.findPeerAddress(leaderID)
		if leaderAddr == "" {
			r.logger.Error("Failed to find leader address", zap.String("leaderID", fmt.Sprintf("%x", leaderID)))
			return
		}
		r.logger.Info("Leader changed, connecting to new leader", zap.String("leader", leaderAddr))
		// Note: The unified gRPC server stays running to receive Raft messages

		// Connect to leader's gRPC server (same address as Raft transport)
		// The unified gRPC server listens on the same port as Raft transport
		grpcAddr := leaderAddr

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
		// Stop client (no leader to connect to)
		// Note: The unified gRPC server stays running to receive Raft messages
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
	// Peers are in format "<id>/<address>", parse them
	for _, peerEntry := range r.config.Peers {
		parts := strings.SplitN(peerEntry, "/", 2)
		if len(parts) != 2 {
			continue
		}
		peerIDStr := parts[0]
		peerAddr := parts[1]

		id, err := strconv.ParseUint(peerIDStr, 10, 64)
		if err != nil {
			continue
		}
		if id == peerID {
			return peerAddr
		}
	}

	return ""
}

// routeMessageToBucketGroup routes a message to the appropriate bucket Raft group
func (r *Cluster) routeMessageToBucketGroup(msg raftpb.Message) {
	r.muGroups.RLock()
	defer r.muGroups.RUnlock()

	// Find the bucket group that should receive this message
	// Messages are for bucket groups if To or From >= 0x10000
	targetNodeID := msg.To
	if msg.From >= 0x10000 {
		targetNodeID = msg.From
	}

	// Extract groupID from nodeID: groupID = nodeID & 0xFFFF0000 (upper bits)
	groupID := targetNodeID & 0xFFFF0000

	// Find bucket group with matching groupID
	for _, group := range r.bucketGroups {
		if group.groupID == groupID {
			select {
			case group.msgCh <- msg:
			default:
				r.logger.Warn("Bucket group message channel full, dropping message",
					zap.String("bucket", group.bucketName),
					zap.String("to", fmt.Sprintf("%x", msg.To)),
					zap.String("from", fmt.Sprintf("%x", msg.From)))
			}
			return
		}
	}

	r.logger.Debug("No bucket group found for message",
		zap.String("to", fmt.Sprintf("%x", msg.To)),
		zap.String("from", fmt.Sprintf("%x", msg.From)),
		zap.String("groupID", fmt.Sprintf("%x", groupID)))
}

// routeUnreachableToBucketGroup routes an unreachable peer notification to the appropriate bucket group
func (r *Cluster) routeUnreachableToBucketGroup(peerID uint64) {
	r.muGroups.RLock()
	defer r.muGroups.RUnlock()

	// Extract groupID from peerID: groupID = peerID & 0xFFFF0000 (upper bits)
	groupID := peerID & 0xFFFF0000

	// Extract the actual peer node ID in the bucket group context
	// peerID = groupID + nodeID, so nodeID = peerID - groupID
	// But we need to report the full peerID to the bucket group
	// Actually, etcd/raft expects the node ID as configured, which is groupNodeID
	// So we should report peerID directly

	// Find bucket group with matching groupID
	for _, group := range r.bucketGroups {
		if group.groupID == groupID {
			// Report unreachable to the bucket group's Raft node
			// We need to call ReportUnreachable on the group's node
			// But we can't access it directly from here. We'll need to add a method.
			// For now, we'll create a mechanism to pass this through the message channel
			// Actually, bucket groups should handle this themselves when they receive messages
			// Let's add a method to report unreachable
			group.reportUnreachable(peerID)
			return
		}
	}

	r.logger.Debug("No bucket group found for unreachable peer",
		zap.String("peerID", fmt.Sprintf("%x", peerID)),
		zap.String("groupID", fmt.Sprintf("%x", groupID)))
}

func (r *Cluster) Shutdown() error {
	r.logger.Info("Shutting down Raft cluster")

	// Stop all bucket Raft groups
	r.muGroups.Lock()
	for bucketName, group := range r.bucketGroups {
		if err := group.Stop(); err != nil {
			r.logger.Error("Failed to stop bucket Raft group", zap.String("bucket", bucketName), zap.Error(err))
		}
	}
	r.bucketGroups = make(map[string]*BucketRaftGroup)
	r.muGroups.Unlock()

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

	return nil
}

func (r *Cluster) GetRaft() *raft.RawNode {
	return r.node.RawNode()
}

// GetLedgerService returns the ledger service that routes to bucket Raft groups
func (r *Cluster) GetLedgerService() service.Ledger {
	return r.ledgerService
}

func (r *Cluster) GetGRPCClient() service.GRPCClient {
	return r.grpcClient
}

// bucketLedgerRouter routes ledger requests to the appropriate bucket's DefaultLedger
type bucketLedgerRouter struct {
	cluster *Cluster
}

// getBucketLedger gets the DefaultLedger for a given ledger name
func (r *bucketLedgerRouter) getBucketLedger(ledgerName string) (*service.DefaultLedger, error) {
	// Find the bucket containing this ledger
	bucketName, err := r.cluster.FindBucketForLedger(ledgerName)
	if err != nil {
		return nil, fmt.Errorf("finding bucket for ledger %s: %w", ledgerName, err)
	}

	// Get the bucket Raft group
	r.cluster.muGroups.RLock()
	bucketGroup, exists := r.cluster.bucketGroups[bucketName]
	r.cluster.muGroups.RUnlock()

	if !exists {
		return nil, fmt.Errorf("bucket Raft group not found for bucket: %s", bucketName)
	}

	// Get the default ledger from the bucket group
	defaultLedger := bucketGroup.GetDefaultLedger()
	if defaultLedger == nil {
		return nil, fmt.Errorf("default ledger not found for bucket: %s", bucketName)
	}

	return defaultLedger, nil
}

// CreateTransaction routes the transaction creation to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) CreateTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[service.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return nil, nil, err
	}
	return defaultLedger.CreateTransaction(ctx, ledgerName, parameters)
}

// RevertTransaction routes the transaction reversion to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) RevertTransaction(ctx context.Context, ledgerName string, parameters service.Parameters[service.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return nil, nil, err
	}
	return defaultLedger.RevertTransaction(ctx, ledgerName, parameters)
}

// SaveTransactionMetadata routes the metadata save to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) SaveTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.SaveTransactionMetadata]) (*ledger.Log, error) {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return nil, err
	}
	return defaultLedger.SaveTransactionMetadata(ctx, ledgerName, parameters)
}

// SaveAccountMetadata routes the account metadata save to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) SaveAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.SaveAccountMetadata]) (*ledger.Log, error) {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return nil, err
	}
	return defaultLedger.SaveAccountMetadata(ctx, ledgerName, parameters)
}

// DeleteTransactionMetadata routes the metadata deletion to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) DeleteTransactionMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.DeleteTransactionMetadata]) (*ledger.Log, error) {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return nil, err
	}
	return defaultLedger.DeleteTransactionMetadata(ctx, ledgerName, parameters)
}

// DeleteAccountMetadata routes the account metadata deletion to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) DeleteAccountMetadata(ctx context.Context, ledgerName string, parameters service.Parameters[service.DeleteAccountMetadata]) (*ledger.Log, error) {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return nil, err
	}
	return defaultLedger.DeleteAccountMetadata(ctx, ledgerName, parameters)
}

// Import routes the import to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) Import(ctx context.Context, ledgerName string, stream chan ledger.Log) error {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return err
	}
	return defaultLedger.Import(ctx, ledgerName, stream)
}

// Export routes the export to the appropriate bucket's ledger service
func (r *bucketLedgerRouter) Export(ctx context.Context, ledgerName string, w service.ExportWriter) error {
	defaultLedger, err := r.getBucketLedger(ledgerName)
	if err != nil {
		return err
	}
	return defaultLedger.Export(ctx, ledgerName, w)
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
		snapshotData, err := r.fsm.CreateSnapshot()
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
		leader = fmt.Sprintf("%x", leaderID)
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
			addr = fmt.Sprintf("%x", id)
		}
		nodes = append(nodes, http.NodeInfo{
			ID:       fmt.Sprintf("%x", id),
			Address:  addr,
			Suffrage: suffrage,
		})
	}

	return &http.ClusterState{
		State:     stateStr,
		Leader:    leader,
		Nodes:     nodes,
		LocalNode: fmt.Sprintf("%x", r.config.NodeID),
	}, nil
}

// CreateLedger creates a new ledger in a bucket via the bucket's Raft group
func (r *Cluster) CreateLedger(bucketName, ledgerName string, metadata metadata.Metadata) error {
	// Check if ledger already exists in any bucket (global uniqueness)
	r.muGroups.RLock()
	for name, group := range r.bucketGroups {
		if _, exists := group.GetLedger(ledgerName); exists {
			r.muGroups.RUnlock()
			return fmt.Errorf("ledger with name %s already exists in bucket %s", ledgerName, name)
		}
	}
	r.muGroups.RUnlock()

	// Get the bucket Raft group
	r.muGroups.RLock()
	group, exists := r.bucketGroups[bucketName]
	r.muGroups.RUnlock()

	if !exists {
		return fmt.Errorf("bucket %s not found or Raft group not started", bucketName)
	}

	// Create ledger via bucket Raft group
	if err := group.CreateLedger(ledgerName, metadata); err != nil {
		return fmt.Errorf("creating ledger in bucket %s: %w", bucketName, err)
	}

	r.logger.Info("Ledger creation proposed via bucket Raft", zap.String("bucket", bucketName), zap.String("name", ledgerName))
	return nil
}

// FindBucketForLedger finds the bucket that contains a ledger with the given name
func (r *Cluster) FindBucketForLedger(ledgerName string) (string, error) {
	r.muGroups.RLock()
	bucketNames := make([]string, 0, len(r.bucketGroups))
	for name := range r.bucketGroups {
		bucketNames = append(bucketNames, name)
	}
	r.muGroups.RUnlock()

	for _, bucketName := range bucketNames {
		r.muGroups.RLock()
		group, exists := r.bucketGroups[bucketName]
		r.muGroups.RUnlock()

		if !exists {
			continue
		}

		_, ledgerExists := group.GetLedger(ledgerName)
		if ledgerExists {
			return bucketName, nil
		}
	}

	return "", fmt.Errorf("ledger %s not found in any bucket", ledgerName)
}

// GetLedger retrieves a ledger from a bucket
func (r *Cluster) GetLedger(bucketName, ledgerName string) (service.LedgerInfo, bool, error) {
	// Get the bucket Raft group
	r.muGroups.RLock()
	group, exists := r.bucketGroups[bucketName]
	r.muGroups.RUnlock()

	if !exists {
		return service.LedgerInfo{}, false, fmt.Errorf("bucket %s not found or Raft group not started", bucketName)
	}

	// Get ledger from bucket Raft group
	ledgerInfo, exists := group.GetLedger(ledgerName)
	if !exists {
		return service.LedgerInfo{}, false, nil
	}

	return ledgerInfo, true, nil
}

// GetLedgerByName retrieves a ledger by its name, finding the bucket automatically
func (r *Cluster) GetLedgerByName(ledgerName string) (service.LedgerInfo, string, bool, error) {
	bucketName, err := r.FindBucketForLedger(ledgerName)
	if err != nil {
		return service.LedgerInfo{}, "", false, nil
	}

	ledgerInfo, exists, err := r.GetLedger(bucketName, ledgerName)
	if err != nil {
		return service.LedgerInfo{}, "", false, err
	}
	return ledgerInfo, bucketName, exists, nil
}

// GetAllLedgers retrieves all ledgers from a bucket
func (r *Cluster) GetAllLedgers(bucketName string) (map[string]service.LedgerInfo, error) {
	// Get the bucket Raft group
	r.muGroups.RLock()
	group, exists := r.bucketGroups[bucketName]
	r.muGroups.RUnlock()

	if !exists {
		return nil, fmt.Errorf("bucket %s not found or Raft group not started", bucketName)
	}

	// Get all ledgers from bucket Raft group
	ledgers := group.GetAllLedgers()

	return ledgers, nil
}

// CreateBucket creates a new bucket via a FSM command
func (r *Cluster) CreateBucket(name, driver string, config map[string]interface{}) error {
	// Create the command
	cmd, err := fsm.NewCreateBucketCommand(name, driver, config)
	if err != nil {
		return fmt.Errorf("creating create bucket command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, err = r.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command via raft: %w", err)
	}

	r.logger.Info("Bucket created via Raft", zap.String("name", name), zap.String("driver", driver), zap.Uint64("commandID", cmd.ID))
	return nil
}

// GetBucket returns the bucket info for a given name
func (r *Cluster) GetBucket(name string) (service.BucketInfo, bool) {
	return r.fsm.GetBucket(name)
}

// GetAllBuckets returns all buckets
func (r *Cluster) GetAllBuckets() map[string]service.BucketInfo {
	return r.fsm.GetAllBuckets()
}

// DeleteBucket deletes a bucket via a FSM command
func (r *Cluster) DeleteBucket(name string) error {
	// Create the command
	cmd, err := fsm.NewDeleteBucketCommand(name)
	if err != nil {
		return fmt.Errorf("creating delete bucket command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, err = r.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command via raft: %w", err)
	}

	r.logger.Info("Bucket deleted via Raft", zap.String("name", name), zap.Uint64("commandID", cmd.ID))
	return nil
}

// startBucketRaftGroupsFromFSM starts all Raft groups for existing buckets from FSM
func (r *Cluster) startBucketRaftGroupsFromFSM() {
	bucketRaftGroups := r.fsm.GetAllBucketRaftGroups()
	buckets := r.fsm.GetAllBuckets()
	for bucketName, bucketID := range bucketRaftGroups {
		bucketInfo, exists := buckets[bucketName]
		if !exists {
			r.logger.Warn("Bucket Raft group found but bucket info missing", zap.String("bucket", bucketName))
			continue
		}
		if err := r.startBucketRaftGroupFromFSM(bucketName, bucketID, bucketInfo); err != nil {
			r.logger.Error("Failed to start Raft group for existing bucket", zap.String("bucket", bucketName), zap.Error(err))
			// Continue with other buckets even if one fails
		}
	}
}

// startBucketRaftGroupFromFSM starts a Raft group for a bucket using information from the FSM
func (r *Cluster) startBucketRaftGroupFromFSM(bucketName string, bucketID uint64, bucketInfo service.BucketInfo) error {
	r.muGroups.Lock()
	defer r.muGroups.Unlock()

	// Check if group already exists
	if _, exists := r.bucketGroups[bucketName]; exists {
		r.logger.Warn("Bucket Raft group already exists", zap.String("bucket", bucketName))
		return nil
	}

	// Create new bucket Raft group
	group, err := NewBucketRaftGroup(r.ctx, bucketName, bucketID, bucketInfo, r.transport, r.config, r.logger)
	if err != nil {
		return fmt.Errorf("creating bucket Raft group: %w", err)
	}

	// Start the group
	if err := group.Start(); err != nil {
		return fmt.Errorf("starting bucket Raft group: %w", err)
	}

	// Store the group
	r.bucketGroups[bucketName] = group

	r.logger.Info("Bucket Raft group started from FSM", zap.String("bucket", bucketName), zap.Uint64("bucketID", bucketID))
	return nil
}

// stopBucketRaftGroup stops a Raft group for a bucket
func (r *Cluster) stopBucketRaftGroup(bucketName string) error {
	r.muGroups.Lock()
	defer r.muGroups.Unlock()

	group, exists := r.bucketGroups[bucketName]
	if !exists {
		r.logger.Warn("Bucket Raft group does not exist", zap.String("bucket", bucketName))
		return nil
	}

	// Stop the group
	if err := group.Stop(); err != nil {
		return fmt.Errorf("stopping bucket Raft group: %w", err)
	}

	// Remove from map
	delete(r.bucketGroups, bucketName)

	r.logger.Info("Bucket Raft group stopped", zap.String("bucket", bucketName))
	return nil
}

// GetBucketRaftGroup returns the Raft group for a bucket
func (r *Cluster) GetBucketRaftGroup(bucketName string) (*BucketRaftGroup, bool) {
	r.muGroups.RLock()
	defer r.muGroups.RUnlock()
	group, exists := r.bucketGroups[bucketName]
	return group, exists
}

// CreateBucketSnapshot creates a snapshot for a specific bucket's Raft group
func (r *Cluster) CreateBucketSnapshot(bucketName string) error {
	group, exists := r.GetBucketRaftGroup(bucketName)
	if !exists {
		return fmt.Errorf("bucket does not exist: %s", bucketName)
	}

	return group.Snapshot()
}

// GetBucketWithRaftState returns a bucket with its Raft cluster state
func (r *Cluster) GetBucketWithRaftState(name string) (*http.BucketWithRaftState, error) {
	// Get bucket info from FSM
	bucketInfo, exists := r.fsm.GetBucket(name)
	if !exists {
		return nil, nil // Bucket not found
	}

	// Get Raft group state if it exists
	var raftState *http.ClusterState
	group, exists := r.GetBucketRaftGroup(name)
	if exists {
		raftState = group.GetRaftState()
	}

	return &http.BucketWithRaftState{
		BucketInfo: bucketInfo,
		RaftState:  raftState,
	}, nil
}
