package raft

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft/fsm"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// restoreFSMFromStorage restores the FSM state from storage by reading the last snapshot
// and applying all entries after the snapshot
func restoreFSMFromStorage(fsmInstance *fsm.FSM, storage *Storage, logger logging.Logger) error {
	logger.Infof("Restoring FSM from storage")
	// Read the last snapshot
	snapshot, err := storage.Snapshot()
	if err != nil {
		return fmt.Errorf("reading snapshot: %w", err)
	}

	// If snapshot exists, restore FSM from it
	if snapshot.Metadata.Index > 0 {
		logger.WithFields(map[string]any{"index": snapshot.Metadata.Index}).Infof("Restoring FSM from snapshot")
		if err := fsmInstance.RestoreSnapshot(snapshot.Data); err != nil {
			return fmt.Errorf("restoring FSM from snapshot: %w", err)
		}
	} else {
		logger.Infof("No snapshot found, starting with empty FSM")
	}

	// Read all entries after the snapshot
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return fmt.Errorf("getting first index: %w", err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		return fmt.Errorf("getting last index: %w", err)
	}

	// If there are entries after the snapshot, apply them to the FSM
	if firstIndex <= lastIndex {
		logger.WithFields(map[string]any{"firstIndex": firstIndex, "lastIndex": lastIndex}).Infof("Applying entries after snapshot")
		// Read entries in batches to avoid loading everything in memory at once
		const maxBatchSize = 1000
		for i := firstIndex; i <= lastIndex; i += maxBatchSize {
			endIndex := i + maxBatchSize
			if endIndex > lastIndex+1 {
				endIndex = lastIndex + 1
			}

			entries, err := storage.Entries(i, endIndex, 10*1024*1024) // 10MB max size per batch
			if err != nil {
				return fmt.Errorf("reading entries [%d, %d): %w", i, endIndex, err)
			}

			// Apply each entry to the FSM
			for _, entry := range entries {
				// Skip configuration change entries
				if entry.Type == raftpb.EntryConfChange {
					continue
				}
				// Skip other non-normal entries
				if entry.Type != raftpb.EntryNormal {
					continue
				}
				// Skip empty entries
				if len(entry.Data) == 0 {
					continue
				}

				// Decode the command
				var cmd service.Command
				if err := cmd.UnmarshalBinary(entry.Data); err != nil {
					logger.WithFields(map[string]any{"index": entry.Index, "error": err}).Infof("WARN: Failed to unmarshal command during FSM restoration")
					continue
				}

				// Apply the command to the FSM
				switch cmd.Type {
				case fsm.CommandTypeCreateBucket:
					if err := fsmInstance.HandleCreateBucket(cmd, entry.Index); err != nil {
						logger.WithFields(map[string]any{"index": entry.Index, "error": err}).Infof("WARN: Failed to apply create bucket command during FSM restoration")
						// Continue with other entries even if one fails
					}
				case fsm.CommandTypeDeleteBucket:
					if err := fsmInstance.HandleDeleteBucket(cmd, entry.Index); err != nil {
						logger.WithFields(map[string]any{"index": entry.Index, "error": err}).Infof("WARN: Failed to apply delete bucket command during FSM restoration")
						// Continue with other entries even if one fails
					}
				default:
					logger.WithFields(map[string]any{"type": string(cmd.Type), "index": entry.Index}).Debugf("Skipping unknown command type during FSM restoration")
				}
			}
		}
		logger.WithFields(map[string]any{"lastIndex": lastIndex}).Infof("Finished applying entries after snapshot")
	}

	return nil
}

type Cluster struct {
	node          *NodeWrapper
	fsm           *fsm.FSM
	storage       *Storage
	transport     *Transport
	config        *config.Config
	logger        logging.Logger
	ledgerService service.Ledger // Routed ledger service that routes to bucket Raft groups
	ctx           context.Context
	cancel        context.CancelFunc
	nodeID        uint64
	bucketGroups  map[string]*BucketRaftGroup // Map of bucket name -> bucket Raft group
	muGroups      sync.RWMutex                // Mutex for bucketGroups map
}

func NewRaftCluster(parentCtx context.Context, cfg *config.Config, logger logging.Logger, transport *Transport) (*Cluster, error) {
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
		// todo
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
			logger.Infof("Storage is not empty, skipping bootstrap")
		} else {
			peers := make([]raft.Peer, 0, len(cfg.Peers)+1)
			peers = append(peers, raft.Peer{ID: nodeID})

			// Add peers if provided
			// Peers are in format "<id>/<address>", parse them
			for _, peerEntry := range cfg.Peers {
				parts := strings.SplitN(peerEntry, "/", 2)
				if len(parts) != 2 {
					logger.WithFields(map[string]any{"peer": peerEntry}).Infof("WARN: Invalid peer format, skipping")
					continue
				}
				peerIDStr := parts[0]

				peerID, err := strconv.ParseUint(peerIDStr, 10, 64)
				if err != nil {
					logger.WithFields(map[string]any{"peer": peerEntry, "error": err}).Infof("WARN: Invalid peer ID, skipping")
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
			logger.WithFields(map[string]any{"peers": len(peers)}).Infof("Cluster bootstrapped")
		}
	}

	// Create FSM (Finite State Machine)
	mainFSM := fsm.NewFSM(logger)

	// Restore FSM state from storage (snapshot + entries after snapshot)
	if err := restoreFSMFromStorage(mainFSM, storage, logger); err != nil {
		cancel()
		transport.Stop()
		return nil, fmt.Errorf("restoring FSM from storage: %w", err)
	}

	cluster := &Cluster{
		node:         node,
		fsm:          mainFSM,
		storage:      storage,
		transport:    transport,
		config:       cfg,
		logger:       logger,
		ctx:          ctx,
		cancel:       cancel,
		nodeID:       nodeID,
		bucketGroups: make(map[string]*BucketRaftGroup),
	}

	// Create a routed ledger service that routes to the appropriate bucket
	routedLedger := service.NewRoutedLedger(cluster, &bucketLedgerRouter{cluster: cluster}, logger)
	cluster.ledgerService = routedLedger

	// Add peers to transport
	// Peers are in format "<id>/<address>", parse them
	for _, peerEntry := range cfg.Peers {
		parts := strings.SplitN(peerEntry, "/", 2)
		if len(parts) != 2 {
			logger.WithFields(map[string]any{"peer": peerEntry}).Infof("WARN: Invalid peer format, skipping")
			continue
		}
		peerIDStr := parts[0]
		peerAddr := parts[1]

		peerID, err := strconv.ParseUint(peerIDStr, 10, 64)
		if err != nil {
			logger.WithFields(map[string]any{"peer": peerEntry, "error": err}).Infof("WARN: Invalid peer ID, skipping")
			continue
		}

		transport.AddPeer(peerID, peerAddr)
	}

	// Start Raft groups for existing buckets from FSM after Cluster is created
	// This ensures that buckets created before this node started have their Raft groups running
	cluster.startBucketRaftGroupsFromFSM()

	return cluster, nil
}

func (r *Cluster) Start() error {
	// Start the Ready loop - it will receive all messages and route them appropriately
	go r.readyLoop()

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
				r.logger.WithFields(map[string]any{"from": fmt.Sprintf("%x", msg.From), "to": fmt.Sprintf("%x", msg.To)}).Debugf("Received message for bucket group")
				r.routeMessageToBucketGroup(msg)
			} else {
				r.logger.WithFields(map[string]any{"from": fmt.Sprintf("%x", msg.From), "to": fmt.Sprintf("%x", msg.To)}).Debugf("Received message for main cluster")
				// Process message for main cluster
				r.node.RawNode().Step(msg)
			}
		case peerID := <-r.transport.Unreachable():
			// Report unreachable peer to Raft
			// If peerID >= 0x10000, it's for a bucket group, route it there
			if peerID >= 0x10000 {
				r.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", peerID)}).Debugf("Received unreachable message for bucket group")
				r.routeUnreachableToBucketGroup(peerID)
			} else {
				// Report to main cluster
				r.logger.WithFields(map[string]any{"peer": fmt.Sprintf("%x", peerID)}).Debugf("Received unreachable message for main cluster")
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
					r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to append entries")
					continue
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				r.logger.WithFields(map[string]any{"index": rd.Snapshot.Metadata.Index}).Infof("Applying snapshot")
				// Apply snapshot to storage
				if err := r.storage.ApplySnapshot(rd.Snapshot); err != nil {
					r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to apply snapshot to storage")
					continue
				}
				// Restore FSM from snapshot
				if err := r.fsm.RestoreSnapshot(rd.Snapshot.Data); err != nil {
					r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to restore FSM from snapshot")
					continue
				}

				r.node.node.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)

				// Start Raft groups for existing buckets from FSM after snapshot restoration
				// This ensures that buckets created before this node started have their Raft groups running
				// Note: This is now handled during initialization, but we still need to handle runtime snapshots
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
						r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal ConfChange")
						continue
					}
					r.logger.WithFields(map[string]any{"type": cc.Type.String(), "nodeID": fmt.Sprintf("%x", cc.NodeID)}).Infof("Applying configuration change")
					// Apply the conf change to update the ConfState
					r.node.RawNode().ApplyConfChange(cc)
					continue
				}
				// Skip other non-normal entries
				if entry.Type != raftpb.EntryNormal {
					r.logger.WithFields(map[string]any{"index": entry.Index, "type": uint64(entry.Type)}).Debugf("Skipping non-normal entry")
					continue
				}
				// Skip empty entries (they might be used for heartbeat or other Raft internal purposes)
				if len(entry.Data) == 0 {
					r.logger.WithFields(map[string]any{"index": entry.Index}).Debugf("Skipping empty entry")
					continue
				}
				// Decode the command to get its ID
				var cmd service.Command
				if err := cmd.UnmarshalBinary(entry.Data); err != nil {
					r.logger.WithFields(map[string]any{"index": entry.Index, "error": err}).Errorf("Failed to unmarshal command for notification")
					continue
				}

				result, applyErr := r.applyEntry(entry)
				// Notify the wrapper that this command has been applied using its ID
				r.node.NotifyApplied(cmd.ID, result, entry.Index, applyErr)
				if applyErr != nil {
					r.logger.WithFields(map[string]any{"index": entry.Index, "commandID": cmd.ID, "error": applyErr}).Errorf("Failed to apply entry")
				}
			}

			// Advance the node
			r.node.RawNode().Advance(rd)
		}
	}
}

// applyEntry applies a Raft log entry to the FSM
func (r *Cluster) applyEntry(entry raftpb.Entry) (any, error) {
	// Decode the command from the Raft log data
	var cmd service.Command
	if err := cmd.UnmarshalBinary(entry.Data); err != nil {
		return nil, fmt.Errorf("unmarshaling command: %w", err)
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
							r.logger.WithFields(map[string]any{"bucket": createCmd.Name, "error": startErr}).Errorf("Failed to start bucket Raft group")
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
					r.logger.WithFields(map[string]any{"bucket": deleteCmd.Name, "error": stopErr}).Errorf("Failed to stop bucket Raft group")
					// Don't fail the entry application, just log the error
				}
			}
		}
	default:
		return nil, fmt.Errorf("unknown command type: %s", cmd.Type)
	}

	return nil, err
}

// findPeerAddress finds the address of a peer by ID
func (r *Cluster) findPeerAddress(peerID uint64) string {
	// First, check if it's the local node
	if peerID == r.nodeID {
		return r.config.AdvertiseAddr
	}

	// Try to find it in the transport (which has the actual peer addresses)
	if addr, ok := r.transport.GetPeerAddress(peerID); ok {
		return addr
	}

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
				r.logger.WithFields(map[string]any{"bucket": group.bucketName, "to": fmt.Sprintf("%x", msg.To), "from": fmt.Sprintf("%x", msg.From)}).Infof("WARN: Bucket group message channel full, dropping message")
			}
			return
		}
	}

	r.logger.WithFields(map[string]any{"to": fmt.Sprintf("%x", msg.To), "from": fmt.Sprintf("%x", msg.From), "groupID": fmt.Sprintf("%x", groupID)}).Debugf("No bucket group found for message")
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

	r.logger.WithFields(map[string]any{"peerID": fmt.Sprintf("%x", peerID), "groupID": fmt.Sprintf("%x", groupID)}).Debugf("No bucket group found for unreachable peer")
}

func (r *Cluster) Shutdown() error {
	r.logger.Infof("Shutting down Raft cluster")

	// Stop all bucket Raft groups
	r.muGroups.Lock()
	for bucketName, group := range r.bucketGroups {
		if err := group.Stop(); err != nil {
			r.logger.WithFields(map[string]any{"bucket": bucketName, "error": err}).Errorf("Failed to stop bucket Raft group")
		}
	}
	r.bucketGroups = make(map[string]*BucketRaftGroup)
	r.muGroups.Unlock()

	// Cancel context to stop monitoring
	r.cancel()

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

// GetTransport returns the transport instance
func (r *Cluster) GetTransport() *Transport {
	return r.transport
}

// GetLeaderGRPCClient returns the gRPC client for the current leader
// It uses the transport's existing connection to avoid creating duplicate connections
func (r *Cluster) GetLeaderGRPCClient() service.SystemServiceClient {
	// Get current leader
	status := r.node.RawNode().Status()
	leaderID := status.Lead

	// If we're the leader or no leader, return nil
	if leaderID == r.nodeID || leaderID == 0 {
		return nil
	}

	// Get connection from transport
	conn := r.transport.GetPeerConnection(leaderID)
	if conn == nil {
		r.logger.WithFields(map[string]any{"leaderID": fmt.Sprintf("%x", leaderID)}).Infof("WARN: No gRPC connection available for leader")
		return nil
	}

	// Create client from existing connection
	return service.NewSystemServiceClient(conn)
}

// GetLeaderLedgerGRPCClient returns the LedgerService gRPC client for the current leader
// It uses the transport's existing connection to avoid creating duplicate connections
func (r *Cluster) GetLeaderLedgerGRPCClient() service.LedgerServiceClient {
	// Get current leader
	status := r.node.RawNode().Status()
	leaderID := status.Lead

	// If we're the leader or no leader, return nil
	if leaderID == r.nodeID || leaderID == 0 {
		return nil
	}

	// Get connection from transport
	conn := r.transport.GetPeerConnection(leaderID)
	if conn == nil {
		r.logger.WithFields(map[string]any{"leaderID": fmt.Sprintf("%x", leaderID)}).Infof("WARN: No gRPC connection available for leader")
		return nil
	}

	// Create client from existing connection
	return service.NewLedgerServiceClient(conn)
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
		r.logger.WithFields(map[string]any{"state": status.RaftState.String()}).Infof("WARN: Snapshot requested but not leader")
		return fmt.Errorf("only leader can create snapshots, current state: %v", status.RaftState)
	}

	r.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("Creating snapshot")

	// Trigger snapshot creation
	// In etcd/raft, snapshots are created automatically when needed
	// We can trigger one manually by checking the status
	if status.Applied > 0 {
		r.logger.WithFields(map[string]any{"applied": status.Applied}).Debugf("Creating snapshot data via FSM")
		// Create snapshot data via FSM
		snapshotData, err := r.fsm.CreateSnapshot()
		if err != nil {
			r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot data")
			return fmt.Errorf("creating snapshot data: %w", err)
		}
		r.logger.WithFields(map[string]any{"size": len(snapshotData)}).Debugf("Snapshot data created")

		// Get current configuration state from storage
		r.logger.Debugf("Getting initial state from storage")
		_, confState, err := r.storage.InitialState()
		if err != nil {
			r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to get initial state")
			return fmt.Errorf("getting initial state: %w", err)
		}

		// Create snapshot via storage
		r.logger.WithFields(map[string]any{"index": status.Applied}).Debugf("Creating snapshot in storage")
		_, err = r.storage.CreateSnapshot(status.Applied, &confState, snapshotData)
		if err != nil {
			r.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot in storage")
			return fmt.Errorf("creating snapshot: %w", err)
		}

		r.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("Snapshot created successfully")
	} else {
		r.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("WARN: No applied entries to snapshot")
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

	r.logger.WithFields(map[string]any{"bucket": bucketName, "name": ledgerName}).Infof("Ledger creation proposed via bucket Raft")
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
	_, _, err = r.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command via raft: %w", err)
	}

	r.logger.WithFields(map[string]any{"name": name, "driver": driver, "commandID": cmd.ID}).Infof("Bucket created via Raft")
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
	_, _, err = r.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command via raft: %w", err)
	}

	r.logger.WithFields(map[string]any{"name": name, "commandID": cmd.ID}).Infof("Bucket deleted via Raft")
	return nil
}

// startBucketRaftGroupsFromFSM starts all Raft groups for existing buckets from FSM
func (r *Cluster) startBucketRaftGroupsFromFSM() {
	bucketRaftGroups := r.fsm.GetAllBucketRaftGroups()
	buckets := r.fsm.GetAllBuckets()
	for bucketName, bucketID := range bucketRaftGroups {
		r.logger.WithFields(map[string]any{"bucket": bucketName, "bucketID": bucketID}).Infof("Starting Raft group for bucket")
		bucketInfo, exists := buckets[bucketName]
		if !exists {
			r.logger.WithFields(map[string]any{"bucket": bucketName}).Infof("WARN: Bucket Raft group found but bucket info missing")
			continue
		}
		if err := r.startBucketRaftGroupFromFSM(bucketName, bucketID, bucketInfo); err != nil {
			r.logger.WithFields(map[string]any{"bucket": bucketName, "error": err}).Errorf("Failed to start Raft group for existing bucket")
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
		r.logger.WithFields(map[string]any{"bucket": bucketName}).Infof("WARN: Bucket Raft group already exists")
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

	r.logger.WithFields(map[string]any{"bucket": bucketName, "bucketID": bucketID}).Infof("Bucket Raft group started from FSM")
	return nil
}

// stopBucketRaftGroup stops a Raft group for a bucket
func (r *Cluster) stopBucketRaftGroup(bucketName string) error {
	r.muGroups.Lock()
	defer r.muGroups.Unlock()

	group, exists := r.bucketGroups[bucketName]
	if !exists {
		r.logger.WithFields(map[string]any{"bucket": bucketName}).Infof("WARN: Bucket Raft group does not exist")
		return nil
	}

	// Stop the group
	if err := group.Stop(); err != nil {
		return fmt.Errorf("stopping bucket Raft group: %w", err)
	}

	// Remove from map
	delete(r.bucketGroups, bucketName)

	r.logger.WithFields(map[string]any{"bucket": bucketName}).Infof("Bucket Raft group stopped")
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
