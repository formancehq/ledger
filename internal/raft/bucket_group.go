package raft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/raft/bucketfsm"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// BucketRaftGroup represents a Raft group for a specific bucket
type BucketRaftGroup struct {
	bucketName    string
	node          *NodeWrapper
	storage       *Storage
	fsm           *bucketfsm.BucketFSM // FSM for managing ledgers in this bucket
	transport     *Transport
	config        *config.Config
	logger        logging.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	nodeID        uint64
	groupID       uint64                 // Unique ID for this bucket group
	msgCh         chan raftpb.Message    // Channel for receiving messages routed from main cluster
	defaultLedger *service.DefaultLedger // Ledger service for this bucket
	logStore      service.LogStore       // Log store for this bucket
	mu            sync.RWMutex           // Mutex for thread-safe access
}

// bucketGroupID generates a unique ID for a bucket Raft group based on the bucket's sequential ID
// This ID is used as the base Raft group ID for this specific bucket
func bucketGroupID(bucketID uint64) uint64 {
	// Use bucket ID with a prefix to ensure uniqueness and avoid collisions with node IDs
	// Each bucket gets a unique group ID: base offset + bucket ID
	return bucketID << 16
}

// NewBucketRaftGroup creates a new Raft group for a bucket
func NewBucketRaftGroup(
	parentCtx context.Context,
	bucketName string,
	bucketID uint64,
	bucketInfo service.BucketInfo,
	transport *Transport,
	cfg *config.Config,
	logger logging.Logger,
) (*BucketRaftGroup, error) {
	ctx, cancel := context.WithCancel(parentCtx)

	// Generate unique group ID for this bucket based on its sequential ID
	groupID := bucketGroupID(bucketID)

	// Create data directory for this bucket group (for Raft storage)
	bucketDataDir := filepath.Join(cfg.DataDir, "buckets", bucketName, "raft")
	if err := os.MkdirAll(bucketDataDir, 0755); err != nil {
		cancel()
		return nil, fmt.Errorf("creating bucket data directory: %w", err)
	}

	// Create Raft storage for this bucket group
	storage, err := NewStorage(bucketDataDir, logger.WithFields(map[string]any{"bucket": bucketName}))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating storage for bucket %s: %w", bucketName, err)
	}

	// Create bucket FSM for managing ledgers
	bucketFSM := bucketfsm.NewBucketFSM(bucketName, logger)

	groupNodeID := groupID + cfg.NodeID // Unique ID for this node in this bucket group

	// Create Raft configuration for this bucket group
	raftConfig := &raft.Config{
		ID:              groupNodeID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         storage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	// Create RawNode
	rawNode, err := raft.NewRawNode(raftConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating raw node for bucket %s: %w", bucketName, err)
	}

	// Wrap the RawNode with our wrapper
	bucketLogger := logger.WithFields(map[string]any{"bucket": bucketName})
	nodeWrapper := NewNodeWrapper(rawNode, bucketLogger)

	// Bootstrap if storage is empty
	// For bucket groups, each node has a unique ID: groupID + nodeOffset
	// This ensures that messages are correctly routed to the right bucket group
	if cfg.Bootstrap && storage.IsEmpty() {
		peers := make([]raft.Peer, 0, len(cfg.Peers)+1)

		// Add local node peer with bucket-specific ID
		peers = append(peers, raft.Peer{ID: groupNodeID})

		// Add peers if provided
		// Peers are in format "<id>/<address>", parse them
		for _, peerEntry := range cfg.Peers {
			parts := strings.SplitN(peerEntry, "/", 2)
			if len(parts) != 2 {
				logger.WithFields(map[string]any{"peer": peerEntry}).Infof("WARN: Invalid peer format, skipping")
				continue
			}
			peerIDStr := parts[0]

			peerNodeID, err := strconv.ParseUint(peerIDStr, 10, 64)
			if err != nil {
				logger.WithFields(map[string]any{"peer": peerEntry, "error": err}).Infof("WARN: Invalid peer ID, skipping")
				continue
			}

			// Calculate peer ID in this bucket group: groupID + peer's node ID
			peerID := groupID + peerNodeID
			peers = append(peers, raft.Peer{ID: peerID})
		}

		if err := nodeWrapper.RawNode().Bootstrap(peers); err != nil {
			cancel()
			return nil, fmt.Errorf("bootstrapping bucket group %s: %w", bucketName, err)
		}
		logger.WithFields(map[string]any{"bucket": bucketName, "groupID": fmt.Sprintf("%x", groupID), "groupNodeID": fmt.Sprintf("%x", groupNodeID), "peers": len(peers)}).Infof("Bucket Raft group bootstrapped")
	}

	// Create application log store for this bucket based on bucket driver
	var appLogStore service.LogStore
	switch bucketInfo.Driver {
	case "sqlite":
		dsn, ok := bucketInfo.Config["dsn"].(string)
		if !ok || dsn == "" {
			cancel()
			return nil, fmt.Errorf("sqlite driver requires 'dsn' configuration for bucket %s", bucketName)
		}
		sqliteStore, err := service.NewSQLiteLogStore(ctx, dsn, bucketLogger)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("creating sqlite log store for bucket %s: %w", bucketName, err)
		}
		appLogStore = sqliteStore
	case "file":
		path, ok := bucketInfo.Config["path"].(string)
		if !ok || path == "" {
			cancel()
			return nil, fmt.Errorf("file driver requires 'path' configuration for bucket %s", bucketName)
		}
		// Create logs directory within the bucket path
		logsPath := filepath.Join(path, "logs.jsonl")

		if err := os.MkdirAll(path, 0755); err != nil {
			cancel()
			return nil, fmt.Errorf("creating logs directory for bucket %s: %w", bucketName, err)
		}

		fileStore, err := service.NewFileLogStore(logsPath, bucketLogger)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("creating file log store for bucket %s: %w", bucketName, err)
		}
		appLogStore = fileStore
	default:
		cancel()
		return nil, fmt.Errorf("unsupported bucket driver for log store: %s", bucketInfo.Driver)
	}

	// Create bucket group first (will be used as TransactionCreator)
	group := &BucketRaftGroup{
		bucketName: bucketName,
		node:       nodeWrapper,
		storage:    storage,
		fsm:        bucketFSM,
		transport:  transport,
		config:     cfg,
		logger:     bucketLogger.WithFields(map[string]any{"component": "bucket-raft-group"}),
		ctx:        ctx,
		cancel:     cancel,
		nodeID:     groupNodeID, // Use bucket-specific node ID
		groupID:    groupID,
		msgCh:      make(chan raftpb.Message, 100), // Channel for messages routed from main cluster
		logStore:   appLogStore,
	}

	// Create reconstructed volumes store
	reconstructedVolumesStore := service.NewReconstructedBalancesStore(appLogStore)

	consolidatedVolumesStore := service.NewConsolidatedBalancesStore(reconstructedVolumesStore, bucketFSM)

	// Create locked volumes store
	lockedVolumesStore := service.NewDefaultLockedBalancesStore(consolidatedVolumesStore)

	// Create ledger service for this bucket (will use stores for balance checking and log writing)
	defaultLedger := service.NewDefaultLedger(group, lockedVolumesStore, struct {
		service.LogWriter
		service.LogReader
	}{
		LogWriter: appLogStore,
		LogReader: service.NewConsolidatedLogReader(appLogStore, bucketFSM),
	}, bucketLogger)
	group.defaultLedger = defaultLedger

	return group, nil
}

// Start starts the bucket Raft group
func (g *BucketRaftGroup) Start() error {
	go g.readyLoopWithChannel(g.msgCh)
	return nil
}

// GetDefaultLedger returns the default ledger service for this bucket
func (g *BucketRaftGroup) GetDefaultLedger() *service.DefaultLedger {
	return g.defaultLedger
}

// GetMessageChannel returns the message channel for this bucket group
func (g *BucketRaftGroup) GetMessageChannel() chan<- raftpb.Message {
	return g.msgCh
}

// readyLoopWithChannel processes Ready structures from etcd/raft for this bucket group with a specific message channel
func (g *BucketRaftGroup) readyLoopWithChannel(msgCh <-chan raftpb.Message) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-g.ctx.Done():
			return
		case <-ticker.C:
			g.node.RawNode().Tick()
		case msg := <-msgCh:
			// Messages are already filtered by the transport subscription
			// Only messages where To or From matches groupNodeID are received
			// Verify that this is indeed a bucket group message (node ID >= 0x10000)
			if msg.To >= 0x10000 || msg.From >= 0x10000 {
				g.logger.WithFields(map[string]any{"from": fmt.Sprintf("%x", msg.From), "to": fmt.Sprintf("%x", msg.To)}).Debugf("Received message for bucket group")
				g.node.RawNode().Step(msg)
			} else {
				g.logger.WithFields(map[string]any{"to": fmt.Sprintf("%x", msg.To), "from": fmt.Sprintf("%x", msg.From), "bucket": g.bucketName}).Infof("WARN: Received message for main cluster in bucket group, ignoring")
			}
			// TODO: Handle messages for other bucket groups
			// Unreachable peers are handled by the main cluster and routed here if needed
			// For now, bucket groups don't directly handle unreachable notifications
		}

		// Process Ready structures
		for g.node.RawNode().HasReady() {
			rd := g.node.RawNode().Ready()

			// Save HardState, Entries and Snapshot to storage
			if !raft.IsEmptyHardState(rd.HardState) {
				g.storage.SetHardState(rd.HardState)
			}

			if len(rd.Entries) > 0 {
				if err := g.storage.Append(rd.Entries); err != nil {
					g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to append entries")
					continue
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				g.logger.WithFields(map[string]any{"index": rd.Snapshot.Metadata.Index}).Infof("Applying snapshot")

				if err := g.storage.ApplySnapshot(rd.Snapshot); err != nil {
					g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to apply snapshot to storage")
					continue
				}
				// Restore bucket FSM from snapshot
				if err := g.fsm.RestoreSnapshot(rd.Snapshot.Data); err != nil {
					g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to restore bucket FSM from snapshot")
					continue
				}

				g.node.node.ReportSnapshot(rd.Snapshot.Metadata.Index, raft.SnapshotFinish)
			}

			// Send messages via transport
			for _, msg := range rd.Messages {
				g.transport.Send(msg)
			}

			// Apply committed entries
			for _, entry := range rd.CommittedEntries {
				if entry.Type == raftpb.EntryConfChange {
					var cc raftpb.ConfChange
					if err := cc.Unmarshal(entry.Data); err != nil {
						g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to unmarshal ConfChange")
						continue
					}
					g.logger.WithFields(map[string]any{"type": cc.Type.String(), "nodeID": fmt.Sprintf("%x", cc.NodeID)}).Infof("Applying configuration change")
					g.node.RawNode().ApplyConfChange(cc)
					continue
				}

				if entry.Type != raftpb.EntryNormal {
					g.logger.WithFields(map[string]any{"index": entry.Index, "type": uint64(entry.Type)}).Debugf("Skipping non-normal entry")
					continue
				}
				// Skip empty entries (they might be used for heartbeat or other Raft internal purposes)
				if len(entry.Data) == 0 {
					g.logger.WithFields(map[string]any{"index": entry.Index}).Debugf("Skipping empty entry")
					continue
				}

				// Decode the command to get its ID
				var cmd service.Command
				if err := cmd.UnmarshalBinary(entry.Data); err != nil {
					g.logger.WithFields(map[string]any{"index": entry.Index, "error": err}).Errorf("Failed to unmarshal command for notification")
					continue
				}

				// Apply bucket-specific entries to bucket FSM
				result, applyErr := g.applyEntry(entry)
				// Notify the wrapper that this command has been applied using its ID
				g.node.NotifyApplied(cmd.ID, result, entry.Index, applyErr)
				if applyErr != nil {
					g.logger.WithFields(map[string]any{"error": applyErr, "index": entry.Index, "commandID": cmd.ID, "entry": string(entry.Data)}).Errorf("Failed to apply entry to bucket FSM")
				}
			}

			// Advance the node
			g.node.RawNode().Advance(rd)

			// Check if we need to create a snapshot (every 1000 entries or when log is getting large)
			status := g.node.RawNode().Status()
			if status.Applied > 0 && status.Applied%1000 == 0 {
				// Create snapshot: write logs to store and create snapshot data
				snapshotData, err := g.fsm.CreateSnapshot(g.ctx, g.logStore)
				if err != nil {
					g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot data")
					continue
				}

				// Get current configuration state from storage
				_, confState, err := g.storage.InitialState()
				if err != nil {
					g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to get initial state for snapshot")
					continue
				}

				// Create snapshot in storage
				_, err = g.storage.CreateSnapshot(status.Applied, &confState, snapshotData)
				if err != nil {
					// Check if error is ErrSnapOutOfDate (expected if snapshot was already created)
					if err != ErrSnapOutOfDate {
						g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot in storage")
					}
					// ErrSnapOutOfDate is expected if snapshot was already created
					continue
				}

				g.logger.WithFields(map[string]any{"bucket": g.bucketName, "index": status.Applied}).Infof("Snapshot created for bucket")
			}
		}
	}
}

// reportUnreachable reports an unreachable peer to the bucket group's Raft node
func (g *BucketRaftGroup) reportUnreachable(peerID uint64) {
	// Extract the node ID from the peerID in the bucket group context
	// peerID = groupID + nodeID, so we need to report the full peerID
	g.node.RawNode().ReportUnreachable(peerID)
}

// Stop stops the bucket Raft group
func (g *BucketRaftGroup) Stop() error {
	g.cancel()
	close(g.msgCh)

	return nil
}

// Snapshot forces a snapshot of the bucket Raft group
func (g *BucketRaftGroup) Snapshot() error {
	g.logger.WithFields(map[string]any{"bucket": g.bucketName}).Infof("Snapshot request received for bucket")

	// Check if we are the leader (only leader can create snapshots)
	status := g.node.RawNode().Status()
	if status.RaftState != raft.StateLeader {
		g.logger.WithFields(map[string]any{"state": status.RaftState.String()}).Infof("WARN: Snapshot requested but not leader")
		return fmt.Errorf("only leader can create snapshots, current state: %v", status.RaftState)
	}

	g.logger.WithFields(map[string]any{"bucket": g.bucketName, "applied": status.Applied}).Infof("Creating snapshot for bucket")

	// Trigger snapshot creation
	if status.Applied > 0 {
		// Create snapshot data via FSM
		snapshotData, err := g.fsm.CreateSnapshot(g.ctx, g.logStore)
		if err != nil {
			g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot data")
			return fmt.Errorf("creating snapshot data: %w", err)
		}
		g.logger.WithFields(map[string]any{"size": len(snapshotData)}).Debugf("Snapshot data created")

		// Get current configuration state from storage
		_, confState, err := g.storage.InitialState()
		if err != nil {
			g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to get initial state")
			return fmt.Errorf("getting initial state: %w", err)
		}

		// Create snapshot via storage
		_, err = g.storage.CreateSnapshot(status.Applied, &confState, snapshotData)
		if err != nil {
			// Check if error is ErrSnapOutOfDate (expected if snapshot was already created)
			if err != ErrSnapOutOfDate {
				g.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to create snapshot in storage")
				return fmt.Errorf("creating snapshot: %w", err)
			}
			// ErrSnapOutOfDate is expected if snapshot was already created
			g.logger.WithFields(map[string]any{"index": status.Applied}).Infof("Snapshot already exists")
			return nil
		}

		g.logger.WithFields(map[string]any{"bucket": g.bucketName, "applied": status.Applied}).Infof("Snapshot created successfully for bucket")
	} else {
		g.logger.WithFields(map[string]any{"applied": status.Applied}).Infof("WARN: No applied entries to snapshot")
	}
	return nil
}

// applyEntry applies a Raft log entry to the bucket FSM
func (g *BucketRaftGroup) applyEntry(entry raftpb.Entry) (any, error) {
	// Decode the command from the Raft log data
	var cmd service.Command
	if err := cmd.UnmarshalBinary(entry.Data); err != nil {
		return nil, fmt.Errorf("unmarshaling command: %w", err)
	}

	// Route to the appropriate command handler
	switch cmd.Type {
	case bucketfsm.CommandTypeCreateLedger:
		return g.fsm.HandleCreateLedger(cmd, entry.Index)
	case bucketfsm.CommandTypeInsertLog:
		return nil, g.fsm.HandleInsertLog(cmd, entry.Index)
	default:
		g.logger.WithFields(map[string]any{"type": string(cmd.Type)}).Infof("WARN: Unknown command type in bucket FSM")
		return nil, nil // Don't fail on unknown commands
	}
}

// CreateLedger creates a new ledger in this bucket via a FSM command
func (g *BucketRaftGroup) CreateLedger(name string, metadata metadata.Metadata) error {
	// Create the command
	cmd, err := bucketfsm.NewCreateLedgerCommand(name, metadata)
	if err != nil {
		return fmt.Errorf("creating create ledger command: %w", err)
	}

	// Apply the command via Raft (waits for application)
	_, _, err = g.node.Apply(cmd, 5*time.Second)
	if err != nil {
		return fmt.Errorf("applying command via raft: %w", err)
	}

	g.logger.WithFields(map[string]any{"name": name, "bucket": g.bucketName, "commandID": cmd.ID}).Infof("Ledger created via bucket Raft")
	return nil
}

// InsertLogs writes logs via Raft (implements LogWriter)
func (g *BucketRaftGroup) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// For each log, create a command to insert it via Raft
	for _, log := range logs {
		// Create a command to insert the log
		cmd, err := bucketfsm.NewInsertLogCommand(log)
		if err != nil {
			return fmt.Errorf("creating insert log command: %w", err)
		}

		// Apply the command via Raft (waits for application)
		_, _, err = g.node.Apply(cmd, 5*time.Second)
		if err != nil {
			return fmt.Errorf("applying insert log command via raft: %w", err)
		}

		g.logger.WithFields(map[string]any{"ledger": log.Ledger, "commandID": cmd.ID}).Debugf("Log inserted via bucket Raft")
	}

	return nil
}

// GetLedger returns the ledger info for a given name in this bucket
func (g *BucketRaftGroup) GetLedger(name string) (service.LedgerInfo, bool) {
	return g.fsm.GetLedger(name)
}

// GetAllLedgers returns all ledgers in this bucket
func (g *BucketRaftGroup) GetAllLedgers() map[string]service.LedgerInfo {
	return g.fsm.GetAllLedgers()
}

// GetInMemoryDiffBalances returns the in-memory balance diff for a ledger (implements HotDiffBalancesProvider)
func (g *BucketRaftGroup) GetInMemoryDiffBalances(ledgerName string) ledger.Balances {
	return g.fsm.GetInMemoryDiffBalances(ledgerName)
}

// GetInMemoryLogs returns the in-memory logs for a ledger
func (g *BucketRaftGroup) GetInMemoryLogs(ledgerName string) []ledger.Log {
	return g.fsm.GetInMemoryLogs(ledgerName)
}

// GetRaftState returns the current state of the bucket Raft group
func (g *BucketRaftGroup) GetRaftState() *http.ClusterState {
	status := g.node.RawNode().Status()

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
		}
		// Extract global node ID from bucket group node ID
		// id = groupID + nodeID, so nodeID = id & 0xFFFF (lower 16 bits)
		globalNodeID := id & 0xFFFF
		addr := fmt.Sprintf("%x", globalNodeID) // Use global node ID for address
		nodes = append(nodes, http.NodeInfo{
			ID:       fmt.Sprintf("%x", id),
			Address:  addr,
			Suffrage: suffrage,
		})
	}

	// Extract global node ID from groupNodeID for localNode
	localGlobalNodeID := g.nodeID & 0xFFFF

	return &http.ClusterState{
		State:     stateStr,
		Leader:    leader,
		Nodes:     nodes,
		LocalNode: fmt.Sprintf("%x", localGlobalNodeID),
	}
}
