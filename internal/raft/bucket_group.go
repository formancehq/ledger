package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/formancehq/ledger-v3-poc/internal/http"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

// BucketRaftGroup represents a Raft group for a specific bucket
type BucketRaftGroup struct {
	bucketName    string
	node          *raft.RawNode
	storage       *Storage
	bucketStorage service.BucketStorage // Storage for bucket data (SQLite or File)
	fsm           *BucketFSM            // FSM for managing ledgers in this bucket
	transport     *Transport
	config        *config.Config
	logger        *zap.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	nodeID        uint64
	groupID       uint64                 // Unique ID for this bucket group
	msgCh         chan raftpb.Message    // Channel for receiving messages routed from main cluster
	defaultLedger *service.DefaultLedger // Ledger service for this bucket
	logStore      service.LogStore       // Log store for this bucket
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
	logger *zap.Logger,
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
	storage, err := NewStorage(bucketDataDir, logger.With(zap.String("bucket", bucketName)))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating storage for bucket %s: %w", bucketName, err)
	}

	// Create bucket storage based on driver
	bucketStorage, err := service.NewBucketStorage(ctx, bucketInfo.Driver, bucketInfo.Config, logger.With(zap.String("bucket", bucketName)))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating bucket storage for bucket %s: %w", bucketName, err)
	}

	// Create bucket FSM for managing ledgers
	bucketFSM := NewBucketFSM(bucketName, logger)

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
	node, err := raft.NewRawNode(raftConfig)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("creating raw node for bucket %s: %w", bucketName, err)
	}

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
				logger.Warn("Invalid peer format, skipping", zap.String("peer", peerEntry))
				continue
			}
			peerIDStr := parts[0]

			peerNodeID, err := strconv.ParseUint(peerIDStr, 10, 64)
			if err != nil {
				logger.Warn("Invalid peer ID, skipping", zap.String("peer", peerEntry), zap.Error(err))
				continue
			}

			// Calculate peer ID in this bucket group: groupID + peer's node ID
			peerID := groupID + peerNodeID
			peers = append(peers, raft.Peer{ID: peerID})
		}

		if err := node.Bootstrap(peers); err != nil {
			cancel()
			return nil, fmt.Errorf("bootstrapping bucket group %s: %w", bucketName, err)
		}
		logger.Info("Bucket Raft group bootstrapped",
			zap.String("bucket", bucketName),
			zap.String("groupID", fmt.Sprintf("%x", groupID)),
			zap.String("groupNodeID", fmt.Sprintf("%x", groupNodeID)),
			zap.Int("peers", len(peers)))
	}

	// Create application log store for this bucket based on bucket driver
	var appLogStore service.LogStore
	bucketLogger := logger.With(zap.String("bucket", bucketName))
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

	// Create RaftLogWriter for writing logs via Raft (using bucket's node)
	raftLogWriter := service.NewRaftLogWriter(node, bucketLogger)

	// Create reconstructed volumes store (reconstructs volumes from logs)
	volumesStore := service.NewReconstructedVolumesStore(appLogStore)

	// Wrap volumes store with locked volumes store for concurrent access control
	lockedVolumesStore := service.NewDefaultLockedVolumesStore(volumesStore)

	// Create ledger service for this bucket (will use RaftLogWriter to persist logs via bucket Raft)
	// appLogStore implements LogReader, lockedVolumesStore implements LockedVolumesStore
	defaultLedger := service.NewDefaultLedger(raftLogWriter, lockedVolumesStore, appLogStore, bucketLogger)

	group := &BucketRaftGroup{
		bucketName:    bucketName,
		node:          node,
		storage:       storage,
		bucketStorage: bucketStorage,
		fsm:           bucketFSM,
		transport:     transport,
		config:        cfg,
		logger:        bucketLogger.With(zap.String("component", "bucket-raft-group")),
		ctx:           ctx,
		cancel:        cancel,
		nodeID:        groupNodeID, // Use bucket-specific node ID
		groupID:       groupID,
		msgCh:         make(chan raftpb.Message, 100), // Channel for messages routed from main cluster
		defaultLedger: defaultLedger,
		logStore:      appLogStore,
	}

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
			g.node.Tick()
		case msg := <-msgCh:
			// Messages are already filtered by the transport subscription
			// Only messages where To or From matches groupNodeID are received
			// Verify that this is indeed a bucket group message (node ID >= 0x10000)
			if msg.To >= 0x10000 || msg.From >= 0x10000 {
				g.logger.Debug("Received message for bucket group", zap.String("from", fmt.Sprintf("%x", msg.From)), zap.String("to", fmt.Sprintf("%x", msg.To)))
				g.node.Step(msg)
			} else {
				g.logger.Warn("Received message for main cluster in bucket group, ignoring",
					zap.String("to", fmt.Sprintf("%x", msg.To)),
					zap.String("from", fmt.Sprintf("%x", msg.From)),
					zap.String("bucket", g.bucketName))
			}
			// TODO: Handle messages for other bucket groups
			// Unreachable peers are handled by the main cluster and routed here if needed
			// For now, bucket groups don't directly handle unreachable notifications
		}

		// Process Ready structures
		for g.node.HasReady() {
			rd := g.node.Ready()

			// Save HardState, Entries and Snapshot to storage
			if !raft.IsEmptyHardState(rd.HardState) {
				g.storage.SetHardState(rd.HardState)
			}

			if len(rd.Entries) > 0 {
				if err := g.storage.Append(rd.Entries); err != nil {
					g.logger.Error("Failed to append entries", zap.Error(err))
					continue
				}
			}

			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := g.storage.ApplySnapshot(rd.Snapshot); err != nil {
					g.logger.Error("Failed to apply snapshot to storage", zap.Error(err))
					continue
				}
				// Restore bucket FSM from snapshot
				if err := g.fsm.RestoreSnapshot(rd.Snapshot.Data); err != nil {
					g.logger.Error("Failed to restore bucket FSM from snapshot", zap.Error(err))
					continue
				}
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
						g.logger.Error("Failed to unmarshal ConfChange", zap.Error(err))
						continue
					}
					g.logger.Info("Applying configuration change",
						zap.String("type", cc.Type.String()),
						zap.String("nodeID", fmt.Sprintf("%x", cc.NodeID)))
					g.node.ApplyConfChange(cc)
					continue
				}

				if entry.Type != raftpb.EntryNormal {
					g.logger.Debug("Skipping non-normal entry", zap.Uint64("index", entry.Index), zap.Uint64("type", uint64(entry.Type)))
					continue
				}
				// Skip empty entries (they might be used for heartbeat or other Raft internal purposes)
				if len(entry.Data) == 0 {
					g.logger.Debug("Skipping empty entry", zap.Uint64("index", entry.Index))
					continue
				}

				// Apply bucket-specific entries to bucket FSM
				if err := g.applyEntry(entry); err != nil {
					g.logger.Error("Failed to apply entry to bucket FSM",
						zap.Error(err),
						zap.String("entry", string(entry.Data)))
					continue
				}
			}

			// Advance the node
			g.node.Advance(rd)
		}
	}
}

// reportUnreachable reports an unreachable peer to the bucket group's Raft node
func (g *BucketRaftGroup) reportUnreachable(peerID uint64) {
	// Extract the node ID from the peerID in the bucket group context
	// peerID = groupID + nodeID, so we need to report the full peerID
	g.node.ReportUnreachable(peerID)
}

// Stop stops the bucket Raft group
func (g *BucketRaftGroup) Stop() error {
	g.cancel()
	close(g.msgCh)

	// Close bucket storage
	if g.bucketStorage != nil {
		if err := g.bucketStorage.Close(); err != nil {
			g.logger.Error("Failed to close bucket storage", zap.Error(err))
			return fmt.Errorf("closing bucket storage: %w", err)
		}
	}

	return nil
}

// GetBucketName returns the bucket name
func (g *BucketRaftGroup) GetBucketName() string {
	return g.bucketName
}

// GetGroupID returns the group ID
func (g *BucketRaftGroup) GetGroupID() uint64 {
	return g.groupID
}

// applyEntry applies a Raft log entry to the bucket FSM
func (g *BucketRaftGroup) applyEntry(entry raftpb.Entry) error {
	// Decode the command from the Raft log data
	var cmd service.Command
	if err := json.Unmarshal(entry.Data, &cmd); err != nil {
		return fmt.Errorf("unmarshaling command: %w", err)
	}

	// Route to the appropriate command handler
	switch cmd.Type {
	case service.CommandTypeCreateLedger:
		return g.fsm.HandleCreateLedger(cmd.Data, entry.Index)
	case service.CommandTypeInsertLogs:
		return g.handleInsertLogs(cmd.Data)
	default:
		g.logger.Warn("Unknown command type in bucket FSM", zap.String("type", string(cmd.Type)))
		return nil // Don't fail on unknown commands
	}
}

// handleInsertLogs handles the insert logs command by writing logs to the bucket's log store
func (g *BucketRaftGroup) handleInsertLogs(data json.RawMessage) error {
	var insertCmd service.InsertLogsCommand
	if err := json.Unmarshal(data, &insertCmd); err != nil {
		g.logger.Error("Failed to unmarshal insert logs command", zap.Error(err))
		return fmt.Errorf("unmarshaling insert logs command: %w", err)
	}

	if len(insertCmd.Logs) == 0 {
		g.logger.Debug("Insert logs command with no logs, skipping")
		return nil
	}

	// Insert logs into the bucket's log store
	if err := g.logStore.InsertLogs(g.ctx, insertCmd.Logs...); err != nil {
		g.logger.Error("Failed to insert logs into log store", zap.Error(err), zap.Int("count", len(insertCmd.Logs)))
		return fmt.Errorf("inserting logs into log store: %w", err)
	}

	g.logger.Info("Logs inserted into bucket log store", zap.Int("count", len(insertCmd.Logs)))
	return nil
}

// CreateLedger creates a new ledger in this bucket via a FSM command
func (g *BucketRaftGroup) CreateLedger(name string, metadata metadata.Metadata) error {
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
	if err := g.node.Propose(cmdData); err != nil {
		return fmt.Errorf("proposing command via raft: %w", err)
	}

	g.logger.Info("Ledger creation proposed via bucket Raft", zap.String("name", name), zap.String("bucket", g.bucketName))
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

// GetRaftState returns the current state of the bucket Raft group
func (g *BucketRaftGroup) GetRaftState() *http.ClusterState {
	status := g.node.Status()

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
