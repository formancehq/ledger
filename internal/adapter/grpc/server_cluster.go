package grpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/application/indexbuilder"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
	ggrpc "google.golang.org/grpc"
)

type ClusterServiceServerImpl struct {
	clusterpb.UnimplementedClusterServiceServer
	node             *node.Node
	raftTransport    *node.DefaultTransport
	servicePool      *transport.ConnectionPool
	collector        *diskusage.Collector
	store            *dal.Store
	readStore        *readstore.Store
	sharedState      *state.SharedState
	indexBuilder     *indexbuilder.Builder
	logger           logging.Logger
	localRaftAddr    string // This node's own Raft advertise address
	localServiceAddr string // This node's own gRPC service address
	authCfg          internalauth.AuthConfig
}

func NewClusterServiceServer(
	node *node.Node,
	raftTransport *node.DefaultTransport,
	servicePool *transport.ConnectionPool,
	collector *diskusage.Collector,
	store *dal.Store,
	sharedState *state.SharedState,
	indexBuilder *indexbuilder.Builder,
	readStore *readstore.Store,
	logger logging.Logger,
	localRaftAddr string,
	localServiceAddr string,
	authCfg internalauth.AuthConfig,
) clusterpb.ClusterServiceServer {
	return &ClusterServiceServerImpl{
		node:             node,
		raftTransport:    raftTransport,
		servicePool:      servicePool,
		collector:        collector,
		store:            store,
		readStore:        readStore,
		sharedState:      sharedState,
		indexBuilder:     indexBuilder,
		logger:           logger.WithField("component", "cluster-server"),
		localRaftAddr:    localRaftAddr,
		localServiceAddr: localServiceAddr,
		authCfg:          authCfg,
	}
}

func (impl *ClusterServiceServerImpl) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	// Determine target node
	var targetNodeID uint64
	localNodeID := impl.node.GetNodeID()

	if req.NodeId == 0 {
		// No node ID specified, route to leader
		if impl.node.IsLeader() {
			// This node is the leader, handle locally
			return impl.getClusterStateLocal(ctx)
		}

		// Get the leader ID
		targetNodeID = impl.node.GetLeader()
		if targetNodeID == 0 {
			impl.logger.WithFields(map[string]any{
				"localNodeID": localNodeID,
			}).Infof("GetClusterState: no leader known, returning ErrNoLeader")
			return nil, commonpb.ErrNoLeader
		}
	} else {
		// Specific node ID requested
		targetNodeID = uint64(req.NodeId)

		// If requesting this node, handle locally
		if targetNodeID == localNodeID {
			return impl.getClusterStateLocal(ctx)
		}
	}

	// Forward to target node
	grpcConn := impl.servicePool.GetConnection(targetNodeID)
	if grpcConn == nil {
		impl.logger.WithFields(map[string]any{
			"localNodeID":  localNodeID,
			"targetNodeID": targetNodeID,
			"knownPeers":   impl.servicePool.PeerIDs(),
		}).Infof("GetClusterState: no connection to target node, returning ErrNoLeader")
		return nil, commonpb.ErrNoLeader
	}

	impl.logger.WithFields(map[string]any{
		"localNodeID":  localNodeID,
		"targetNodeID": targetNodeID,
	}).Infof("GetClusterState: forwarding to target node")

	client := clusterpb.NewClusterServiceClient(grpcConn)
	return client.GetClusterState(ctx, req)
}

// getClusterStateLocal returns cluster state with peer address information populated.
func (impl *ClusterServiceServerImpl) getClusterStateLocal(ctx context.Context) (*clusterpb.ClusterState, error) {
	clusterState, err := impl.node.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}

	// Populate maintenance mode from shared state
	clusterState.MaintenanceMode = impl.sharedState.MaintenanceMode()

	// Populate local index builder progress on ClusterState (for backward compat / single-node view)
	localIndexProgress := &clusterpb.IndexProgress{
		LastIndexedSequence: impl.indexBuilder.LastIndexedSequence(),
		PebbleLastSequence:  impl.indexBuilder.PebbleLastSequence(),
	}
	clusterState.IndexProgress = localIndexProgress

	// Populate peer addresses, sync progress, and index progress from each node
	localNodeID := impl.node.GetNodeID()
	for _, nodeInfo := range clusterState.Nodes {
		nodeID := uint64(nodeInfo.Id)
		if nodeID == localNodeID {
			nodeInfo.RaftAddress = impl.localRaftAddr
			nodeInfo.ServiceAddress = impl.localServiceAddr
			// Local sync progress is already in clusterState.SyncProgress
			nodeInfo.SyncProgress = clusterState.SyncProgress
			nodeInfo.IndexProgress = localIndexProgress
		} else {
			nodeInfo.RaftAddress = impl.raftTransport.GetPeerAddress(nodeID)
			nodeInfo.ServiceAddress = impl.servicePool.GetPeerAddress(nodeID)
			// Query the peer for its sync progress and index progress
			peerState := impl.fetchPeerState(ctx, nodeID)
			if peerState != nil {
				nodeInfo.SyncProgress = peerState.SyncProgress
				nodeInfo.IndexProgress = peerState.IndexProgress
			}
		}
	}

	return clusterState, nil
}

// fetchPeerState queries a peer node for its local cluster state (sync progress, index progress, etc).
// Returns nil if the peer is unreachable.
func (impl *ClusterServiceServerImpl) fetchPeerState(ctx context.Context, nodeID uint64) *clusterpb.ClusterState {
	conn := impl.servicePool.GetConnection(nodeID)
	if conn == nil {
		return nil
	}

	// Short timeout — this is best-effort and must not slow down the status command.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	client := clusterpb.NewClusterServiceClient(conn)
	peerState, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
		NodeId: uint32(nodeID),
	})
	if err != nil {
		return nil
	}

	return peerState
}

func (impl *ClusterServiceServerImpl) TransferLeadership(ctx context.Context, req *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.Transferee == 0 {
		return nil, fmt.Errorf("transferee node ID must be non-zero")
	}

	transferee := uint64(req.Transferee)

	// If this node is not the leader, forward to the leader
	if !impl.node.IsLeader() {
		leaderID := impl.node.GetLeader()
		if leaderID == 0 {
			return nil, commonpb.ErrNoLeader
		}

		grpcConn := impl.servicePool.GetConnection(leaderID)
		if grpcConn == nil {
			return nil, commonpb.ErrNoLeader
		}

		client := clusterpb.NewClusterServiceClient(grpcConn)
		return client.TransferLeadership(ctx, req)
	}

	if err := impl.node.TransferLeader(ctx, transferee); err != nil {
		return nil, fmt.Errorf("leadership transfer failed: %w", err)
	}

	return &clusterpb.TransferLeadershipResponse{
		NewLeader: req.Transferee,
	}, nil
}

func (impl *ClusterServiceServerImpl) GetDiskUsage(ctx context.Context, _ *clusterpb.GetDiskUsageRequest) (*clusterpb.DiskUsage, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	return &clusterpb.DiskUsage{
		SpoolBytes:           impl.collector.SpoolBytes(),
		WalBytes:             impl.collector.WALBytes(),
		DataBytes:            impl.collector.DataBytes(),
		ReadIndexBytes:       impl.collector.ReadIndexBytes(),
		WalVolumeBytes:       impl.collector.WALVolumeBytes(),
		DataVolumeBytes:      impl.collector.DataVolumeBytes(),
		WalVolumeTotalBytes:  impl.collector.WALVolumeTotalBytes(),
		DataVolumeTotalBytes: impl.collector.DataVolumeTotalBytes(),
	}, nil
}

func (impl *ClusterServiceServerImpl) GetNodeTime(ctx context.Context, _ *clusterpb.GetNodeTimeRequest) (*clusterpb.NodeTime, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	return &clusterpb.NodeTime{
		TimestampUs: uint64(time.Now().UnixMicro()),
	}, nil
}

func (impl *ClusterServiceServerImpl) Backup(req *clusterpb.BackupRequest, stream ggrpc.ServerStreamingServer[clusterpb.BackupResponse]) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return err
	}

	// If this node is the leader, create a checkpoint and stream it
	if impl.node.IsLeader() {
		return impl.backupLocal(stream)
	}

	// Forward to leader
	leaderID := impl.node.GetLeader()
	if leaderID == 0 {
		return commonpb.ErrNoLeader
	}

	grpcConn := impl.servicePool.GetConnection(leaderID)
	if grpcConn == nil {
		return commonpb.ErrNoLeader
	}

	impl.logger.WithFields(map[string]any{
		"leader_id": leaderID,
	}).Infof("Forwarding backup request to leader")

	client := clusterpb.NewClusterServiceClient(grpcConn)
	backupStream, err := client.Backup(stream.Context(), req)
	if err != nil {
		return fmt.Errorf("forwarding backup to leader: %w", err)
	}

	// Relay chunks from leader to caller
	for {
		resp, err := backupStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("receiving backup chunk from leader: %w", err)
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("sending backup chunk to client: %w", err)
		}
	}
}

func (impl *ClusterServiceServerImpl) backupLocal(stream ggrpc.ServerStreamingServer[clusterpb.BackupResponse]) error {
	impl.logger.Infof("Creating backup checkpoint")

	if err := stream.Send(&clusterpb.BackupResponse{StatusMessage: "Creating checkpoint..."}); err != nil {
		return fmt.Errorf("sending status message: %w", err)
	}

	// Create a direct Pebble checkpoint (no Raft consensus needed).
	// Boundaries are always up-to-date in Pebble since they are written on every commit.
	backupPath, err := impl.node.CreateBackupCheckpoint()
	if err != nil {
		return fmt.Errorf("creating backup checkpoint: %w", err)
	}
	defer func() {
		if err := impl.store.RemoveTemporaryCheckpoint("checkpoint"); err != nil {
			impl.logger.WithFields(map[string]any{
				"error": err,
			}).Errorf("Failed to remove backup checkpoint")
		}
	}()

	// Compact attributes to index 0 and reset lastAppliedIndex in the backup.
	// This ensures backups are self-contained and can be restored on a fresh cluster
	// without raft index conflicts in the attribute storage.
	impl.logger.Infof("Compacting backup checkpoint for restore compatibility")

	if err := stream.Send(&clusterpb.BackupResponse{StatusMessage: "Compacting attributes..."}); err != nil {
		return fmt.Errorf("sending status message: %w", err)
	}

	compactStore, err := dal.OpenDirect(backupPath, impl.logger)
	if err != nil {
		return fmt.Errorf("opening backup for compaction: %w", err)
	}

	if err := attributes.CompactAllForBackup(compactStore); err != nil {
		_ = compactStore.Close()
		return fmt.Errorf("compacting backup attributes: %w", err)
	}

	if err := compactStore.Close(); err != nil {
		return fmt.Errorf("closing compacted backup: %w", err)
	}

	impl.logger.Infof("Streaming backup checkpoint")

	return StreamDirAsTar(backupPath, 0, func(chunk TarStreamChunk) error {
		return stream.Send(&clusterpb.BackupResponse{
			ChunkOffset:        chunk.ChunkOffset,
			Data:               chunk.Data,
			Eof:                chunk.IsEOF,
			ContentSha256:      chunk.ContentSHA256,
			ContentSize:        chunk.ContentSize,
			EstimatedTotalSize: chunk.EstimatedTotalSize,
		})
	})
}

func (impl *ClusterServiceServerImpl) AddLearner(ctx context.Context, req *clusterpb.AddLearnerRequest) (*clusterpb.AddLearnerResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.NodeId == 0 {
		return nil, fmt.Errorf("node_id must be non-zero")
	}
	if req.RaftAddress == "" {
		return nil, fmt.Errorf("raft_address is required")
	}
	if req.ServiceAddress == "" {
		return nil, fmt.Errorf("service_address is required")
	}

	impl.logger.WithFields(map[string]any{
		"requestedNodeID":      req.NodeId,
		"requestedRaftAddress": req.RaftAddress,
		"requestedServiceAddr": req.ServiceAddress,
		"isLeader":             impl.node.IsLeader(),
		"localNodeID":          impl.node.GetNodeID(),
	}).Infof("AddLearner: received request")

	// Forward to leader if not leader
	if !impl.node.IsLeader() {
		leaderID := impl.node.GetLeader()
		if leaderID == 0 {
			impl.logger.Infof("AddLearner: not leader and no leader known, returning ErrNoLeader")
			return nil, commonpb.ErrNoLeader
		}

		grpcConn := impl.servicePool.GetConnection(leaderID)
		if grpcConn == nil {
			impl.logger.WithFields(map[string]any{
				"leaderID": leaderID,
			}).Infof("AddLearner: no connection to leader, returning ErrNoLeader")
			return nil, commonpb.ErrNoLeader
		}

		impl.logger.WithFields(map[string]any{
			"leaderID": leaderID,
		}).Infof("AddLearner: forwarding to leader")

		client := clusterpb.NewClusterServiceClient(grpcConn)
		return client.AddLearner(ctx, req)
	}

	// On the leader: add peer to transport and service pool so we can reach it
	impl.logger.WithFields(map[string]any{
		"learnerNodeID":   req.NodeId,
		"raftAddress":     req.RaftAddress,
		"serviceAddress":  req.ServiceAddress,
	}).Infof("AddLearner: processing on leader — adding peer to transport and proposing ConfChange")

	impl.raftTransport.AddPeer(req.NodeId, req.RaftAddress)
	if err := impl.servicePool.AddPeer(req.NodeId, req.ServiceAddress); err != nil {
		impl.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add learner to service pool")
	}

	// Propose the ConfChange
	if err := impl.node.AddLearner(ctx, req.NodeId, req.RaftAddress, req.ServiceAddress); err != nil {
		return nil, fmt.Errorf("adding learner: %w", err)
	}

	impl.logger.WithFields(map[string]any{
		"learnerNodeID": req.NodeId,
	}).Infof("AddLearner: successfully proposed ConfChange for learner")

	return &clusterpb.AddLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) PromoteLearner(ctx context.Context, req *clusterpb.PromoteLearnerRequest) (*clusterpb.PromoteLearnerResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.NodeId == 0 {
		return nil, fmt.Errorf("node_id must be non-zero")
	}

	// Forward to leader if not leader
	if !impl.node.IsLeader() {
		leaderID := impl.node.GetLeader()
		if leaderID == 0 {
			return nil, commonpb.ErrNoLeader
		}

		grpcConn := impl.servicePool.GetConnection(leaderID)
		if grpcConn == nil {
			return nil, commonpb.ErrNoLeader
		}

		client := clusterpb.NewClusterServiceClient(grpcConn)
		return client.PromoteLearner(ctx, req)
	}

	if err := impl.node.PromoteLearner(ctx, req.NodeId); err != nil {
		return nil, fmt.Errorf("promoting learner: %w", err)
	}

	return &clusterpb.PromoteLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) RemoveNode(ctx context.Context, req *clusterpb.RemoveNodeRequest) (*clusterpb.RemoveNodeResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.NodeId == 0 {
		return nil, fmt.Errorf("node_id must be non-zero")
	}

	// Force-remove bypasses consensus and must run on the leader directly.
	// Do NOT forward: the operator already exec's into the leader pod.
	if req.Force {
		if !impl.node.IsLeader() {
			return nil, fmt.Errorf("force-remove must be executed on the leader node")
		}

		if err := impl.node.ForceRemoveNode(ctx, req.NodeId); err != nil {
			return nil, fmt.Errorf("force-removing node: %w", err)
		}

		return &clusterpb.RemoveNodeResponse{}, nil
	}

	// Forward to leader if not leader
	if !impl.node.IsLeader() {
		leaderID := impl.node.GetLeader()
		if leaderID == 0 {
			return nil, commonpb.ErrNoLeader
		}

		grpcConn := impl.servicePool.GetConnection(leaderID)
		if grpcConn == nil {
			return nil, commonpb.ErrNoLeader
		}

		client := clusterpb.NewClusterServiceClient(grpcConn)
		return client.RemoveNode(ctx, req)
	}

	if err := impl.node.RemoveNode(ctx, req.NodeId); err != nil {
		return nil, fmt.Errorf("removing node: %w", err)
	}

	return &clusterpb.RemoveNodeResponse{}, nil
}

func (impl *ClusterServiceServerImpl) CompactStore(ctx context.Context, _ *clusterpb.CompactStoreRequest) (*clusterpb.CompactStoreResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	start := time.Now()
	if err := impl.store.CompactAll(); err != nil {
		return nil, fmt.Errorf("compaction failed: %w", err)
	}

	return &clusterpb.CompactStoreResponse{
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

func (impl *ClusterServiceServerImpl) CompactReadIndex(ctx context.Context, _ *clusterpb.CompactReadIndexRequest) (*clusterpb.CompactReadIndexResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	start := time.Now()
	sizeBefore, sizeAfter, err := impl.readStore.Compact(ctx)
	if err != nil {
		return nil, fmt.Errorf("read index compaction failed: %w", err)
	}

	return &clusterpb.CompactReadIndexResponse{
		DurationMs:      time.Since(start).Milliseconds(),
		SizeBeforeBytes: sizeBefore,
		SizeAfterBytes:  sizeAfter,
	}, nil
}

func (impl *ClusterServiceServerImpl) CreateCheckpoint(ctx context.Context, _ *clusterpb.CreateCheckpointRequest) (*clusterpb.CreateCheckpointResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	checkpointID, err := impl.store.CreateSnapshot()
	if err != nil {
		return nil, fmt.Errorf("checkpoint creation failed: %w", err)
	}

	return &clusterpb.CreateCheckpointResponse{
		CheckpointId: checkpointID,
	}, nil
}

func RegisterClusterService(server *ggrpc.Server, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(server, clusterServiceServer)
}
