package grpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	ggrpc "google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger-v3-poc/internal/adapter/auth"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/application/indexbuilder"
	"github.com/formancehq/ledger-v3-poc/internal/infra/backup"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
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
	admission        ctrl.Admission
	logger           logging.Logger
	localRaftAddr    string // This node's own Raft advertise address
	localServiceAddr string // This node's own gRPC service address
	authCfg          internalauth.AuthConfig
	clusterID        string
	backupMu         sync.Mutex
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
	admission ctrl.Admission,
	logger logging.Logger,
	localRaftAddr string,
	localServiceAddr string,
	authCfg internalauth.AuthConfig,
	clusterID string,
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
		admission:        admission,
		logger:           logger.WithField("component", "cluster-server"),
		localRaftAddr:    localRaftAddr,
		localServiceAddr: localServiceAddr,
		authCfg:          authCfg,
		clusterID:        clusterID,
	}
}

func (impl *ClusterServiceServerImpl) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	// Determine target node
	var targetNodeID uint64

	localNodeID := impl.node.GetNodeID()

	if req.GetNodeId() == 0 {
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
		targetNodeID = uint64(req.GetNodeId())

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

	for _, nodeInfo := range clusterState.GetNodes() {
		nodeID := uint64(nodeInfo.GetId())
		if nodeID == localNodeID {
			nodeInfo.RaftAddress = impl.localRaftAddr
			nodeInfo.ServiceAddress = impl.localServiceAddr
			// Local sync progress is already in clusterState.SyncProgress
			nodeInfo.SyncProgress = clusterState.GetSyncProgress()
			nodeInfo.IndexProgress = localIndexProgress
		} else {
			nodeInfo.RaftAddress = impl.raftTransport.GetPeerAddress(nodeID)
			nodeInfo.ServiceAddress = impl.servicePool.GetPeerAddress(nodeID)
			// Query the peer for its sync progress and index progress
			peerState := impl.fetchPeerState(ctx, nodeID)
			if peerState != nil {
				nodeInfo.SyncProgress = peerState.GetSyncProgress()
				nodeInfo.IndexProgress = peerState.GetIndexProgress()
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

	if req.GetTransferee() == 0 {
		return nil, errors.New("transferee node ID must be non-zero")
	}

	transferee := uint64(req.GetTransferee())

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

	err := impl.node.TransferLeader(ctx, transferee)
	if err != nil {
		return nil, fmt.Errorf("leadership transfer failed: %w", err)
	}

	return &clusterpb.TransferLeadershipResponse{
		NewLeader: req.GetTransferee(),
	}, nil
}

func (impl *ClusterServiceServerImpl) GetDiskUsage(ctx context.Context, _ *clusterpb.GetDiskUsageRequest) (*clusterpb.DiskUsage, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	return &clusterpb.DiskUsage{
		WalVolume: &clusterpb.VolumeUsage{
			UsedBytes:  impl.collector.WALVolume.UsedBytes(),
			TotalBytes: impl.collector.WALVolume.TotalBytes(),
		},
		DataVolume: &clusterpb.VolumeUsage{
			UsedBytes:  impl.collector.DataVolume.UsedBytes(),
			TotalBytes: impl.collector.DataVolume.TotalBytes(),
		},
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

func (impl *ClusterServiceServerImpl) AddLearner(ctx context.Context, req *clusterpb.AddLearnerRequest) (*clusterpb.AddLearnerResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.GetNodeId() == 0 {
		return nil, errors.New("node_id must be non-zero")
	}

	if req.GetRaftAddress() == "" {
		return nil, errors.New("raft_address is required")
	}

	if req.GetServiceAddress() == "" {
		return nil, errors.New("service_address is required")
	}

	impl.logger.WithFields(map[string]any{
		"requestedNodeID":      req.GetNodeId(),
		"requestedRaftAddress": req.GetRaftAddress(),
		"requestedServiceAddr": req.GetServiceAddress(),
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
		"learnerNodeID":  req.GetNodeId(),
		"raftAddress":    req.GetRaftAddress(),
		"serviceAddress": req.GetServiceAddress(),
	}).Infof("AddLearner: processing on leader — adding peer to transport and proposing ConfChange")

	impl.raftTransport.AddPeer(req.GetNodeId(), req.GetRaftAddress())

	err := impl.servicePool.AddPeer(req.GetNodeId(), req.GetServiceAddress())
	if err != nil {
		impl.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add learner to service pool")
	}

	// Propose the ConfChange
	err = impl.node.AddLearner(ctx, req.GetNodeId(), req.GetRaftAddress(), req.GetServiceAddress())
	if err != nil {
		return nil, fmt.Errorf("adding learner: %w", err)
	}

	impl.logger.WithFields(map[string]any{
		"learnerNodeID": req.GetNodeId(),
	}).Infof("AddLearner: successfully proposed ConfChange for learner")

	return &clusterpb.AddLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) PromoteLearner(ctx context.Context, req *clusterpb.PromoteLearnerRequest) (*clusterpb.PromoteLearnerResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.GetNodeId() == 0 {
		return nil, errors.New("node_id must be non-zero")
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

	err := impl.node.PromoteLearner(ctx, req.GetNodeId())
	if err != nil {
		return nil, fmt.Errorf("promoting learner: %w", err)
	}

	return &clusterpb.PromoteLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) RemoveNode(ctx context.Context, req *clusterpb.RemoveNodeRequest) (*clusterpb.RemoveNodeResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if req.GetNodeId() == 0 {
		return nil, errors.New("node_id must be non-zero")
	}

	// Force-remove bypasses consensus and must run on the leader directly.
	// Do NOT forward: the operator already exec's into the leader pod.
	if req.GetForce() {
		if !impl.node.IsLeader() {
			return nil, errors.New("force-remove must be executed on the leader node")
		}

		err := impl.node.ForceRemoveNode(ctx, req.GetNodeId())
		if err != nil {
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

	err := impl.node.RemoveNode(ctx, req.GetNodeId())
	if err != nil {
		return nil, fmt.Errorf("removing node: %w", err)
	}

	return &clusterpb.RemoveNodeResponse{}, nil
}

func (impl *ClusterServiceServerImpl) CompactStore(ctx context.Context, _ *clusterpb.CompactStoreRequest) (*clusterpb.CompactStoreResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	start := time.Now()

	err := impl.store.CompactAll()
	if err != nil {
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

func (impl *ClusterServiceServerImpl) CreateQueryCheckpoint(ctx context.Context, _ *clusterpb.CreateQueryCheckpointRequest) (*clusterpb.CreateQueryCheckpointResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	// Route through Raft so the checkpoint is replicated to all nodes.
	logs, err := impl.admission.Admit(ctx, &servicepb.Request{
		Type: &servicepb.Request_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("creating query checkpoint via raft: %w", err)
	}

	// Extract the assigned checkpoint ID and log sequence from the response.
	if cp := logs[0].GetPayload().GetCreatedQueryCheckpoint(); cp != nil {
		// Wait for the read index to process this log (which triggers the
		// read index checkpoint creation by the index builder).
		if err := impl.readStore.WaitForSequence(ctx, logs[0].GetSequence()); err != nil {
			return nil, fmt.Errorf("waiting for read index checkpoint: %w", err)
		}

		return &clusterpb.CreateQueryCheckpointResponse{
			CheckpointId: cp.GetCheckpointId(),
			MaxSequence:  cp.GetMaxSequence(),
		}, nil
	}

	return nil, errors.New("checkpoint creation log not found in response")
}

func (impl *ClusterServiceServerImpl) DeleteQueryCheckpoint(ctx context.Context, req *clusterpb.DeleteQueryCheckpointRequest) (*clusterpb.DeleteQueryCheckpointResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	// Route through Raft so the deletion is replicated to all nodes.
	_, err := impl.admission.Admit(ctx, &servicepb.Request{
		Type: &servicepb.Request_DeleteQueryCheckpoint{
			DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{
				CheckpointId: req.GetCheckpointId(),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("deleting query checkpoint via raft: %w", err)
	}

	return &clusterpb.DeleteQueryCheckpointResponse{}, nil
}

func (impl *ClusterServiceServerImpl) ListQueryCheckpoints(ctx context.Context, _ *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	handle, err := impl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	cps, err := query.ListQueryCheckpoints(handle)
	if err != nil {
		return nil, fmt.Errorf("listing query checkpoints: %w", err)
	}

	checkpoints := make([]*clusterpb.QueryCheckpointInfo, 0, len(cps))
	for _, cp := range cps {
		checkpoints = append(checkpoints, queryCheckpointToInfo(cp))
	}

	return &clusterpb.ListQueryCheckpointsResponse{Checkpoints: checkpoints}, nil
}

func (impl *ClusterServiceServerImpl) GetQueryCheckpointInfo(ctx context.Context, req *clusterpb.GetQueryCheckpointInfoRequest) (*clusterpb.QueryCheckpointInfo, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	handle, err := impl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	cp, err := query.ReadQueryCheckpoint(handle, req.GetCheckpointId())
	if err != nil {
		return nil, fmt.Errorf("getting query checkpoint info: %w", err)
	}

	if cp == nil {
		return nil, fmt.Errorf("checkpoint %d not found", req.GetCheckpointId())
	}

	return queryCheckpointToInfo(cp), nil
}

func (impl *ClusterServiceServerImpl) GetQueryCheckpointSchedule(ctx context.Context, _ *clusterpb.GetQueryCheckpointScheduleRequest) (*clusterpb.GetQueryCheckpointScheduleResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	handle, err := impl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	cronExpr, err := query.ReadQueryCheckpointSchedule(handle)
	if err != nil {
		return nil, fmt.Errorf("loading query checkpoint schedule: %w", err)
	}

	return &clusterpb.GetQueryCheckpointScheduleResponse{Cron: cronExpr}, nil
}

func queryCheckpointToInfo(cp *raftcmdpb.QueryCheckpointState) *clusterpb.QueryCheckpointInfo {
	return &clusterpb.QueryCheckpointInfo{
		CheckpointId: cp.GetCheckpointId(),
		MaxSequence:  cp.GetMaxSequence(),
		CreatedAt:    cp.GetCreatedAt(),
	}
}

func (impl *ClusterServiceServerImpl) Backup(ctx context.Context, req *clusterpb.BackupRequest) (*clusterpb.BackupResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
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

		return client.Backup(ctx, req)
	}

	// Serialize backups
	impl.backupMu.Lock()
	defer impl.backupMu.Unlock()

	if req.GetDriver() == "" {
		return nil, errors.New("driver is required")
	}

	storage, err := backup.NewStorage(
		req.GetDriver(),
		req.GetBasePath(),
		req.GetS3Bucket(),
		req.GetS3Region(),
		req.GetS3Endpoint(),
	)
	if err != nil {
		return nil, fmt.Errorf("creating backup storage: %w", err)
	}

	bucketID := req.GetBucketId()
	if bucketID == "" {
		bucketID = impl.clusterID
	}

	result, err := backup.RunBackup(ctx, impl.logger, impl.store, storage, bucketID)
	if err != nil {
		return nil, fmt.Errorf("backup failed: %w", err)
	}

	return &clusterpb.BackupResponse{
		FilesUploaded: uint32(result.FilesUploaded),
		FilesDeleted:  uint32(result.FilesDeleted),
		TotalFiles:    uint32(result.TotalFiles),
		DurationMs:    result.Duration.Milliseconds(),
	}, nil
}

func RegisterClusterService(server *ggrpc.Server, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(server, clusterServiceServer)
}
