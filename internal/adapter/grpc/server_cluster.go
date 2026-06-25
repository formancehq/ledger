package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	ggrpc "google.golang.org/grpc"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	backupapp "github.com/formancehq/ledger/v3/internal/application/backup"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/indexbuilder"
	"github.com/formancehq/ledger/v3/internal/application/membership"
	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/diskusage"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

type ClusterServiceServerImpl struct {
	clusterpb.UnimplementedClusterServiceServer

	node             *node.Node
	raftTransport    *node.DefaultTransport
	servicePool      *transport.ConnectionPool
	collector        *diskusage.Collector
	store            *dal.Store
	readStore        *readstore.Store
	cache            *cache.Cache
	sharedState      *state.SharedState
	indexBuilder     *indexbuilder.Builder
	admission        ctrl.Admission
	membership       *membership.Service
	logger           logging.Logger
	localRaftAddr    string // This node's own Raft advertise address
	localServiceAddr string // This node's own gRPC service address
	authCfg          internalauth.AuthConfig
	clusterID        string
	info             version.Info
	backupOrchestra  *backupapp.Orchestrator
	forwarder        nodeForwarder
}

func NewClusterServiceServer(
	node *node.Node,
	raftTransport *node.DefaultTransport,
	servicePool *transport.ConnectionPool,
	collector *diskusage.Collector,
	store *dal.Store,
	cache *cache.Cache,
	sharedState *state.SharedState,
	indexBuilder *indexbuilder.Builder,
	readStore *readstore.Store,
	admission ctrl.Admission,
	membershipSvc *membership.Service,
	backupOrchestra *backupapp.Orchestrator,
	logger logging.Logger,
	localRaftAddr string,
	localServiceAddr string,
	authCfg internalauth.AuthConfig,
	clusterID string,
	info version.Info,
) clusterpb.ClusterServiceServer {
	return &ClusterServiceServerImpl{
		node:             node,
		raftTransport:    raftTransport,
		servicePool:      servicePool,
		collector:        collector,
		store:            store,
		readStore:        readStore,
		cache:            cache,
		sharedState:      sharedState,
		indexBuilder:     indexBuilder,
		admission:        admission,
		membership:       membershipSvc,
		backupOrchestra:  backupOrchestra,
		logger:           logger.WithField("component", "cluster-server"),
		localRaftAddr:    localRaftAddr,
		localServiceAddr: localServiceAddr,
		authCfg:          authCfg,
		clusterID:        clusterID,
		info:             info,
		forwarder:        nodeForwarder{node: node, servicePool: servicePool},
	}
}

func (impl *ClusterServiceServerImpl) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterRead); err != nil {
		return nil, err
	}

	if req.GetNodeId() == 0 {
		// No node ID specified, route to leader
		if impl.node.IsLeader() {
			return impl.getClusterStateLocal(ctx)
		}

		leaderID := impl.node.GetLeader()
		if leaderID == 0 {
			impl.logger.WithFields(map[string]any{
				"localNodeID": impl.node.GetNodeID(),
			}).Infof("GetClusterState: no leader known, returning ErrNoLeader")

			return nil, commonpb.ErrNoLeader
		}

		conn := impl.servicePool.GetConnection(leaderID)
		if conn == nil {
			return nil, commonpb.ErrNoLeader
		}

		return clusterpb.NewClusterServiceClient(conn).GetClusterState(ctx, req)
	}

	// Specific node ID requested — use shared resolver.
	conn, err := impl.forwarder.resolve(req.GetNodeId())
	if err != nil {
		return nil, err
	}

	if conn != nil {
		return clusterpb.NewClusterServiceClient(conn).GetClusterState(ctx, req)
	}

	return impl.getClusterStateLocal(ctx)
}

// mapNodeVersion returns the peer's self-reported version, or "" when the peer
// is unreachable (peerState == nil) or runs a binary that predates the
// node_version field (empty NodeVersion). It deliberately does NOT fall back to
// the local node's version — doing so would mask the very version skew this
// per-node reporting exists to surface. GetNodeVersion is nil-safe.
func mapNodeVersion(peerState *clusterpb.ClusterState) string {
	return peerState.GetNodeVersion()
}

// getClusterStateLocal returns cluster state with peer address information populated.
func (impl *ClusterServiceServerImpl) getClusterStateLocal(ctx context.Context) (*clusterpb.ClusterState, error) {
	clusterState, err := impl.node.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}

	// Populate maintenance mode from shared state
	clusterState.MaintenanceMode = impl.sharedState.MaintenanceMode()
	clusterState.NodeVersion = impl.info.Version
	// Read the full persisted cluster config (includes bloom filter settings).
	// Fall back to a minimal config with just the rotation threshold if not yet persisted.
	if persistedState, err := query.ReadClusterState(impl.store); err == nil && persistedState != nil {
		clusterState.ClusterConfig = persistedState.GetConfig()
	} else {
		clusterState.ClusterConfig = &commonpb.ClusterConfig{
			RotationThreshold: impl.cache.GenerationThreshold(),
		}
	}

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
			nodeInfo.Version = impl.info.Version
		} else {
			nodeInfo.RaftAddress = impl.raftTransport.GetPeerAddress(nodeID)
			nodeInfo.ServiceAddress = impl.servicePool.GetPeerAddress(nodeID)
			// Query the peer for its sync progress and index progress
			peerState := impl.fetchPeerState(ctx, nodeID)
			if peerState != nil {
				nodeInfo.SyncProgress = peerState.GetSyncProgress()
				nodeInfo.IndexProgress = peerState.GetIndexProgress()
			}
			nodeInfo.Version = mapNodeVersion(peerState)
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

// leaderClient returns a ClusterServiceClient connected to the current leader.
// Returns commonpb.ErrNoLeader if no leader is known or unreachable.
func (impl *ClusterServiceServerImpl) leaderClient() (clusterpb.ClusterServiceClient, error) {
	leaderID := impl.node.GetLeader()
	if leaderID == 0 {
		return nil, commonpb.ErrNoLeader
	}

	grpcConn := impl.servicePool.GetConnection(leaderID)
	if grpcConn == nil {
		return nil, commonpb.ErrNoLeader
	}

	return clusterpb.NewClusterServiceClient(grpcConn), nil
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
		client, err := impl.leaderClient()
		if err != nil {
			return nil, err
		}

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
			UsedBytes:  uint64(impl.collector.WALVolume.UsedBytes()),
			TotalBytes: uint64(impl.collector.WALVolume.TotalBytes()),
		},
		DataVolume: &clusterpb.VolumeUsage{
			UsedBytes:  uint64(impl.collector.DataVolume.UsedBytes()),
			TotalBytes: uint64(impl.collector.DataVolume.TotalBytes()),
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

	impl.logger.WithFields(map[string]any{
		"requestedNodeID":      req.GetNodeId(),
		"requestedRaftAddress": req.GetRaftAddress(),
		"requestedServiceAddr": req.GetServiceAddress(),
		"isLeader":             impl.node.IsLeader(),
		"localNodeID":          impl.node.GetNodeID(),
	}).Infof("AddLearner: received request")

	if !impl.node.IsLeader() {
		client, err := impl.leaderClient()
		if err != nil {
			impl.logger.Infof("AddLearner: not leader and leader unreachable, returning ErrNoLeader")

			return nil, err
		}

		impl.logger.Infof("AddLearner: forwarding to leader")

		return client.AddLearner(ctx, req)
	}

	if err := impl.membership.AddLearner(ctx, req.GetNodeId(), req.GetRaftAddress(), req.GetServiceAddress()); err != nil {
		return nil, err
	}

	return &clusterpb.AddLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) PromoteLearner(ctx context.Context, req *clusterpb.PromoteLearnerRequest) (*clusterpb.PromoteLearnerResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	if !impl.node.IsLeader() {
		client, err := impl.leaderClient()
		if err != nil {
			return nil, err
		}

		return client.PromoteLearner(ctx, req)
	}

	if err := impl.membership.PromoteLearner(ctx, req.GetNodeId()); err != nil {
		return nil, err
	}

	return &clusterpb.PromoteLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) RemoveNode(ctx context.Context, req *clusterpb.RemoveNodeRequest) (*clusterpb.RemoveNodeResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	// Force-remove bypasses consensus and must run on the leader directly.
	// Do NOT forward: the operator already exec's into the leader pod.
	if req.GetForce() && !impl.node.IsLeader() {
		return nil, errors.New("force-remove must be executed on the leader node")
	}

	if !req.GetForce() && !impl.node.IsLeader() {
		client, err := impl.leaderClient()
		if err != nil {
			return nil, err
		}

		return client.RemoveNode(ctx, req)
	}

	if err := impl.membership.RemoveNode(ctx, req.GetNodeId(), req.GetForce()); err != nil {
		return nil, err
	}

	return &clusterpb.RemoveNodeResponse{}, nil
}

func (impl *ClusterServiceServerImpl) CompactPrimary(ctx context.Context, _ *clusterpb.CompactPrimaryRequest) (*clusterpb.CompactPrimaryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	start := time.Now()

	err := impl.store.CompactAll()
	if err != nil {
		return nil, fmt.Errorf("compaction failed: %w", err)
	}

	return &clusterpb.CompactPrimaryResponse{
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

func (impl *ClusterServiceServerImpl) CompactSecondary(ctx context.Context, _ *clusterpb.CompactSecondaryRequest) (*clusterpb.CompactSecondaryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	start := time.Now()

	sizeBefore, sizeAfter, err := impl.readStore.Compact(ctx)
	if err != nil {
		return nil, fmt.Errorf("read index compaction failed: %w", err)
	}

	return &clusterpb.CompactSecondaryResponse{
		DurationMs:      time.Since(start).Milliseconds(),
		SizeBeforeBytes: uint64(sizeBefore),
		SizeAfterBytes:  uint64(sizeAfter),
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
	logs, err := impl.admission.Admit(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
		},
	}))
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
	_, err := impl.admission.Admit(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_DeleteQueryCheckpoint{
			DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{
				CheckpointId: req.GetCheckpointId(),
			},
		},
	}))
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
		// Use NotFoundError so the gRPC layer maps this to codes.NotFound
		// rather than the sanitized codes.Unknown that bare fmt.Errorf
		// gets in convertToGRPCError. A follower whose Pebble has not yet
		// applied the checkpoint legitimately returns nil here right
		// after List on another node has already seen it; clients
		// (notably the Antithesis cross-node oracle) need a typed code
		// to classify this as transient and retry.
		return nil, commonpb.NewNotFoundError("query checkpoint %d not found", req.GetCheckpointId())
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

// extractBackupDestination builds the FSM destination tuple and the
// configured Storage driver from an inbound RPC. The bucketId falls
// back to the local clusterID when the caller leaves it blank, which
// matches the previous in-handler behaviour.
//
// Cluster-wide mutual exclusion lives inside the FSM (BackupJobsState):
// a concurrent Backup against a byte-equal destination — same node or
// any other — gets state.ErrBackupInProgress back through the apply
// path; the orchestrator surfaces it as-is to the handler.
func (impl *ClusterServiceServerImpl) extractBackupDestination(storageProto *commonpb.BackupStorage, basePath, bucketIDRaw string) (backup.Storage, *raftcmdpb.BackupDestination, error) {
	cfg, err := storageConfigFromProto(storageProto)
	if err != nil {
		return nil, nil, err
	}

	storage, err := backup.NewStorage(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating backup storage: %w", err)
	}

	bucketID := bucketIDRaw
	if bucketID == "" {
		bucketID = impl.clusterID
	}

	dst := &raftcmdpb.BackupDestination{
		BasePath: basePath,
		BucketId: bucketID,
	}

	switch cfg.Driver {
	case "s3":
		dst.Target = &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{
				Bucket:   cfg.S3Bucket,
				Region:   cfg.S3Region,
				Endpoint: cfg.S3Endpoint,
			},
		}
	case "azure":
		dst.Target = &raftcmdpb.BackupDestination_Azure{
			Azure: &raftcmdpb.AzureBackupTarget{
				AccountName: cfg.AzureAccountName,
				Container:   cfg.AzureContainer,
				Endpoint:    cfg.AzureEndpoint,
			},
		}
	default:
		return nil, nil, fmt.Errorf("backup destination: unsupported driver %q", cfg.Driver)
	}

	return storage, dst, nil
}

func (impl *ClusterServiceServerImpl) Backup(ctx context.Context, req *clusterpb.BackupRequest) (*clusterpb.BackupResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	// Forward to leader if not leader. The orchestrator only runs on the
	// leader (it is the proposer); a follower would just spin forever
	// trying to push proposals.
	if !impl.node.IsLeader() {
		client, err := impl.leaderClient()
		if err != nil {
			return nil, err
		}

		return client.Backup(ctx, req)
	}

	storage, dst, err := impl.extractBackupDestination(req.GetStorage(), req.GetBasePath(), req.GetBucketId())
	if err != nil {
		return nil, err
	}

	result, err := impl.backupOrchestra.RunFull(ctx, dst, storage)
	if err != nil {
		if errors.Is(err, state.ErrBackupInProgress) {
			return nil, fmt.Errorf("backup already in progress for this destination: %w", err)
		}

		return nil, fmt.Errorf("backup failed: %w", err)
	}

	return &clusterpb.BackupResponse{
		FilesUploaded:     uint32(result.FilesUploaded),
		FilesDeleted:      uint32(result.FilesDeleted),
		OrphansDeleted:    uint32(result.OrphansDeleted),
		TotalFiles:        uint32(result.TotalFiles),
		DurationMs:        result.Duration.Milliseconds(),
		LastLogSequence:   result.LastLogSequence,
		LastAuditSequence: result.LastAuditSequence,
		LastAppliedIndex:  result.LastAppliedIndex,
	}, nil
}

func (impl *ClusterServiceServerImpl) IncrementalBackup(ctx context.Context, req *clusterpb.IncrementalBackupRequest) (*clusterpb.IncrementalBackupResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeClusterWrite); err != nil {
		return nil, err
	}

	// Incremental used to skip the leader-routing step because log/audit
	// sequences are identical across replicas. Now the FSM owns the
	// destination slot and only the leader can drive a Raft proposal,
	// so we forward like the full path.
	if !impl.node.IsLeader() {
		client, err := impl.leaderClient()
		if err != nil {
			return nil, err
		}

		return client.IncrementalBackup(ctx, req)
	}

	storage, dst, err := impl.extractBackupDestination(req.GetStorage(), req.GetBasePath(), req.GetBucketId())
	if err != nil {
		return nil, err
	}

	result, err := impl.backupOrchestra.RunIncremental(ctx, dst, storage)
	if err != nil {
		if errors.Is(err, state.ErrBackupInProgress) {
			return nil, fmt.Errorf("incremental backup already in progress for this destination: %w", err)
		}

		return nil, fmt.Errorf("incremental backup failed: %w", err)
	}

	return &clusterpb.IncrementalBackupResponse{
		LogEntriesExported:   result.LogEntriesExported,
		AuditEntriesExported: result.AuditEntriesExported,
		SegmentsUploaded:     uint32(result.SegmentsUploaded),
		OrphansDeleted:       uint32(result.OrphansDeleted),
		DurationMs:           result.Duration.Milliseconds(),
		LastLogSequence:      result.LastLogSequence,
		LastAuditSequence:    result.LastAuditSequence,
	}, nil
}

func RegisterClusterService(registrar ggrpc.ServiceRegistrar, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(registrar, clusterServiceServer)
}
