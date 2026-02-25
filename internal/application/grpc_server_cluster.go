package application

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/diskusage"
	"google.golang.org/grpc"
)

type ClusterServiceServerImpl struct {
	clusterpb.UnimplementedClusterServiceServer
	node             *node.Node
	raftTransport    *node.DefaultTransport
	servicePool      *transport.ConnectionPool
	collector        *diskusage.Collector
	store            *dal.Store
	sharedState      *state.SharedState
	logger           logging.Logger
	localRaftAddr    string // This node's own Raft advertise address
	localServiceAddr string // This node's own gRPC service address
}

func NewClusterServiceServer(
	node *node.Node,
	raftTransport *node.DefaultTransport,
	servicePool *transport.ConnectionPool,
	collector *diskusage.Collector,
	store *dal.Store,
	sharedState *state.SharedState,
	logger logging.Logger,
	localRaftAddr string,
	localServiceAddr string,
) clusterpb.ClusterServiceServer {
	return &ClusterServiceServerImpl{
		node:             node,
		raftTransport:    raftTransport,
		servicePool:      servicePool,
		collector:        collector,
		store:            store,
		sharedState:      sharedState,
		logger:           logger.WithField("component", "cluster-server"),
		localRaftAddr:    localRaftAddr,
		localServiceAddr: localServiceAddr,
	}
}

func (impl *ClusterServiceServerImpl) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	// Determine target node
	var targetNodeID uint64

	if req.NodeId == 0 {
		// No node ID specified, route to leader
		if impl.node.IsLeader() {
			// This node is the leader, handle locally
			return impl.getClusterStateLocal(ctx)
		}

		// Get the leader ID
		targetNodeID = impl.node.GetLeader()
		if targetNodeID == 0 {
			return nil, commonpb.ErrNoLeader
		}
	} else {
		// Specific node ID requested
		targetNodeID = uint64(req.NodeId)

		// If requesting this node, handle locally
		if targetNodeID == impl.node.GetNodeID() {
			return impl.getClusterStateLocal(ctx)
		}
	}

	// Forward to target node
	grpcConn := impl.servicePool.GetConnection(targetNodeID)
	if grpcConn == nil {
		return nil, commonpb.ErrNoLeader
	}

	client := clusterpb.NewClusterServiceClient(grpcConn)
	return client.GetClusterState(ctx, req)
}

// getClusterStateLocal returns cluster state with peer address information populated.
func (impl *ClusterServiceServerImpl) getClusterStateLocal(ctx context.Context) (*clusterpb.ClusterState, error) {
	state, err := impl.node.GetClusterState(ctx)
	if err != nil {
		return nil, err
	}

	// Populate maintenance mode from shared state
	state.MaintenanceMode = impl.sharedState.MaintenanceMode()

	// Populate peer addresses from transport and service pool
	localNodeID := impl.node.GetNodeID()
	for _, nodeInfo := range state.Nodes {
		nodeID := uint64(nodeInfo.Id)
		if nodeID == localNodeID {
			nodeInfo.RaftAddress = impl.localRaftAddr
			nodeInfo.ServiceAddress = impl.localServiceAddr
		} else {
			nodeInfo.RaftAddress = impl.raftTransport.GetPeerAddress(nodeID)
			nodeInfo.ServiceAddress = impl.servicePool.GetPeerAddress(nodeID)
		}
	}

	return state, nil
}

func (impl *ClusterServiceServerImpl) TransferLeadership(ctx context.Context, req *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
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

func (impl *ClusterServiceServerImpl) GetDiskUsage(_ context.Context, _ *clusterpb.GetDiskUsageRequest) (*clusterpb.DiskUsage, error) {
	return &clusterpb.DiskUsage{
		SpoolBytes:           impl.collector.SpoolBytes(),
		WalBytes:             impl.collector.WALBytes(),
		DataBytes:            impl.collector.DataBytes(),
		WalVolumeBytes:       impl.collector.WALVolumeBytes(),
		DataVolumeBytes:      impl.collector.DataVolumeBytes(),
		WalVolumeTotalBytes:  impl.collector.WALVolumeTotalBytes(),
		DataVolumeTotalBytes: impl.collector.DataVolumeTotalBytes(),
	}, nil
}

func (impl *ClusterServiceServerImpl) GetNodeTime(_ context.Context, _ *clusterpb.GetNodeTimeRequest) (*clusterpb.NodeTime, error) {
	return &clusterpb.NodeTime{
		TimestampUs: uint64(time.Now().UnixMicro()),
	}, nil
}

func (impl *ClusterServiceServerImpl) Backup(req *clusterpb.BackupRequest, stream grpc.ServerStreamingServer[clusterpb.BackupResponse]) error {
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

func (impl *ClusterServiceServerImpl) backupLocal(stream grpc.ServerStreamingServer[clusterpb.BackupResponse]) error {
	impl.logger.Infof("Creating backup checkpoint")

	// Propose a CreateCheckpoint through Raft consensus. All nodes enter maintenance
	// mode; the leader creates the backup checkpoint and writes dirty boundaries into it.
	backupPath, err := impl.node.ProposeBackupCheckpoint(stream.Context())
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
			ChunkOffset:   chunk.ChunkOffset,
			Data:          chunk.Data,
			Eof:           chunk.IsEOF,
			ContentSha256: chunk.ContentSHA256,
			ContentSize:   chunk.ContentSize,
		})
	})
}

func (impl *ClusterServiceServerImpl) AddLearner(ctx context.Context, req *clusterpb.AddLearnerRequest) (*clusterpb.AddLearnerResponse, error) {
	if req.NodeId == 0 {
		return nil, fmt.Errorf("node_id must be non-zero")
	}
	if req.RaftAddress == "" {
		return nil, fmt.Errorf("raft_address is required")
	}
	if req.ServiceAddress == "" {
		return nil, fmt.Errorf("service_address is required")
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
		return client.AddLearner(ctx, req)
	}

	// On the leader: add peer to transport and service pool so we can reach it
	impl.raftTransport.AddPeer(req.NodeId, req.RaftAddress)
	if err := impl.servicePool.AddPeer(req.NodeId, req.ServiceAddress); err != nil {
		impl.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to add learner to service pool")
	}

	// Propose the ConfChange
	if err := impl.node.AddLearner(ctx, req.NodeId, req.RaftAddress, req.ServiceAddress); err != nil {
		return nil, fmt.Errorf("adding learner: %w", err)
	}

	return &clusterpb.AddLearnerResponse{}, nil
}

func (impl *ClusterServiceServerImpl) PromoteLearner(ctx context.Context, req *clusterpb.PromoteLearnerRequest) (*clusterpb.PromoteLearnerResponse, error) {
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
		return client.RemoveNode(ctx, req)
	}

	if err := impl.node.RemoveNode(ctx, req.NodeId); err != nil {
		return nil, fmt.Errorf("removing node: %w", err)
	}

	return &clusterpb.RemoveNodeResponse{}, nil
}

func RegisterClusterService(server *grpc.Server, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(server, clusterServiceServer)
}
