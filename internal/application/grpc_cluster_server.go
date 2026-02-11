package application

import (
	"context"
	"time"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/diskusage"
	"google.golang.org/grpc"
)

type ClusterServiceServerImpl struct {
	clusterpb.UnimplementedClusterServiceServer
	node        *node.Node
	servicePool *transport.ServiceConnectionPool
	collector   *diskusage.Collector
}

func NewClusterServiceServer(node *node.Node, servicePool *transport.ServiceConnectionPool, collector *diskusage.Collector) clusterpb.ClusterServiceServer {
	return &ClusterServiceServerImpl{
		node:        node,
		servicePool: servicePool,
		collector:   collector,
	}
}

func (impl *ClusterServiceServerImpl) GetClusterState(ctx context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	// Determine target node
	var targetNodeID uint64

	if req.NodeId == 0 {
		// No node ID specified, route to leader
		if impl.node.IsLeader() {
			// This node is the leader, handle locally
			return impl.node.GetClusterState(ctx)
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
			return impl.node.GetClusterState(ctx)
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

func RegisterClusterService(server *grpc.Server, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(server, clusterServiceServer)
}
