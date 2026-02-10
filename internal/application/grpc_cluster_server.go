package application

import (
	"context"

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

func (impl *ClusterServiceServerImpl) GetDiskUsage(_ context.Context, _ *clusterpb.GetDiskUsageRequest) (*clusterpb.DiskUsage, error) {
	return &clusterpb.DiskUsage{
		SpoolBytes:      impl.collector.SpoolBytes(),
		WalBytes:        impl.collector.WALBytes(),
		DataBytes:       impl.collector.DataBytes(),
		WalVolumeBytes:  impl.collector.WALVolumeBytes(),
		DataVolumeBytes: impl.collector.DataVolumeBytes(),
	}, nil
}

func RegisterClusterService(server *grpc.Server, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(server, clusterServiceServer)
}
