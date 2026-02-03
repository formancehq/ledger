package application

import (
	"context"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"google.golang.org/grpc"
)

type ClusterServiceServerImpl struct {
	clusterpb.UnimplementedClusterServiceServer
	node *node.Node
}

func NewClusterServiceServer(node *node.Node) clusterpb.ClusterServiceServer {
	return &ClusterServiceServerImpl{
		node: node,
	}
}

func (impl *ClusterServiceServerImpl) GetClusterState(ctx context.Context, _ *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	return impl.node.GetClusterState(ctx)
}

func RegisterClusterService(server *grpc.Server, clusterServiceServer clusterpb.ClusterServiceServer) {
	clusterpb.RegisterClusterServiceServer(server, clusterServiceServer)
}
