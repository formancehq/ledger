package http

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/http/bulking"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
)

type Server struct {
	logger               logging.Logger
	backend              Backend
	bulkerFactory        bulking.BulkerFactory
	bulkHandlerFactories map[string]bulking.HandlerFactory
}

// NewServer creates a new server instance (used by handlers)
func NewServer(logger logging.Logger, backend Backend) *Server {
	return &Server{
		logger:  logger,
		backend: backend,
	}
}

type Backend interface {
	service.Controller
	GetClusterState(context context.Context) (*raftcmdpb.ClusterState, error)
	IsHealthy() bool
}

type DefaultBackend struct {
	service.Controller
	Node *raft.Node
}

func (b *DefaultBackend) GetClusterState(ctx context.Context) (*raftcmdpb.ClusterState, error) {
	return b.Node.GetClusterState(ctx)
}

func (b *DefaultBackend) IsHealthy() bool {
	return b.Node.IsHealthy()
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(node *raft.Node, ctrl service.Controller) *DefaultBackend {
	return &DefaultBackend{
		Node:       node,
		Controller: ctrl,
	}
}
