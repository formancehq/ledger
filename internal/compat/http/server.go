package http

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
)

type Server struct {
	logger      logging.Logger
	backend     Backend
	bulkMaxSize int
}

// NewServer creates a new server instance (used by handlers)
func NewServer(logger logging.Logger, backend Backend, bulkMaxSize int) *Server {
	return &Server{
		logger:      logger,
		backend:     backend,
		bulkMaxSize: bulkMaxSize,
	}
}

type Backend interface {
	ctrl.Controller
	GetClusterState(context context.Context) (*clusterpb.ClusterState, error)
	IsHealthy() bool
}

type DefaultBackend struct {
	ctrl.Controller
	Node *node.Node
}

func (b *DefaultBackend) GetClusterState(ctx context.Context) (*clusterpb.ClusterState, error) {
	return b.Node.GetClusterState(ctx)
}

func (b *DefaultBackend) IsHealthy() bool {
	return b.Node.IsHealthy()
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(node *node.Node, ctrl ctrl.Controller) *DefaultBackend {
	return &DefaultBackend{
		Node:       node,
		Controller: ctrl,
	}
}
