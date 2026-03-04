package http

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/infra/health"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
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
	IsReady() bool
	NotReadyReasons() []string
}

type DefaultBackend struct {
	ctrl.Controller
	Node          *node.Node
	healthChecker health.Checker
}

func (b *DefaultBackend) GetClusterState(ctx context.Context) (*clusterpb.ClusterState, error) {
	return b.Node.GetClusterState(ctx)
}

func (b *DefaultBackend) IsHealthy() bool {
	return b.Node.IsHealthy()
}

// IsReady returns true when the node is part of a healthy cluster: the local
// Raft state machine is healthy, a leader has been elected, and disk/clock
// health checks pass.
func (b *DefaultBackend) IsReady() bool {
	return b.Node.IsHealthy() && b.Node.GetLeader() != 0 && b.healthChecker.IsHealthy()
}

// NotReadyReasons returns a list of human-readable reasons why the node is not
// ready. Returns nil when the node is fully ready.
func (b *DefaultBackend) NotReadyReasons() []string {
	var reasons []string
	if !b.Node.IsHealthy() {
		reasons = append(reasons, "raft state machine is not healthy")
	}
	if b.Node.GetLeader() == 0 {
		reasons = append(reasons, "no leader elected")
	}
	if !b.healthChecker.IsHealthy() {
		reasons = append(reasons, "cluster health check failing (disk usage or clock skew)")
	}
	return reasons
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(node *node.Node, ctrl ctrl.Controller, healthChecker health.Checker) *DefaultBackend {
	return &DefaultBackend{
		Node:          node,
		Controller:    ctrl,
		healthChecker: healthChecker,
	}
}
