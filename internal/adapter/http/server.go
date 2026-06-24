package http

import (
	"context"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/health"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type Server struct {
	logger      logging.Logger
	backend     Backend
	bulkMaxSize int
}

// NewServer creates a new server instance (used by handlers).
func NewServer(logger logging.Logger, backend Backend, bulkMaxSize int) *Server {
	return &Server{
		logger:      logger,
		backend:     backend,
		bulkMaxSize: bulkMaxSize,
	}
}

// applyUnsigned wraps Requests into unsigned Envelopes and forwards to the
// backend. The HTTP API never signs requests itself — signing flows through
// gRPC where the caller controls the envelope construction.
func (s *Server) applyUnsigned(ctx context.Context, idempotencyKey string, reqs ...*servicepb.Request) ([]*commonpb.Log, error) {
	return s.backend.Apply(ctx, servicepb.UnsignedApplyRequest(idempotencyKey, reqs...))
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -destination backend_generated_test.go -typed -package http . Backend

type Backend interface {
	ctrl.Controller
	GetClusterState(context context.Context) (*clusterpb.ClusterState, error)
	IsHealthy() bool
	IsReady() bool
	NotReadyReasons() []string
	IsClusterReady() bool
	NotClusterReadyReasons() []string
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

// IsReady returns true once the local Raft loop has started, regardless of
// whether a leader has been elected. This is the StatefulSet readiness gate:
// it must return true even during a cold-start where quorum is not yet
// achievable, otherwise OrderedReady deadlocks the cluster on first boot.
// Use IsClusterReady for the stricter "the node can actually serve traffic"
// signal.
func (b *DefaultBackend) IsReady() bool {
	return b.Node.IsStarted()
}

// NotReadyReasons returns a list of human-readable reasons why the node is not
// ready (i.e. its Raft loop has not started yet). Returns nil when the node is
// ready.
func (b *DefaultBackend) NotReadyReasons() []string {
	if !b.Node.IsStarted() {
		return []string{"raft loop has not started"}
	}

	return nil
}

// IsClusterReady returns true when the node is part of a healthy cluster: the
// local Raft state machine is connected (leader or follower), a leader has
// been elected, and disk/clock health checks pass. This is the strict signal
// for "this node can serve cluster-dependent traffic"; expose it on /clusterz
// for monitoring and clients that need cluster-availability semantics.
func (b *DefaultBackend) IsClusterReady() bool {
	return b.Node.IsHealthy() && b.Node.GetLeader() != 0 && b.healthChecker.IsHealthy()
}

// NotClusterReadyReasons returns a list of human-readable reasons why the
// cluster-readiness check is failing. Returns nil when IsClusterReady is true.
func (b *DefaultBackend) NotClusterReadyReasons() []string {
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
