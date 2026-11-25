package http

import (
	"context"

	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type Server struct {
	logger      logging.Logger
	backend     Backend
	authCfg     internalauth.AuthConfig
	bulkMaxSize int
}

// NewServer creates a new server instance (used by handlers).
func NewServer(logger logging.Logger, backend Backend, authCfg internalauth.AuthConfig, bulkMaxSize int) *Server {
	return &Server{
		logger:      logger,
		backend:     backend,
		authCfg:     authCfg,
		bulkMaxSize: bulkMaxSize,
	}
}

// applyUnsigned wraps Requests into unsigned Envelopes and forwards to the
// backend. Signed transaction routes use applyRequests instead; signing always
// remains client-side.
func (s *Server) applyUnsigned(ctx context.Context, idempotencyKey string, reqs ...*servicepb.Request) ([]*commonpb.Log, error) {
	result, err := s.backend.Apply(ctx, servicepb.UnsignedApplyRequest(idempotencyKey, reqs...))
	if err != nil {
		return nil, err
	}

	return result.Logs, nil
}

// applyRequests never signs or reserializes a client's payload. PeekBatch is
// non-authoritative inspection: matching the full batch prevents route/scope
// tunneling, and backend.Apply still verifies the original signature and policy.
func (s *Server) applyRequests(ctx context.Context, key string, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	envelope := signedBatchFromContext(ctx)
	if envelope == nil {
		return s.applyUnsigned(ctx, key, requests...)
	}
	apply := servicepb.SignedApplyRequest(envelope)
	batch, err := servicepb.PeekBatch(apply)
	if err != nil || !proto.Equal(batch, &servicepb.ApplyBatch{IdempotencyKey: key, Requests: requests}) {
		return nil, signing.ErrInvalidSignature
	}
	result, err := s.backend.Apply(ctx, apply)
	if err != nil {
		return nil, err
	}

	return result.Logs, nil
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source server.go -destination backend_generated_test.go -typed -package http Backend

type Backend interface {
	ctrl.Controller
	ClusterAdmin
	GetClusterState(context context.Context) (*clusterpb.ClusterState, error)
	IsHealthy() bool
	IsReady() bool
	NotReadyReasons() []string
	IsClusterReady() bool
	NotClusterReadyReasons() []string
}

// ClusterAdmin is the cluster administration surface of the gRPC
// ClusterService. HTTP routes call it in process so leader forwarding, drain
// preflight and request validation match the gRPC contract.
type ClusterAdmin interface {
	TransferLeadership(ctx context.Context, req *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error)
	AddLearner(ctx context.Context, req *clusterpb.AddLearnerRequest) (*clusterpb.AddLearnerResponse, error)
	PromoteLearner(ctx context.Context, req *clusterpb.PromoteLearnerRequest) (*clusterpb.PromoteLearnerResponse, error)
	RemoveNode(ctx context.Context, req *clusterpb.RemoveNodeRequest) (*clusterpb.RemoveNodeResponse, error)
	BeginDrain(ctx context.Context, req *clusterpb.BeginDrainRequest) (*clusterpb.BeginDrainResponse, error)
	EndDrain(ctx context.Context, req *clusterpb.EndDrainRequest) (*clusterpb.EndDrainResponse, error)
	GetDrainStatus(ctx context.Context, req *clusterpb.GetDrainStatusRequest) (*clusterpb.DrainStatus, error)
	CompactPrimary(ctx context.Context, req *clusterpb.CompactPrimaryRequest) (*clusterpb.CompactPrimaryResponse, error)
	CompactSecondary(ctx context.Context, req *clusterpb.CompactSecondaryRequest) (*clusterpb.CompactSecondaryResponse, error)
}

var _ ClusterAdmin = (clusterpb.ClusterServiceServer)(nil)

type DefaultBackend struct {
	ctrl.Controller
	ClusterAdmin

	Node *node.Node
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
// local Raft state machine is connected (leader or follower) and a leader has
// been elected. Disk and clock skew gate writes (see admission), not cluster
// readiness. This is the strict signal for "this node can serve
// cluster-dependent traffic"; expose it on /clusterz for monitoring and
// clients that need cluster-availability semantics.
func (b *DefaultBackend) IsClusterReady() bool {
	return b.Node.IsHealthy() && b.Node.GetLeader() != 0
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

	return reasons
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(node *node.Node, ctrl ctrl.Controller, cluster ClusterAdmin) *DefaultBackend {
	return &DefaultBackend{
		Node:         node,
		Controller:   ctrl,
		ClusterAdmin: cluster,
	}
}
