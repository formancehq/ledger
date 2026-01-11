package http

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/http/bulking"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/systempb"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
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
	GetClusterState(context context.Context) (*ledgerpb.ClusterState[*systempb.State], error)
	CreateLedger(ctx context.Context, req *systempb.CreateLedgerRequest) (*ledgerpb.LedgerInfo, error)
	GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error)
	GetLedger(ctx context.Context, name string) (service.Ledger, error)
	DeleteLedger(ctx context.Context, name string) error
	GetLedgerClusterState(ctx context.Context, name string) (*ledgerpb.ClusterState[*ledgerpb.LedgerState], error)
	IsHealthy() bool
	GetAllLedgers(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error)
}

type DefaultBackend struct {
	*system.Node
	connectionPool *transport.ConnectionPool
	nodeId         uint64
}

func (b *DefaultBackend) getMainCluster() (service.System, error) {
	if b.IsLeader() {
		return b.Node, nil
	}
	if b.GetLeader() == 0 {
		return nil, ledgerpb.ErrNoLeader
	}

	grpcConn := b.connectionPool.GetConnection(b.GetLeader())

	return service.NewGrpcSystemClient(systempb.NewSystemServiceClient(grpcConn)), nil
}

func (b *DefaultBackend) GetClusterState(ctx context.Context) (*ledgerpb.ClusterState[*systempb.State], error) {
	return b.Node.GetClusterState(ctx)
}

func (b *DefaultBackend) CreateLedger(ctx context.Context, req *systempb.CreateLedgerRequest) (*ledgerpb.LedgerInfo, error) {
	clusterLeader, err := b.getMainCluster()
	if err != nil {
		return nil, err
	}
	return clusterLeader.CreateLedger(ctx, req)
}

func (b *DefaultBackend) GetLedgerInfo(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	return b.Node.GetLedgerInfo(ctx, name)
}

func (b *DefaultBackend) GetLedger(ctx context.Context, name string) (service.Ledger, error) {
	ledgerNode, err := b.GetLedgerNode(ctx, name)
	if err != nil {
		return nil, err
	}

	if ledgerNode.IsLeader() {
		return ledgerNode, nil
	}

	leader := ledgerNode.GetLeader()
	if leader == 0 {
		return nil, ledgerpb.ErrNoLeader
	}
	ledgerLeaderNodeID := system.NodeIDFromLedgerNodeID(leader)

	grpcConn := b.connectionPool.GetConnection(ledgerLeaderNodeID)
	if grpcConn == nil {
		logging.FromContext(ctx).Errorf("no connection for node %d", ledgerLeaderNodeID)
		return nil, ledgerpb.ErrNoLeader
	}

	return service.NewLedgerGrpcClient(name, ledgerpb.NewLedgerServiceClient(grpcConn)), nil
}

func (b *DefaultBackend) DeleteLedger(ctx context.Context, name string) error {
	clusterLeader, err := b.getMainCluster()
	if err != nil {
		return err
	}
	return clusterLeader.DeleteLedger(ctx, name)
}

func (b *DefaultBackend) GetLedgerClusterState(ctx context.Context, name string) (*ledgerpb.ClusterState[*ledgerpb.LedgerState], error) {
	ledgerNode, err := b.GetLedgerNode(ctx, name)
	if err != nil {
		return nil, err
	}

	return ledgerNode.GetClusterState(ctx)
}

func (b *DefaultBackend) IsHealthy() bool {
	return b.Node.IsHealthy()
}

func (b *DefaultBackend) GetAllLedgers(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	return b.GetAllLedgersInfo(ctx)
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(node *system.Node, connectionPool *transport.ConnectionPool, nodeId uint64) *DefaultBackend {
	return &DefaultBackend{
		Node:           node,
		connectionPool: connectionPool,
		nodeId:         nodeId,
	}
}
