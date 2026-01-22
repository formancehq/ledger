package http

import (
	"context"
	"errors"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/http/bulking"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"github.com/formancehq/ledger-v3-poc/internal/store"
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
	service.Controller
	GetClusterState(context context.Context) (*ledgerpb.ClusterState, error)
	IsHealthy() bool
}

type DefaultBackend struct {
	*raft.Node
	connectionPool *transport.ConnectionPool
	nodeId         uint64
}

func (b *DefaultBackend) getCtrl() (service.Controller, error) {
	if b.IsLeader() {
		return b.Node, nil
	}
	if b.GetLeader() == 0 {
		return nil, ledgerpb.ErrNoLeader
	}

	grpcConn := b.connectionPool.GetConnection(b.GetLeader())

	return service.NewLedgerGrpcClient(ledgerpb.NewLedgerServiceClient(grpcConn)), nil
}

func (b *DefaultBackend) GetClusterState(ctx context.Context) (*ledgerpb.ClusterState, error) {
	return b.Node.GetClusterState(ctx)
}

func (b *DefaultBackend) CreateLedger(ctx context.Context, req *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error) {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return clusterLeader.CreateLedger(ctx, req)
}

func (b *DefaultBackend) GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	ledgerInfo, err := b.Node.GetLedgerByName(ctx, name)
	if err != nil && !errors.Is(err, &ledgerpb.NotFoundError{}) {
		return nil, err
	}
	if err == nil {
		return ledgerInfo, nil
	}

	clusterLeader, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return clusterLeader.GetLedgerByName(ctx, name)
}

func (b *DefaultBackend) DeleteLedger(ctx context.Context, id uint32) error {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return err
	}
	return clusterLeader.DeleteLedger(ctx, id)
}

func (b *DefaultBackend) IsHealthy() bool {
	return b.Node.IsHealthy()
}

func (b *DefaultBackend) GetAllLedgersInfo(ctx context.Context) (map[string]*ledgerpb.LedgerInfo, error) {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return clusterLeader.GetAllLedgersInfo(ctx)
}

func (b *DefaultBackend) CreateTransaction(ctx context.Context, ledger uint32, parameters service.Parameters[*ledgerpb.CreateTransactionRequestPayload]) (*ledgerpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.CreateTransaction(ctx, ledger, parameters)
}
func (b *DefaultBackend) RevertTransaction(ctx context.Context, ledger uint32, parameters service.Parameters[*ledgerpb.RevertTransactionRequestPayload]) (*ledgerpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.RevertTransaction(ctx, ledger, parameters)
}
func (b *DefaultBackend) SaveTransactionMetadata(ctx context.Context, ledger uint32, parameters service.Parameters[*ledgerpb.SaveTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.SaveTransactionMetadata(ctx, ledger, parameters)
}
func (b *DefaultBackend) SaveAccountMetadata(ctx context.Context, ledger uint32, parameters service.Parameters[*ledgerpb.SaveAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.SaveAccountMetadata(ctx, ledger, parameters)
}
func (b *DefaultBackend) DeleteTransactionMetadata(ctx context.Context, ledger uint32, parameters service.Parameters[*ledgerpb.DeleteTransactionMetadataRequestPayload]) (*ledgerpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.DeleteTransactionMetadata(ctx, ledger, parameters)
}
func (b *DefaultBackend) DeleteAccountMetadata(ctx context.Context, ledger uint32, parameters service.Parameters[*ledgerpb.DeleteAccountMetadataRequestPayload]) (*ledgerpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.DeleteAccountMetadata(ctx, ledger, parameters)
}
func (b *DefaultBackend) Import(ctx context.Context, ledger uint32, stream chan *ledgerpb.Log) error {
	ctrl, err := b.getCtrl()
	if err != nil {
		return err
	}

	return ctrl.Import(ctx, ledger, stream)
}
func (b *DefaultBackend) Export(ctx context.Context, ledger uint32, w service.ExportWriter) error {
	ctrl, err := b.getCtrl()
	if err != nil {
		return err
	}

	return ctrl.Export(ctx, ledger, w)
}
func (b *DefaultBackend) GetAllLogs(ctx context.Context, ledger uint32, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.GetAllLogs(ctx, ledger, from, to)
}

var _ Backend = (*DefaultBackend)(nil)

func NewDefaultBackend(node *raft.Node, connectionPool *transport.ConnectionPool, nodeId uint64) *DefaultBackend {
	return &DefaultBackend{
		Node:           node,
		connectionPool: connectionPool,
		nodeId:         nodeId,
	}
}
