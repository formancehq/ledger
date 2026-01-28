package service

import (
	"context"
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/ledger-v3-poc/internal/transport"
)

type RoutedController struct {
	*raft.Node
	connectionPool  *transport.ConnectionPool
	localController Controller
}

func (b *RoutedController) getCtrl() (Controller, error) {
	if b.IsLeader() {
		return b.localController, nil
	}
	if b.GetLeader() == 0 {
		return nil, commonpb.ErrNoLeader
	}

	grpcConn := b.connectionPool.GetConnection(b.GetLeader())

	return NewLedgerGrpcClient(servicepb.NewLedgerServiceClient(grpcConn)), nil
}

func (b *RoutedController) CreateLedger(ctx context.Context, req *raftcmdpb.CreateLedgerCommand) (*commonpb.LedgerInfo, error) {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return clusterLeader.CreateLedger(ctx, req)
}

func (b *RoutedController) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	ledgerInfo, err := b.localController.GetLedgerByName(ctx, name)
	if err != nil && !errors.Is(err, &commonpb.NotFoundError{}) {
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

func (b *RoutedController) DeleteLedger(ctx context.Context, id uint32) error {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return err
	}
	return clusterLeader.DeleteLedger(ctx, id)
}

func (b *RoutedController) IsHealthy() bool {
	return b.Node.IsHealthy()
}

func (b *RoutedController) GetAllLedgersInfo(ctx context.Context) (map[string]*commonpb.LedgerInfo, error) {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return clusterLeader.GetAllLedgersInfo(ctx)
}

func (b *RoutedController) Apply(ctx context.Context, action *servicepb.LedgerAction) (*commonpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return ctrl.Apply(ctx, action)
}

func (b *RoutedController) GetTransaction(ctx context.Context, ledger uint32, transactionID uint64) (*commonpb.Transaction, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.GetTransaction(ctx, ledger, transactionID)
}

func (b *RoutedController) Import(ctx context.Context, ledger uint32, stream chan *commonpb.Log) error {
	ctrl, err := b.getCtrl()
	if err != nil {
		return err
	}

	return ctrl.Import(ctx, ledger, stream)
}

func (b *RoutedController) Export(ctx context.Context, ledger uint32, w ExportWriter) error {
	ctrl, err := b.getCtrl()
	if err != nil {
		return err
	}

	return ctrl.Export(ctx, ledger, w)
}

func (b *RoutedController) GetAllLogs(ctx context.Context, ledger uint32, from uint64, to uint64) (store.Cursor[*commonpb.Log], error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.GetAllLogs(ctx, ledger, from, to)
}

var _ Controller = (*RoutedController)(nil)

func NewRoutedController(localController Controller, node *raft.Node, connectionPool *transport.ConnectionPool) *RoutedController {
	return &RoutedController{
		Node:            node,
		connectionPool:  connectionPool,
		localController: localController,
	}
}
