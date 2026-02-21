package application

import (
	"context"
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/service/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

type RoutedController struct {
	*node.Node
	servicePool     *transport.ConnectionPool
	localController ctrl.Controller
}

func (b *RoutedController) getCtrl() (ctrl.Controller, error) {
	if b.IsLeader() {
		return b.localController, nil
	}
	if b.GetLeader() == 0 {
		return nil, commonpb.ErrNoLeader
	}

	grpcConn := b.servicePool.GetConnection(b.GetLeader())
	if grpcConn == nil {
		return nil, commonpb.ErrNoLeader
	}

	return NewLedgerGrpcClient(servicepb.NewBucketServiceClient(grpcConn)), nil
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

func (b *RoutedController) IsHealthy() bool {
	return b.Node.IsHealthy()
}

func (b *RoutedController) ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	clusterLeader, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return clusterLeader.ListLedgers(ctx)
}

func (b *RoutedController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return ctrl.Apply(ctx, requests...)
}

func (b *RoutedController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.GetTransaction(ctx, ledgerName, transactionID)
}

func (b *RoutedController) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64) (dal.Cursor[*commonpb.Transaction], error) {
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}

	return ctrl.ListTransactions(ctx, ledgerName, pageSize, afterTxID)
}

func (b *RoutedController) ListPeriods(ctx context.Context) (dal.Cursor[*commonpb.Period], error) {
	// Period state is in-memory on the leader — route to leader
	ctrl, err := b.getCtrl()
	if err != nil {
		return nil, err
	}
	return ctrl.ListPeriods(ctx)
}

func (b *RoutedController) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32) (dal.Cursor[*commonpb.Log], error) {
	// Read from local store - logs are replicated via Raft
	return b.localController.ListLogs(ctx, afterSequence, pageSize)
}

func (b *RoutedController) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error) {
	// Read from local store - audit entries are replicated via Raft
	return b.localController.ListAuditEntries(ctx, afterSequence, failuresOnly, pageSize)
}

func (b *RoutedController) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	// Read from local store - audit entries are replicated via Raft
	return b.localController.GetAuditEntry(ctx, sequence)
}

func (b *RoutedController) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	// Read from local store - data is replicated via Raft
	return b.localController.GetAccount(ctx, ledgerName, address)
}

func (b *RoutedController) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, prefix string) (dal.Cursor[*commonpb.Account], error) {
	// Read from local store - data is replicated via Raft
	return b.localController.ListAccounts(ctx, ledgerName, pageSize, afterAddress, prefix)
}

func (b *RoutedController) ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	// Read from local store - signing keys are replicated via Raft
	return b.localController.ListSigningKeys(ctx)
}

var _ ctrl.Controller = (*RoutedController)(nil)

func NewRoutedController(localController ctrl.Controller, node *node.Node, servicePool *transport.ConnectionPool) *RoutedController {
	return &RoutedController{
		Node:            node,
		servicePool:     servicePool,
		localController: localController,
	}
}
