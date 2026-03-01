package bootstrap

import (
	"context"
	"errors"

	grpcadp "github.com/formancehq/ledger-v3-poc/internal/adapter/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

type RoutedController struct {
	*node.Node
	servicePool     *transport.ConnectionPool
	localController ctrl.Controller
}

// getLeaderCtrl returns a controller that talks to the leader.
// Used only for operations that must execute on the leader (Apply, ListPeriods).
func (b *RoutedController) getLeaderCtrl() (ctrl.Controller, error) {
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

	return grpcadp.NewLedgerGrpcClient(servicepb.NewBucketServiceClient(grpcConn)), nil
}

// readCtrl returns the controller to use for a read operation.
// The consistency level is determined from the context (set by the gRPC interceptor):
//   - linearizable (default): ReadIndex+WaitForApplied barrier on the local node
//   - stale: skip the barrier and read from the local store directly
//   - leader: forward the read to the leader node
//
// For linearizable reads, if the local node is still syncing the read is
// transparently forwarded to the leader.
func (b *RoutedController) readCtrl(ctx context.Context) (ctrl.Controller, error) {
	switch grpcadp.ConsistencyFromContext(ctx) {
	case grpcadp.ConsistencyStale:
		return b.localController, nil
	case grpcadp.ConsistencyLeader:
		return b.getLeaderCtrl()
	}

	err := b.ReadIndexAndWait(ctx)
	if err == nil {
		return b.localController, nil
	}
	if errors.Is(err, node.ErrNodeSyncing) || errors.Is(err, node.ErrNotLeader) {
		return b.getLeaderCtrl()
	}
	return nil, err
}

func (b *RoutedController) IsHealthy() bool {
	return b.Node.IsHealthy()
}

// --- Write operations: forwarded to leader ---

func (b *RoutedController) Apply(ctx context.Context, requests ...*servicepb.Request) ([]*commonpb.Log, error) {
	leaderCtrl, err := b.getLeaderCtrl()
	if err != nil {
		return nil, err
	}
	return leaderCtrl.Apply(ctx, requests...)
}

// --- Read operations requiring leader state: forwarded to leader ---

func (b *RoutedController) ListPeriods(ctx context.Context) (dal.Cursor[*commonpb.Period], error) {
	// Period state is in-memory on the leader — route to leader
	leaderCtrl, err := b.getLeaderCtrl()
	if err != nil {
		return nil, err
	}
	return leaderCtrl.ListPeriods(ctx)
}

// --- Linearizable reads: ReadIndex + local read ---

func (b *RoutedController) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetLedgerByName(ctx, name)
}

func (b *RoutedController) ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.ListLedgers(ctx)
}

func (b *RoutedController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetTransaction(ctx, ledgerName, transactionID)
}

func (b *RoutedController) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Transaction], error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.ListTransactions(ctx, ledgerName, pageSize, afterTxID, filter, reverse)
}

func (b *RoutedController) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32) (dal.Cursor[*commonpb.Log], error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.ListLogs(ctx, afterSequence, pageSize)
}

func (b *RoutedController) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetLog(ctx, sequence)
}

func (b *RoutedController) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32) (dal.Cursor[*auditpb.AuditEntry], error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.ListAuditEntries(ctx, afterSequence, failuresOnly, pageSize)
}

func (b *RoutedController) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetAuditEntry(ctx, sequence)
}

func (b *RoutedController) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetAccount(ctx, ledgerName, address)
}

func (b *RoutedController) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Account], error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.ListAccounts(ctx, ledgerName, pageSize, afterAddress, filter, reverse)
}

func (b *RoutedController) ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.ListSigningKeys(ctx)
}

func (b *RoutedController) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.GetMetadataSchemaStatus(ctx, ledgerName)
}

func (b *RoutedController) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32) (*servicepb.AnalyzeAccountsResponse, error) {
	c, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}
	return c.AnalyzeAccounts(ctx, ledgerName, variableThreshold)
}

func (b *RoutedController) ListPreparedQueries(ctx context.Context, ledger string) ([]*commonpb.PreparedQuery, error) {
	// Read from local store - prepared queries are replicated via Raft
	return b.localController.ListPreparedQueries(ctx, ledger)
}

func (b *RoutedController) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	// Execute locally - both read index and Pebble data are available on all nodes
	return b.localController.ExecutePreparedQuery(ctx, req)
}

var _ ctrl.Controller = (*RoutedController)(nil)

func NewRoutedController(localController ctrl.Controller, node *node.Node, servicePool *transport.ConnectionPool) *RoutedController {
	return &RoutedController{
		Node:            node,
		servicePool:     servicePool,
		localController: localController,
	}
}
