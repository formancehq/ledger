package bootstrap

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	grpcadp "github.com/formancehq/ledger-v3-poc/internal/adapter/grpc"
	"github.com/formancehq/ledger-v3-poc/internal/application/ctrl"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

var routerTracer = otel.Tracer("router")

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

// readCtrl returns the controller to use for a read operation, along with
// diagnostic barrier info (nil when the read is stale or forwarded).
// The consistency level is determined from the context (set by the gRPC interceptor):
//   - linearizable (default): ReadIndex+WaitForApplied barrier on the local node
//   - stale: skip the barrier and read from the local store directly
//   - leader: forward the read to the leader node
//
// For linearizable reads, if the local node is still syncing the read is
// transparently forwarded to the leader.
func (b *RoutedController) readCtrl(ctx context.Context) (ctrl.Controller, *node.ReadBarrierInfo, error) {
	consistency := grpcadp.ConsistencyFromContext(ctx)

	ctx, span := routerTracer.Start(ctx, "router.read_ctrl",
		trace.WithAttributes(attribute.String("consistency", consistency)))
	defer span.End()

	switch consistency {
	case grpcadp.ConsistencyStale:
		span.SetAttributes(attribute.String("route", "local_stale"))

		return b.localController, nil, nil
	case grpcadp.ConsistencyLeader:
		span.SetAttributes(attribute.String("route", "leader"))

		c, err := b.getLeaderCtrl()

		return c, nil, err
	}

	barrier, err := b.ReadIndexAndWait(ctx)
	if err == nil {
		span.SetAttributes(attribute.String("route", "local_linearizable"))

		return b.localController, barrier, nil
	}

	if errors.Is(err, node.ErrNodeSyncing) || errors.Is(err, node.ErrNotLeader) {
		// Only fallback to the leader if we are NOT the leader ourselves.
		// If we ARE the leader but ReadIndex failed (quorum not yet confirmed
		// after election), we must NOT serve a stale local read without a barrier.
		if !b.IsLeader() {
			span.SetAttributes(attribute.String("route", "leader_fallback"))

			c, leaderErr := b.getLeaderCtrl()

			return c, nil, leaderErr
		}

		span.SetAttributes(attribute.String("route", "leader_readindex_failed"))
	}

	return nil, nil, err
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

func (b *RoutedController) Barrier(ctx context.Context) (uint64, error) {
	leaderCtrl, err := b.getLeaderCtrl()
	if err != nil {
		return 0, err
	}

	return leaderCtrl.Barrier(ctx)
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
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetLedgerByName(ctx, name)
}

func (b *RoutedController) ListLedgers(ctx context.Context) (dal.Cursor[*commonpb.LedgerInfo], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListLedgers(ctx)
}

func (b *RoutedController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	tx, err := c.GetTransaction(ctx, ledgerName, transactionID)
	if errors.Is(err, &commonpb.NotFoundError{}) {
		fields := map[string]any{
			"ledger":         ledgerName,
			"transactionId":  transactionID,
			"nodeId":         b.GetNodeID(),
			"leader":         b.GetLeader(),
			"isLeader":       b.IsLeader(),
			"persistedIndex": b.LastPersistedIndex(),
			"forwarded":      c != b.localController,
		}
		if barrier != nil {
			fields["barrierCommitIndex"] = barrier.CommitIndex
			fields["barrierPersistedAfter"] = barrier.PersistedAfter
		}

		b.Node.Logger().WithFields(fields).Errorf("GetTransaction returned NotFound for committed transaction")
	}

	return tx, err
}

func (b *RoutedController) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Transaction], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListTransactions(ctx, ledgerName, pageSize, afterTxID, filter, reverse)
}

func (b *RoutedController) ListLogs(ctx context.Context, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (dal.Cursor[*commonpb.Log], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListLogs(ctx, afterSequence, pageSize, filter)
}

func (b *RoutedController) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetLog(ctx, sequence)
}

func (b *RoutedController) ListAuditEntries(ctx context.Context, afterSequence *uint64, failuresOnly bool, pageSize uint32, ledger string) (dal.Cursor[*auditpb.AuditEntry], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListAuditEntries(ctx, afterSequence, failuresOnly, pageSize, ledger)
}

func (b *RoutedController) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetAuditEntry(ctx, sequence)
}

func (b *RoutedController) GetAccount(ctx context.Context, ledgerName string, address string) (*commonpb.Account, error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	if barrier != nil {
		b.Node.Logger().WithFields(map[string]any{
			"op":             "GetAccount",
			"ledger":         ledgerName,
			"address":        address,
			"commitIndex":    barrier.CommitIndex,
			"persistedAfter": barrier.PersistedAfter,
			"currentPersist": b.Node.LastPersistedIndex(),
			"nodeId":         b.Node.GetNodeID(),
		}).Infof("read barrier for GetAccount")
	}

	return c.GetAccount(ctx, ledgerName, address)
}

func (b *RoutedController) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (dal.Cursor[*commonpb.Account], error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	if barrier != nil {
		b.Node.Logger().WithFields(map[string]any{
			"op":             "ListAccounts",
			"ledger":         ledgerName,
			"commitIndex":    barrier.CommitIndex,
			"persistedAfter": barrier.PersistedAfter,
			"currentPersist": b.Node.LastPersistedIndex(),
			"nodeId":         b.Node.GetNodeID(),
		}).Infof("read barrier for ListAccounts")
	}

	return c.ListAccounts(ctx, ledgerName, pageSize, afterAddress, filter, reverse)
}

func (b *RoutedController) AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.AggregateVolumes(ctx, ledgerName, filter, opts)
}

func (b *RoutedController) ListSigningKeys(ctx context.Context) (dal.Cursor[*commonpb.SigningKey], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListSigningKeys(ctx)
}

func (b *RoutedController) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetMetadataSchemaStatus(ctx, ledgerName)
}

func (b *RoutedController) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.AnalyzeAccounts(ctx, ledgerName, variableThreshold, onProgress)
}

func (b *RoutedController) AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.AnalyzeTransactions(ctx, ledgerName, variableThreshold, onProgress)
}

func (b *RoutedController) ListPreparedQueries(ctx context.Context, ledger string) ([]*commonpb.PreparedQuery, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListPreparedQueries(ctx, ledger)
}

func (b *RoutedController) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ExecutePreparedQuery(ctx, req)
}

func (b *RoutedController) GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetLedgerStats(ctx, ledgerName)
}

func (b *RoutedController) GetNumscript(ctx context.Context, ledger, name string, version string) (*commonpb.NumscriptInfo, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetNumscript(ctx, ledger, name, version)
}

func (b *RoutedController) ListNumscripts(ctx context.Context, ledger string) ([]*commonpb.NumscriptInfo, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListNumscripts(ctx, ledger)
}

func (b *RoutedController) GetPeriodSchedule(ctx context.Context) (string, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return "", err
	}

	return c.GetPeriodSchedule(ctx)
}

func (b *RoutedController) GetEventsSinks(ctx context.Context) ([]*commonpb.SinkConfig, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetEventsSinks(ctx)
}

var _ ctrl.Controller = (*RoutedController)(nil)

func NewRoutedController(localController ctrl.Controller, node *node.Node, servicePool *transport.ConnectionPool) *RoutedController {
	return &RoutedController{
		Node:            node,
		servicePool:     servicePool,
		localController: localController,
	}
}
