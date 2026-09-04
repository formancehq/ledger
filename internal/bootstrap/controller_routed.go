package bootstrap

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

var routerTracer = otel.Tracer("router")

type RoutedController struct {
	*node.Node

	servicePool     *transport.ConnectionPool
	localController ctrl.Controller
}

// getLeaderCtrl returns the local controller when this node considers itself
// leader, or a client controller for the currently known remote leader.
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
//
// For linearizable reads, if the local node is still syncing the read is
// transparently forwarded to the leader.
func (b *RoutedController) readCtrl(ctx context.Context) (ctrl.Controller, *node.ReadBarrierInfo, error) {
	consistency := grpcadp.ConsistencyFromContext(ctx)

	ctx, span := routerTracer.Start(ctx, "router.read_ctrl",
		trace.WithAttributes(attribute.String("consistency", consistency)))
	defer span.End()

	if consistency == grpcadp.ConsistencyStale {
		span.SetAttributes(attribute.String("route", "local_stale"))

		return b.localController, nil, nil
	}

	// The ReadIndex quorum round-trip plus the local WaitForApplied catch-up is
	// consensus latency incurred to honour the caller's linearizable-read
	// request, not query work. Charge it to the profile's barrier phase so it is
	// visible but excluded from the server-cost total (EN-1859). Charged whether
	// or not the barrier succeeds — a failed attempt is still time the caller
	// waited (see the fallback branch below).
	barrierStart := time.Now()
	barrier, err := b.ReadIndexAndWait(ctx)
	query.ProfileFromContext(ctx).AddBarrierWait(time.Since(barrierStart))

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

			// The read leaves this node, so the phase breakdown describes the
			// local hop only. The barrier already recorded above is KEPT: the caller
			// really did wait for a quorum attempt that then failed. Dropping it
			// would not delete the time — readCtrl runs inside the caller's
			// EnterExecute/LeaveExecute bracket, so an uncharged wait stays in
			// execute_duration_us and from there in the server total. Hence
			// forwarded=true with a non-zero barrier_duration_us is a valid,
			// documented combination.
			c, leaderErr := b.getLeaderCtrl()

			return b.finishLeaderFallback(ctx, c, leaderErr, err)
		}

		span.SetAttributes(attribute.String("route", "leader_readindex_failed"))
	}

	return nil, nil, err
}

// finishLeaderFallback closes the leadership-transition race between the
// initial IsLeader check and getLeaderCtrl. If leadership moved to this node in
// that window, getLeaderCtrl returns the local controller; serving it would
// bypass the ReadIndex barrier that just failed. Return the original barrier
// failure instead. A route is profiled as forwarded only after a remote
// controller has been resolved successfully.
func (b *RoutedController) finishLeaderFallback(ctx context.Context, selected ctrl.Controller, resolutionErr, barrierErr error) (ctrl.Controller, *node.ReadBarrierInfo, error) {
	if resolutionErr != nil {
		return nil, nil, resolutionErr
	}

	if selected == b.localController {
		return nil, nil, barrierErr
	}

	query.ProfileFromContext(ctx).MarkForwarded()

	return selected, nil, nil
}

func (b *RoutedController) withLocalBarrierHorizon(ctx context.Context, selected ctrl.Controller, barrier *node.ReadBarrierInfo) context.Context {
	if selected == b.localController && barrier != nil {
		return query.WithReadBarrierHorizon(ctx, barrier.CommitIndex)
	}

	return ctx
}

func (b *RoutedController) IsHealthy() bool {
	return b.Node.IsHealthy()
}

// --- Write operations: routed to leader ---

func (b *RoutedController) Apply(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
	leaderCtrl, err := b.getLeaderCtrl()
	if err != nil {
		return nil, err
	}

	return leaderCtrl.Apply(ctx, req)
}

func (b *RoutedController) Barrier(ctx context.Context) (uint64, error) {
	leaderCtrl, err := b.getLeaderCtrl()
	if err != nil {
		return 0, err
	}

	return leaderCtrl.Barrier(ctx)
}

// --- Linearizable reads: ReadIndex + local read ---

func (b *RoutedController) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetLedgerByName(ctx, name)
}

func (b *RoutedController) ListLedgers(ctx context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListLedgers(ctx)
}

func (b *RoutedController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, *string, error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, nil, err
	}

	tx, receipt, err := c.GetTransaction(ctx, ledgerName, transactionID)
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

	return tx, receipt, err
}

func (b *RoutedController) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListTransactions(b.withLocalBarrierHorizon(ctx, c, barrier), ledgerName, pageSize, afterTxID, filter, reverse)
}

func (b *RoutedController) ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListLogs(b.withLocalBarrierHorizon(ctx, c, barrier), ledgerName, afterSequence, pageSize, filter)
}

func (b *RoutedController) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetLog(ctx, sequence)
}

func (b *RoutedController) ListAuditEntries(ctx context.Context, pageSize uint32, afterSequence uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListAuditEntries(b.withLocalBarrierHorizon(ctx, c, barrier), pageSize, afterSequence, filter, reverse)
}

func (b *RoutedController) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetAuditEntry(ctx, sequence)
}

func (b *RoutedController) GetAccount(ctx context.Context, ledgerName string, address string, opts ctrl.GetAccountOptions) (*commonpb.Account, error) {
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

	return c.GetAccount(ctx, ledgerName, address, opts)
}

func (b *RoutedController) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Account], error) {
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

	return c.ListAccounts(b.withLocalBarrierHorizon(ctx, c, barrier), ledgerName, pageSize, afterAddress, filter, reverse)
}

func (b *RoutedController) AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.AggregateVolumes(b.withLocalBarrierHorizon(ctx, c, barrier), ledgerName, filter, opts)
}

func (b *RoutedController) ListSigningKeys(ctx context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
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
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ExecutePreparedQuery(b.withLocalBarrierHorizon(ctx, c, barrier), req)
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

func (b *RoutedController) GetTemplateUsage(ctx context.Context, ledger, name string) (*commonpb.TemplateUsage, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetTemplateUsage(ctx, ledger, name)
}

func (b *RoutedController) ListNumscriptVersions(ctx context.Context, ledger, name string) (string, []*commonpb.NumscriptVersionEntry, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return "", nil, err
	}

	return c.ListNumscriptVersions(ctx, ledger, name)
}

func (b *RoutedController) GetEventsSinks(ctx context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, nil, err
	}

	return c.GetEventsSinks(ctx)
}

func (b *RoutedController) InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
	c, barrier, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.InspectIndex(b.withLocalBarrierHorizon(ctx, c, barrier), req)
}

func (b *RoutedController) GetIndexStatus(ctx context.Context, req *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetIndexStatus(ctx, req)
}

func (b *RoutedController) GetIndex(ctx context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetIndex(ctx, req)
}

func (b *RoutedController) GetIndexEntryStatus(ctx context.Context, req *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetIndexEntryStatus(ctx, req)
}

func (b *RoutedController) ListIndexes(ctx context.Context, req *servicepb.ListIndexesRequest) (cursor.Cursor[*commonpb.Index], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListIndexes(ctx, req)
}

var _ ctrl.Controller = (*RoutedController)(nil)

func NewRoutedController(localController ctrl.Controller, node *node.Node, servicePool *transport.ConnectionPool) *RoutedController {
	return &RoutedController{
		Node:            node,
		servicePool:     servicePool,
		localController: localController,
	}
}
