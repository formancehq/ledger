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
	"github.com/formancehq/ledger/v3/internal/domain"
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
	// leaderResolver resolves the forward target for stale-binding retries;
	// nil means getLeaderCtrl. Tests inject a stub to drive the retry path
	// without a raft soft state.
	leaderResolver func() (ctrl.Controller, error)
	// readBarrier performs the linearizable-read barrier; nil means
	// Node.ReadIndexAndWait. Tests inject a stub to drive the wrapped read
	// methods end to end without a raft node.
	readBarrier func(ctx context.Context) (*node.ReadBarrierInfo, error)
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
//   - leader: route the read to the node currently considered leader; the local
//     leader shortcut does not perform a ReadIndex barrier
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
		if err != nil {
			return nil, nil, err
		}

		// When getLeaderCtrl returns a remote controller, that node runs its own
		// ReadIndex barrier (x-consistency is not propagated, so it defaults to
		// linearizable there). The remote barrier and execution are invisible to
		// this profile — the whole remote cost arrives as row-production time
		// inside the local execute phase, and this node's barrier_duration_us stays
		// 0. Flag it so a reader does not take that 0 to mean "no barrier was
		// needed" (EN-1859). When this node already considers itself leader,
		// getLeaderCtrl returns the local controller and no barrier is performed.
		b.markForwardedIfRemote(ctx, c)

		return c, nil, nil
	}

	// The ReadIndex quorum round-trip plus the local WaitForApplied catch-up is
	// consensus latency incurred to honour the caller's linearizable-read
	// request, not query work. Charge it to the profile's barrier phase so it is
	// visible but excluded from the server-cost total (EN-1859). Charged whether
	// or not the barrier succeeds — a failed attempt is still time the caller
	// waited (see the fallback branch below).
	readIndexAndWait := b.Node.ReadIndexAndWait
	if b.readBarrier != nil {
		readIndexAndWait = b.readBarrier
	}

	barrierStart := time.Now()
	barrier, err := readIndexAndWait(ctx)
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

			// Same as the explicit-leader branch: the read leaves this node, so
			// the phase breakdown describes the local hop only. Unlike that
			// branch, the barrier already recorded above is KEPT: the caller
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

// markForwardedIfRemote preserves the query-profile contract that Forwarded
// means another node served the read. The explicit leader-consistency path can
// resolve to the local controller when this node considers itself leader.
func (b *RoutedController) markForwardedIfRemote(ctx context.Context, selected ctrl.Controller) {
	if selected != b.localController {
		query.ProfileFromContext(ctx).MarkForwarded()
	}
}

// retryOnStaleBinding re-runs a read on the leader when the local replica
// refused it as INDEX_BUILDING. A rewound read store (WAL-less; a hard kill
// rewinds it to the last flush) re-walks the index's retype chain, and until
// its bindings converge on the schema the compile gate refuses the stale
// semantics — a rebuild in progress. The refusal is per-index and retryable,
// but a converged replica can answer NOW, and the leader is the replica
// least likely to be mid-rebuild: forward instead of bouncing the client.
//
// The forward keys on the whole INDEX_BUILDING class, not just rewound
// bindings: an initial backfill or activation-pending index refuses the same
// way, and the same reasoning applies — a replica whose build is complete can
// serve what this one cannot yet.
//
// The leader never forwards (no loops); if the leader itself is mid-rebuild,
// the client keeps the retryable INDEX_BUILDING. Explicitly-stale reads are
// exempt — they ask for THIS node's view, and forwarding would silently
// substitute another node's. Only a refusal served by the LOCAL controller
// forwards: when readCtrl already routed the read to a remote leader (explicit
// leader consistency, or the syncing fallback), it already ran on the node a
// forward would target, and re-sending the identical read buys nothing.
func retryOnStaleBinding[T any](b *RoutedController, ctx context.Context, served ctrl.Controller, out T, err error, run func(ctrl.Controller) (T, error)) (T, error) {
	if !shouldForwardIndexBuilding(err, served == b.localController, b.IsLeader(), grpcadp.ConsistencyFromContext(ctx)) {
		return out, err
	}

	resolve := b.getLeaderCtrl
	if b.leaderResolver != nil {
		resolve = b.leaderResolver
	}

	leader, leaderErr := resolve()
	if leaderErr != nil || leader == b.localController {
		// Resolution failed, or leadership moved here after the IsLeader check
		// (re-running locally would repeat the refusal). The local refusal is
		// already the retryable class — surface it.
		return out, err
	}

	query.ProfileFromContext(ctx).MarkForwarded()

	return run(leader)
}

// shouldForwardIndexBuilding is retryOnStaleBinding's decision: forward only a
// follower's own local INDEX_BUILDING refusal on a read that did not ask for
// this node's view.
func shouldForwardIndexBuilding(err error, servedLocally, isLeader bool, consistency string) bool {
	var building *domain.ErrIndexBuilding
	if err == nil || !errors.As(err, &building) || !servedLocally || isLeader {
		return false
	}

	return consistency != grpcadp.ConsistencyStale
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
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	out, err := c.ListTransactions(ctx, ledgerName, pageSize, afterTxID, filter, reverse)

	return retryOnStaleBinding(b, ctx, c, out, err, func(leader ctrl.Controller) (cursor.Cursor[*commonpb.Transaction], error) {
		return leader.ListTransactions(ctx, ledgerName, pageSize, afterTxID, filter, reverse)
	})
}

func (b *RoutedController) ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	out, err := c.ListLogs(ctx, ledgerName, afterSequence, pageSize, filter)

	return retryOnStaleBinding(b, ctx, c, out, err, func(leader ctrl.Controller) (cursor.Cursor[*commonpb.Log], error) {
		return leader.ListLogs(ctx, ledgerName, afterSequence, pageSize, filter)
	})
}

func (b *RoutedController) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.GetLog(ctx, sequence)
}

func (b *RoutedController) ListAuditEntries(ctx context.Context, pageSize uint32, afterSequence uint64, filter *commonpb.QueryFilter, reverse bool, minLogSequence uint64) (cursor.Cursor[*auditpb.AuditEntry], error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	return c.ListAuditEntries(ctx, pageSize, afterSequence, filter, reverse, minLogSequence)
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

	out, err := c.ListAccounts(ctx, ledgerName, pageSize, afterAddress, filter, reverse)

	return retryOnStaleBinding(b, ctx, c, out, err, func(leader ctrl.Controller) (cursor.Cursor[*commonpb.Account], error) {
		return leader.ListAccounts(ctx, ledgerName, pageSize, afterAddress, filter, reverse)
	})
}

func (b *RoutedController) AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	out, err := c.AggregateVolumes(ctx, ledgerName, filter, opts)

	return retryOnStaleBinding(b, ctx, c, out, err, func(leader ctrl.Controller) (*commonpb.AggregateResult, error) {
		return leader.AggregateVolumes(ctx, ledgerName, filter, opts)
	})
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
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	out, err := c.ExecutePreparedQuery(ctx, req)

	return retryOnStaleBinding(b, ctx, c, out, err, func(leader ctrl.Controller) (*servicepb.ExecutePreparedQueryResponse, error) {
		return leader.ExecutePreparedQuery(ctx, req)
	})
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
	c, _, err := b.readCtrl(ctx)
	if err != nil {
		return nil, err
	}

	out, err := c.InspectIndex(ctx, req)

	return retryOnStaleBinding(b, ctx, c, out, err, func(leader ctrl.Controller) (*servicepb.InspectIndexResponse, error) {
		return leader.InspectIndex(ctx, req)
	})
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
