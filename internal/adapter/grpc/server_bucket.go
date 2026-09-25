package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

var bucketTracer = otel.Tracer("grpc.bucket")

const (
	metadataKeyApplyReplayed      = "ledger-apply-replayed"
	metadataKeyQueryProfile       = "x-query-profile"
	metadataKeyQueryProfileResult = "x-query-profile-result-bin"
)

type BucketServiceServerImpl struct {
	servicepb.UnimplementedBucketServiceServer

	logger                logging.Logger
	ctrl                  ctrl.Controller
	localCtrl             *ctrl.DefaultController
	store                 *dal.Store
	readStore             *readstore.Store
	attrs                 *attributes.Attributes
	sharedState           *state.SharedState
	responseSigner        *signing.ResponseSigner
	queryProfileThreshold time.Duration
	clusterID             string
	info                  version.Info
	applyDuration         metric.Int64Histogram
	forwarder             nodeForwarder
	checkpointStores      checkpointStoreCache
}

func NewBucketServiceServer(logger logging.Logger, c ctrl.Controller, localCtrl *ctrl.DefaultController, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, sharedState *state.SharedState, responseSigner *signing.ResponseSigner, queryProfileThreshold time.Duration, clusterID string, meterProvider metric.MeterProvider, n *node.Node, servicePool *transport.ConnectionPool, info version.Info) servicepb.BucketServiceServer {
	meter := meterProvider.Meter("grpc")
	applyDuration, _ := meter.Int64Histogram("grpc.apply.duration",
		metric.WithUnit("us"),
		metric.WithDescription("Total duration of the gRPC Apply handler (forwarded identity + ctrl.Apply + signing)"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 2000, 10000, 50000, 200000, 1000000,
		),
	)

	return &BucketServiceServerImpl{
		logger:                logger,
		ctrl:                  c,
		localCtrl:             localCtrl,
		store:                 s,
		readStore:             rs,
		attrs:                 attrs,
		sharedState:           sharedState,
		responseSigner:        responseSigner,
		queryProfileThreshold: queryProfileThreshold,
		clusterID:             clusterID,
		info:                  info,
		applyDuration:         applyDuration,
		forwarder:             nodeForwarder{node: n, servicePool: servicePool},
	}
}

func (impl *BucketServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	start := time.Now()

	ctx, err := impl.adoptForwardedSnapshotIfTrusted(ctx, req)
	if err != nil {
		return nil, err
	}

	batchSize, _ := ctx.Value(applyBatchSizeKey{}).(int)

	if impl.logger.Enabled(logging.TraceLevel) {
		impl.logger.Tracef("Apply request received with %d requests", batchSize)
	}

	result, err := impl.ctrl.Apply(ctx, req)
	if err != nil {
		return nil, err
	}

	logs := result.Logs
	if !result.Replayed {
		if err := impl.waitCreatedQueryCheckpoints(ctx, logs); err != nil {
			return nil, err
		}
	}

	// Preserve server-produced provenance across follower forwarding without
	// adding a client-controlled request flag or a public protobuf field.
	if ggrpc.ServerTransportStreamFromContext(ctx) != nil {
		if err := ggrpc.SetTrailer(ctx, metadata.Pairs(metadataKeyApplyReplayed, strconv.FormatBool(result.Replayed))); err != nil {
			return nil, fmt.Errorf("setting Apply execution provenance: %w", err)
		}
	}

	skipResponse := req.GetSkipResponse()

	if !skipResponse {
		// Sign response logs with server Ed25519 key
		if impl.responseSigner != nil {
			for _, log := range logs {
				log.ResponseSignature = impl.responseSigner.SignLog(log)
			}
		}
	}

	impl.applyDuration.Record(ctx, time.Since(start).Microseconds(),
		metric.WithAttributes(attribute.Int("batch_size", batchSize)))

	if skipResponse {
		for _, log := range logs {
			log.Payload = nil
			log.ResponseSignature = nil
		}
	}

	return &servicepb.ApplyResponse{Logs: logs}, nil
}

// waitCreatedQueryCheckpoints blocks until every query checkpoint created by
// this newly executed batch is materialized in the local read index or deleted.
// Replayed results never enter this wait: their logs describe historical state.
//
// A successful Apply proves the FSM applied the checkpoint order, not that THIS
// node — the one the client is talking to — can serve a read at the returned
// checkpoint_id. Concurrent deletion supersedes readiness without undoing the
// committed success. We wait on the local .ready marker and not on the index
// builder progress cursor, whose fast path was the EN-1460 root cause: the
// cursor is persisted in the batch that precedes the physical checkpoint
// creation, so it reaches the target sequence ~100-150ms before the directory
// exists. The checkpoint is materialized per-replica; reads routed to another
// node whose builder has not yet crossed the log get a typed, retryable
// Unavailable (ErrCheckpointNotReady) until that node materializes it inline.
//
// Admission rejects a batch whose checkpoint trigger is not the last order, so
// there is at most one per response. Scanning every log keeps the wait correct
// without depending on that invariant, and without indexing a fixed position.
//
// Called before response payload stripping: skip_response nils out the payload
// that carries the checkpoint id, so a later scan would find nothing to wait on.
func (impl *BucketServiceServerImpl) waitCreatedQueryCheckpoints(ctx context.Context, logs []*commonpb.Log) error {
	for _, log := range logs {
		cp := log.GetPayload().GetCreatedQueryCheckpoint()
		if cp == nil {
			continue
		}

		readIndexDir := impl.store.QueryCheckpointReadIndexDir(cp.GetCheckpointId())
		if err := impl.readStore.WaitForCheckpoint(ctx, readIndexDir, func() (bool, error) {
			return impl.queryCheckpointDeleted(cp.GetCheckpointId())
		}); err != nil {
			return fmt.Errorf("waiting for read index checkpoint: %w", err)
		}
	}

	return nil
}

// queryCheckpointDeleted distinguishes deletion from follower lag using one
// fresh snapshot. IDs below the next-ID counter have already been allocated;
// an absent row for such an ID proves that creation was superseded by deletion.
func (impl *BucketServiceServerImpl) queryCheckpointDeleted(id uint64) (deleted bool, err error) {
	handle, err := impl.store.NewReadHandle()
	if err != nil {
		return false, fmt.Errorf("opening checkpoint lifecycle snapshot: %w", err)
	}
	defer func() { err = errors.Join(err, handle.Close()) }()

	checkpoint, err := query.ReadQueryCheckpoint(handle, id)
	if err != nil || checkpoint != nil {
		return false, err
	}
	nextID, err := query.ReadNextQueryCheckpointID(handle)

	return nextID > id, err
}

// adoptForwardedSnapshotIfTrusted attaches the request's
// forwarded_caller_snapshot to the context when (and only when) the
// connection authenticated via the cluster-secret. This is the trust
// boundary that lets a follower forward a user's admission-time snapshot
// (principal plus effective authorization) to the leader for audit purposes without
// letting regular clients spoof it.
//
// A forwarded snapshot arriving on a non-cluster-internal connection means the
// cluster secret is unset or mismatched between peers. It is rejected: the
// write would otherwise commit an unattributed audit entry for an
// authenticated user, so failing loud forces the misconfiguration to surface.
func (impl *BucketServiceServerImpl) adoptForwardedSnapshotIfTrusted(ctx context.Context, req *servicepb.ApplyRequest) (context.Context, error) {
	fc := req.GetForwardedCallerSnapshot()
	if fc == nil {
		return ctx, nil
	}

	if !internalauth.IsClusterInternal(ctx) {
		impl.logger.Errorf("rejecting forwarded caller snapshot on a non-cluster-internal connection; " +
			"cluster secret is likely unset or mismatched between peers")

		return ctx, status.Error(codes.PermissionDenied,
			"forwarded caller snapshot on a non-cluster-internal connection")
	}

	return internalauth.WithForwardedSnapshot(ctx, fc), nil
}

func (impl *BucketServiceServerImpl) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	var (
		tx  *commonpb.Transaction
		err error
	)

	checkpoint := req.GetCheckpointId() > 0
	if checkpoint {
		mainStore, _, cleanup, closeErr := impl.openCheckpointStores(ctx, req.GetCheckpointId())
		if closeErr != nil {
			return nil, closeErr
		}

		defer cleanup()

		tx, err = impl.localCtrl.GetTransactionFrom(ctx, mainStore, req.GetLedger(), req.GetTransactionId())
		if err != nil {
			return nil, err
		}
	} else {
		tx, err = impl.ctrl.GetTransaction(ctx, req.GetLedger(), req.GetTransactionId())
		if err != nil {
			return nil, err
		}
	}

	return &servicepb.GetTransactionResponse{Transaction: tx}, nil
}

// openCheckpointStores opens the checkpoint's main store and read index in
// read-only mode. The caller must invoke cleanup after its last access: it
// drops this reader's hold, closing both stores if it was the last, and then
// releases its filesystem lease. The stores are shared with the checkpoint's
// other readers, so cleanup ignores every call after the first.
func (impl *BucketServiceServerImpl) openCheckpointStores(ctx context.Context, checkpointID uint64) (*dal.Store, *readstore.Store, func(), error) {
	release, acquired := impl.store.AcquireQueryCheckpoint(checkpointID)
	if !acquired {
		return nil, nil, nil, impl.resolveMissingMarker(ctx, checkpointID)
	}
	keepLease := false
	defer func() {
		if !keepLease {
			release()
		}
	}()

	exists, err := impl.queryCheckpointExists(checkpointID)
	if err != nil {
		return nil, nil, nil, err
	}
	if !exists {
		return nil, nil, nil, impl.resolveMissingMarker(ctx, checkpointID)
	}

	mainPath := impl.store.QueryCheckpointMainDir(checkpointID)
	readIndexPath := impl.store.QueryCheckpointReadIndexDir(checkpointID)

	// A checkpoint has two independently-materialized directories on each replica:
	// the main store (created by the applier during apply) and the read index
	// (created by the index builder when it crosses the CreatedQueryCheckpoint
	// log). Either can lag the other, and both lag the cluster on a follower.
	// Opening a not-yet-materialized directory would surface an opaque,
	// non-retryable Unknown (EN-1460).
	//
	// Both halves write their .ready marker atomically last, after the directory
	// it vouches for has been renamed into place, so the pair of markers is the
	// gate. Neither marker says anything about the other half; a missing one
	// means "not materialized here yet" and routes through
	// resolveMissingMarker, which returns a retryable ErrCheckpointNotReady for
	// a registered checkpoint (or NotFound after a barrier confirms it does not
	// exist).
	//
	// Openability is not a completeness gate, so the markers are checked before
	// the open rather than inferred from it: pebble writes the MANIFEST that
	// makes a directory openable BEFORE it copies the WAL files, so an unmarked
	// directory can open cleanly while missing every write still resident in the
	// source memtable.
	if !dal.CheckpointDirReady(readIndexPath) || !dal.CheckpointDirReady(mainPath) {
		return nil, nil, nil, impl.resolveMissingMarker(ctx, checkpointID)
	}

	// One open of the checkpoint's two directories is shared by its concurrent
	// readers; see checkpointStoreCache. The lease above stays per-reader so that
	// a reader arriving after a committed deletion is refused at acquisition.
	mainStore, readIdx, releaseStores, err := impl.checkpointStores.acquire(ctx, checkpointID, impl.logger, func() (*dal.Store, *readstore.Store, error) {
		return openCheckpointDirs(mainPath, readIndexPath, impl.logger)
	})
	if err != nil {
		return nil, nil, nil, err
	}

	keepLease = true
	cleanup := func() {
		releaseStores()
		release()
	}

	return mainStore, readIdx, cleanup, nil
}

// resolveMissingMarker classifies a checkpoint read whose local .ready marker is
// absent, returning the error to surface. It must never return a permanent
// NotFound for a checkpoint that exists cluster-wide but simply has not been
// applied/materialized on this replica yet.
//
// Checkpoint reads are served locally on whichever node receives the request and
// deliberately skip the live-read barrier (a checkpoint is a fixed snapshot).
// That means the local QueryCheckpointState registry can lag the cluster on a
// follower whose FSM has not yet applied the CreatedQueryCheckpoint entry — so
// "absent locally" alone does NOT prove the checkpoint does not exist.
//
// We therefore only conclude NotFound after a linearizable barrier
// (ReadIndexAndWait) confirms the local FSM has caught up to the cluster commit
// index and the checkpoint is still absent:
//   - registered locally (before or after the barrier)      -> ErrCheckpointNotReady (Unavailable): the read
//     index just is not materialized on this replica yet.
//   - node still syncing / no leader (barrier inconclusive)  -> ErrCheckpointNotReady (Unavailable): we cannot
//     prove absence, so stay retryable rather than lie NotFound.
//   - absent after a successful barrier                      -> NotFound (permanent): the id genuinely does not
//     exist cluster-wide.
func (impl *BucketServiceServerImpl) resolveMissingMarker(ctx context.Context, checkpointID uint64) error {
	exists, err := impl.queryCheckpointExists(checkpointID)
	if err != nil {
		return err
	}

	if exists {
		// Registered but not materialized on this replica yet — mirrors the
		// INDEX_BUILDING -> Unavailable pattern for metadata indexes.
		return &domain.ErrCheckpointNotReady{CheckpointID: checkpointID}
	}

	// Absent locally is inconclusive on a lagging follower: catch the local FSM
	// up to the cluster commit index before deciding. If the barrier cannot be
	// established (node syncing, no leader), stay retryable — never NotFound.
	if _, err := impl.forwarder.node.ReadIndexAndWait(ctx); err != nil {
		return &domain.ErrCheckpointNotReady{CheckpointID: checkpointID}
	}

	// The local FSM is now caught up to the cluster. Re-check: still absent means
	// the checkpoint genuinely does not exist cluster-wide.
	exists, err = impl.queryCheckpointExists(checkpointID)
	if err != nil {
		return err
	}

	if !exists {
		return commonpb.NewNotFoundError("query checkpoint %d not found", checkpointID)
	}

	// Applied between the two reads — registered now, materialization pending.
	return &domain.ErrCheckpointNotReady{CheckpointID: checkpointID}
}

// queryCheckpointExists reports whether a query checkpoint id is present in the
// Raft-replicated QueryCheckpointState registry as applied on this node.
func (impl *BucketServiceServerImpl) queryCheckpointExists(checkpointID uint64) (bool, error) {
	handle, err := impl.store.NewReadHandle()
	if err != nil {
		return false, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	cp, err := query.ReadQueryCheckpoint(handle, checkpointID)
	if err != nil {
		return false, fmt.Errorf("reading query checkpoint %d: %w", checkpointID, err)
	}

	return cp != nil, nil
}

// readController selects the controller to serve a read from. When
// checkpointID is non-zero it opens the query checkpoint's stores and returns
// a checkpoint-scoped local controller plus a cleanup that closes them; reads
// then reflect the checkpoint's point-in-time state. When zero it returns the
// live (routed) controller and a no-op cleanup. The caller must always defer
// the returned cleanup.
func (impl *BucketServiceServerImpl) readController(ctx context.Context, checkpointID uint64) (ctrl.Controller, func(), error) {
	if checkpointID == 0 {
		return impl.ctrl, func() {}, nil
	}

	mainStore, readIdx, cleanup, err := impl.openCheckpointStores(ctx, checkpointID)
	if err != nil {
		return nil, nil, err
	}

	return impl.localCtrl.WithStores(mainStore, readIdx), cleanup, nil
}

func (impl *BucketServiceServerImpl) ListTransactions(req *servicepb.ListTransactionsRequest, stream servicepb.BucketService_ListTransactionsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListTransactions",
		trace.WithAttributes(attribute.String("ledger", req.GetLedger())))
	defer span.End()

	ctx, profile := withTransportQueryProfile(ctx)
	defer impl.emitProfile(ctx, profile)

	if req.GetLedger() == "" {
		return domain.ErrLedgerNameRequired
	}

	opts := req.GetOptions()
	pageSize := ctrl.ClampPageSize(opts.GetPageSize())
	// Ask the controller for one extra item beyond pageSize so
	// sendPagedToStream can peek-ahead and only emit x-next-cursor when
	// another page actually exists.
	fetchSize := pageSizePlusOne(pageSize)

	afterTxID, err := parseUint64Cursor(opts.GetCursor())
	if err != nil {
		return err
	}

	if impl.logger.Enabled(logging.TraceLevel) {
		impl.logger.Tracef("ListTransactions request received for ledger %s (pageSize=%d, afterTxID=%d, hasFilter=%v, reverse=%v)",
			req.GetLedger(), pageSize, afterTxID, opts.GetFilter() != nil, opts.GetReverse())
	}

	var c cursor.Cursor[*commonpb.Transaction]

	if cpID := opts.GetRead().GetCheckpointId(); cpID > 0 {
		mainStore, readIdx, cleanup, openErr := impl.openCheckpointStores(ctx, cpID)
		if openErr != nil {
			return openErr
		}

		defer cleanup()

		profile.EnterExecute()
		c, err = impl.localCtrl.ListTransactionsFrom(ctx, mainStore, readIdx, req.GetLedger(), fetchSize, afterTxID, opts.GetFilter(), opts.GetReverse())
		profile.LeaveExecute()
	} else {
		profile.EnterExecute()
		c, err = impl.ctrl.ListTransactions(ctx, req.GetLedger(), fetchSize, afterTxID, opts.GetFilter(), opts.GetReverse())
		profile.LeaveExecute()
	}

	if err != nil {
		return fmt.Errorf("listing transactions: %w", err)
	}

	return sendPagedToStream(ctx, c, stream, "transaction", pageSize, txCursorOf)
}

// txCursorOf returns the opaque next-page cursor for a transaction (its id
// encoded as decimal).
func txCursorOf(tx *commonpb.Transaction) string {
	return strconv.FormatUint(tx.GetId(), 10)
}

// parseUint64Cursor decodes the opaque ListOptions.cursor as a uint64. Empty
// is the canonical "start at the head" marker.
func parseUint64Cursor(cursor string) (uint64, error) {
	if cursor == "" {
		return 0, nil
	}

	v, err := strconv.ParseUint(cursor, 10, 64)
	if err != nil {
		return 0, status.Errorf(codes.InvalidArgument, "invalid cursor %q: %v", cursor, err)
	}

	return v, nil
}

func (impl *BucketServiceServerImpl) ListLedgers(req *servicepb.ListLedgersRequest, stream servicepb.BucketService_ListLedgersServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListLedgers")
	defer span.End()

	opts := req.GetOptions()
	read := opts.GetRead()

	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true, CheckpointID: true}); err != nil {
		return err
	}

	listingCtrl, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return err
	}
	defer cleanup()

	c, err := listingCtrl.ListLedgers(ctx)
	if err != nil {
		return fmt.Errorf("listing ledgers: %w", err)
	}

	cursorKey := opts.GetCursor()
	reverse := opts.GetReverse()
	pageSize := ctrl.ClampPageSize(opts.GetPageSize())

	c, err = ApplyHandlerPagination(
		c,
		skipByStringKey(cursorKey, reverse, func(item *commonpb.LedgerInfo) string { return item.GetName() }),
		reverse,
	)
	if err != nil {
		return fmt.Errorf("paginating ledgers: %w", err)
	}

	return sendPagedToStream(ctx, c, stream, "ledger", pageSize, func(l *commonpb.LedgerInfo) string {
		return l.GetName()
	})
}

func (impl *BucketServiceServerImpl) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	ctx, span := bucketTracer.Start(ctx, "grpc.GetLedger")
	defer span.End()

	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	read := req.GetRead()

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetLedgerByName(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	c, cleanup, err := impl.readController(ctx, req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetAccount(ctx, req.GetLedger(), req.GetAddress(), ctrl.GetAccountOptions{
		CollapseColors: req.GetCollapseColors(),
	})
}

func (impl *BucketServiceServerImpl) ListAccounts(req *servicepb.ListAccountsRequest, stream servicepb.BucketService_ListAccountsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListAccounts",
		trace.WithAttributes(attribute.String("ledger", req.GetLedger())))
	defer span.End()

	// See ListTransactions: the profile adopts the pre-authentication clock
	// carried by the public server's interceptor chain.
	ctx, profile := withTransportQueryProfile(ctx)
	defer impl.emitProfile(ctx, profile)

	if req.GetLedger() == "" {
		return domain.ErrLedgerNameRequired
	}

	opts := req.GetOptions()
	read := opts.GetRead()
	pageSize := ctrl.ClampPageSize(opts.GetPageSize())

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return err
	}
	defer cleanup()

	if impl.logger.Enabled(logging.TraceLevel) {
		impl.logger.Tracef("ListAccounts request received for ledger %s (pageSize=%d, cursor=%q, hasFilter=%v, reverse=%v)",
			req.GetLedger(), pageSize, opts.GetCursor(), opts.GetFilter() != nil, opts.GetReverse())
	}

	profile.EnterExecute()
	cur, err := c.ListAccounts(ctx, req.GetLedger(), pageSizePlusOne(pageSize), opts.GetCursor(), opts.GetFilter(), opts.GetReverse())
	profile.LeaveExecute()

	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}

	return sendPagedToStream(ctx, cur, stream, "account", pageSize, accountCursorOf)
}

// accountCursorOf returns the opaque next-page cursor for an account (its
// address). Used as both ListAccounts cursorOf and exported for use by the
// Aggregate helper if it ever needs to paginate.
func accountCursorOf(a *commonpb.Account) string {
	return a.GetAddress()
}

func (impl *BucketServiceServerImpl) GetPrimaryMetrics(ctx context.Context, req *servicepb.GetPrimaryMetricsRequest) (*servicepb.GetPrimaryMetricsResponse, error) {
	if conn, err := impl.forwarder.resolve(req.GetNodeId()); err != nil {
		return nil, err
	} else if conn != nil {
		return servicepb.NewBucketServiceClient(conn).GetPrimaryMetrics(ctx, req)
	}

	// Get metrics from the Pebble store directly
	metrics, ok := impl.store.GetMetrics().(*servicepb.PebbleMetrics)
	if !ok {
		return &servicepb.GetPrimaryMetricsResponse{
			Available: false,
		}, nil
	}

	return &servicepb.GetPrimaryMetricsResponse{
		Available: true,
		Metrics:   metrics,
	}, nil
}

func (impl *BucketServiceServerImpl) GetSecondaryMetrics(ctx context.Context, req *servicepb.GetSecondaryMetricsRequest) (*servicepb.GetSecondaryMetricsResponse, error) {
	if conn, err := impl.forwarder.resolve(req.GetNodeId()); err != nil {
		return nil, err
	} else if conn != nil {
		return servicepb.NewBucketServiceClient(conn).GetSecondaryMetrics(ctx, req)
	}

	if impl.readStore == nil {
		return &servicepb.GetSecondaryMetricsResponse{
			Available: false,
		}, nil
	}

	return &servicepb.GetSecondaryMetricsResponse{
		Available: true,
		Metrics:   impl.readStore.GetMetrics(),
	}, nil
}

func (impl *BucketServiceServerImpl) GetIndexStatus(ctx context.Context, req *servicepb.GetIndexStatusRequest) (*servicepb.GetIndexStatusResponse, error) {
	return impl.ctrl.GetIndexStatus(ctx, req)
}

// GetIndex returns a single Index registry entry. Scope aligns with
// ListIndexes SCOPE_LEDGER (a per-ledger read tokens must be accepted).
func (impl *BucketServiceServerImpl) GetIndex(ctx context.Context, req *servicepb.GetIndexRequest) (*commonpb.Index, error) {
	return impl.ctrl.GetIndex(ctx, req)
}

// GetIndexEntryStatus returns the per-replica status view for a single
// index. Same auth model as GetIndex.
func (impl *BucketServiceServerImpl) GetIndexEntryStatus(ctx context.Context, req *servicepb.GetIndexEntryStatusRequest) (*servicepb.IndexEntry, error) {
	return impl.ctrl.GetIndexEntryStatus(ctx, req)
}

// ListIndexes streams the bucket-scoped index registry, optionally filtered
// to a ledger (or bucket-scoped entries only) via the request Scope field.
// The filtering and orphan-entry skipping are implemented by
// DefaultController.ListIndexes; the interceptor authorizes the first request
// message before this handler pumps the cursor onto the stream.
func (impl *BucketServiceServerImpl) ListIndexes(req *servicepb.ListIndexesRequest, stream servicepb.BucketService_ListIndexesServer) error {
	ctx := stream.Context()

	c, err := impl.ctrl.ListIndexes(ctx, req)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrLedgerNameRequired):
			return status.Error(codes.InvalidArgument, "scope SCOPE_LEDGER requires a non-empty ledger name")
		default:
			return err
		}
	}
	defer func() { _ = c.Close() }()

	for {
		idx, err := c.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if err := stream.Send(idx); err != nil {
			return err
		}
	}
}

func (impl *BucketServiceServerImpl) CheckStore(_ *servicepb.CheckStoreRequest, stream servicepb.BucketService_CheckStoreServer) error {
	checker := check.NewChecker(impl.store, impl.attrs, impl.clusterID, impl.readStore, impl.logger)

	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		_ = stream.Send(event)
	})
}

func (impl *BucketServiceServerImpl) GetAuditEntry(ctx context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
	return impl.ctrl.GetAuditEntry(ctx, req.GetSequence())
}

func (impl *BucketServiceServerImpl) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListAuditEntries")
	defer span.End()

	opts := req.GetOptions()

	// Audit listing is fully under the shared contract: filter (bare audit fields
	// conditions), reverse, and checkpoint_id are all honored (EN-1241).
	if err := ValidateListOptions(opts, ListOptionsSupport{Filter: true, Reverse: true, CheckpointID: true}); err != nil {
		return err
	}

	afterSeq, err := parseUint64Cursor(opts.GetCursor())
	if err != nil {
		return err
	}

	pageSize := ctrl.ClampPageSize(opts.GetPageSize())
	fetchSize := pageSizePlusOne(pageSize)

	var c cursor.Cursor[*auditpb.AuditEntry]

	if cpID := opts.GetRead().GetCheckpointId(); cpID > 0 {
		mainStore, readIdx, cleanup, openErr := impl.openCheckpointStores(ctx, cpID)
		if openErr != nil {
			return openErr
		}

		defer cleanup()

		// Checkpoint publication certifies both the normal and audit projections
		// at the checkpoint's fixed Raft horizon before writing .ready. The audit
		// projection may already be ahead of the frozen main store, so the local
		// controller also trims compiled candidates to the checkpoint's audit
		// sequence.
		c, err = impl.localCtrl.ListAuditEntriesFrom(ctx, mainStore, readIdx, fetchSize, afterSeq, opts.GetFilter(), opts.GetReverse())
	} else {
		c, err = impl.ctrl.ListAuditEntries(ctx, fetchSize, afterSeq, opts.GetFilter(), opts.GetReverse())
	}

	if err != nil {
		return fmt.Errorf("listing audit entries: %w", err)
	}

	return sendPagedToStream(ctx, c, stream, "audit entry", pageSize, func(e *auditpb.AuditEntry) string {
		return strconv.FormatUint(e.GetSequence(), 10)
	})
}

func (impl *BucketServiceServerImpl) GetLog(ctx context.Context, req *servicepb.GetLogRequest) (*commonpb.Log, error) {
	c, cleanup, err := impl.readController(ctx, req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetLog(ctx, req.GetSequence())
}

func (impl *BucketServiceServerImpl) ListLogs(req *servicepb.ListLogsRequest, stream servicepb.BucketService_ListLogsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListLogs")
	defer span.End()

	opts := req.GetOptions()
	read := opts.GetRead()

	// ListLogs honors filter and checkpoint_id; reverse iteration over the
	// log zone still needs PaginateBackward — see follow-up tracked in the
	// PR description.
	if err := ValidateListOptions(opts, ListOptionsSupport{Filter: true, CheckpointID: true}); err != nil {
		return err
	}

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return err
	}
	defer cleanup()

	if req.GetLedger() == "" {
		return domain.ErrLedgerNameRequired
	}

	afterSequence, err := parseUint64Cursor(opts.GetCursor())
	if err != nil {
		return err
	}

	pageSize := ctrl.ClampPageSize(opts.GetPageSize())

	cur, err := c.ListLogs(ctx, req.GetLedger(), afterSequence, pageSizePlusOne(pageSize), opts.GetFilter())
	if err != nil {
		return fmt.Errorf("listing logs: %w", err)
	}

	// The cursor MUST be the ledger-local LedgerLog.Id — DefaultController.ListLogs
	// compiles afterSequence into a `LogId > afterSequence` filter against the
	// ledger-local id. Emitting the global raft sequence (Log.Sequence) would
	// skip valid ledger logs on the next page as soon as the two diverge
	// (after ledger creation or with >1 ledger).
	//
	// Defensive: if a non-apply payload ever reaches this path (today the
	// ListLogs filter only yields Apply logs, but the proto leaves room for
	// future payload kinds), GetApply() returns nil and Id defaults to 0 —
	// which would publish a bogus `x-next-cursor: "0"` and trap the client
	// in an infinite resume loop. Return an empty cursor in that case so the
	// stream signals "no more pages" instead.
	return sendPagedToStream(ctx, cur, stream, "log", pageSize, func(l *commonpb.Log) string {
		apply := l.GetPayload().GetApply()
		if apply == nil {
			return ""
		}

		return strconv.FormatUint(apply.GetLog().GetId(), 10)
	})
}

func (impl *BucketServiceServerImpl) GetEventsSinks(ctx context.Context, _ *servicepb.GetEventsSinksRequest) (*servicepb.GetEventsSinksResponse, error) {
	// Sink configs + per-sink status enrichment both live on the controller now,
	// so gRPC and HTTP return identical data from one snapshot (EN-1472).
	sinks, statuses, err := impl.ctrl.GetEventsSinks(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading events sinks: %w", err)
	}

	return &servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	}, nil
}

func (impl *BucketServiceServerImpl) ListSigningKeys(req *servicepb.ListSigningKeysRequest, stream servicepb.BucketService_ListSigningKeysServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListSigningKeys")
	defer span.End()

	opts := req.GetOptions()

	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true}); err != nil {
		return err
	}

	raw, err := impl.ctrl.ListSigningKeys(ctx)
	if err != nil {
		return fmt.Errorf("listing signing keys: %w", err)
	}

	// ReadSigningKeysCursor ranges over a Go map (random order). Sort by KeyId
	// before applying the opaque-cursor pagination so resume tokens stay
	// stable across requests — otherwise pages skip or duplicate keys.
	keys, err := cursor.Collect(raw)
	if err != nil {
		return fmt.Errorf("collecting signing keys: %w", err)
	}

	sort.Slice(keys, func(i, j int) bool { return keys[i].GetKeyId() < keys[j].GetKeyId() })

	cursorKey := opts.GetCursor()
	reverse := opts.GetReverse()
	pageSize := ctrl.ClampPageSize(opts.GetPageSize())

	c, err := ApplyHandlerPagination(
		cursor.NewSliceCursor(keys),
		skipByStringKey(cursorKey, reverse, func(item *commonpb.SigningKey) string { return item.GetKeyId() }),
		reverse,
	)
	if err != nil {
		return fmt.Errorf("paginating signing keys: %w", err)
	}

	return sendPagedToStream(ctx, c, stream, "signing key", pageSize, func(k *commonpb.SigningKey) string {
		return k.GetKeyId()
	})
}

func (impl *BucketServiceServerImpl) GetMetadataSchemaStatus(ctx context.Context, req *servicepb.GetMetadataSchemaStatusRequest) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	return impl.ctrl.GetMetadataSchemaStatus(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) AnalyzeAccounts(req *servicepb.AnalyzeAccountsRequest, stream servicepb.BucketService_AnalyzeAccountsServer) error {
	if req.GetLedger() == "" {
		return domain.ErrLedgerNameRequired
	}

	onProgress := func(processed, total uint64) {
		_ = stream.Send(&servicepb.AnalyzeAccountsEvent{
			Type: &servicepb.AnalyzeAccountsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{
					Processed: processed,
					Total:     total,
					Phase:     "scanning",
				},
			},
		})
	}

	resp, err := impl.ctrl.AnalyzeAccounts(stream.Context(), req.GetLedger(), req.GetVariableThreshold(), onProgress)
	if err != nil {
		return err
	}

	return stream.Send(&servicepb.AnalyzeAccountsEvent{
		Type: &servicepb.AnalyzeAccountsEvent_Result{Result: resp},
	})
}

func (impl *BucketServiceServerImpl) AnalyzeTransactions(req *servicepb.AnalyzeTransactionsRequest, stream servicepb.BucketService_AnalyzeTransactionsServer) error {
	if req.GetLedger() == "" {
		return domain.ErrLedgerNameRequired
	}

	onProgress := func(processed, total uint64) {
		_ = stream.Send(&servicepb.AnalyzeTransactionsEvent{
			Type: &servicepb.AnalyzeTransactionsEvent_Progress{
				Progress: &servicepb.AnalyzeProgress{
					Processed: processed,
					Total:     total,
				},
			},
		})
	}

	resp, err := impl.ctrl.AnalyzeTransactions(stream.Context(), req.GetLedger(), req.GetVariableThreshold(), onProgress)
	if err != nil {
		return err
	}

	return stream.Send(&servicepb.AnalyzeTransactionsEvent{
		Type: &servicepb.AnalyzeTransactionsEvent_Result{Result: resp},
	})
}

func (impl *BucketServiceServerImpl) ListPreparedQueries(ctx context.Context, req *servicepb.ListPreparedQueriesRequest) (*servicepb.ListPreparedQueriesResponse, error) {
	queries, err := impl.ctrl.ListPreparedQueries(ctx, req.GetLedger())
	if err != nil {
		return nil, err
	}

	return &servicepb.ListPreparedQueriesResponse{Queries: queries}, nil
}

func (impl *BucketServiceServerImpl) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	ctx, profile := withTransportQueryProfile(ctx)
	defer impl.emitProfile(ctx, profile)

	profile.EnterExecute()
	resp, err := impl.ctrl.ExecutePreparedQuery(ctx, req)
	// No delivery phase on a unary reply: the gRPC codec marshals it after the
	// handler returns, past the point where the trailer is set.
	profile.LeaveExecute()

	return resp, err
}

func (impl *BucketServiceServerImpl) GetLedgerStats(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	c, cleanup, err := impl.readController(ctx, req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetLedgerStats(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) AggregateVolumes(ctx context.Context, req *servicepb.AggregateVolumesRequest) (*commonpb.AggregateResult, error) {
	ctx, profile := withTransportQueryProfile(ctx)
	defer impl.emitProfile(ctx, profile)

	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	c, cleanup, err := impl.readController(ctx, req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	profile.EnterExecute()
	result, err := c.AggregateVolumes(ctx, req.GetLedger(), req.GetFilter(), query.AggregateOptions{
		UseMaxPrecision: req.GetUseMaxPrecision(),
		GroupByPrefixes: req.GetGroupByPrefixes(),
		CollapseColors:  req.GetCollapseColors(),
	})
	profile.LeaveExecute()

	return result, err
}

func (impl *BucketServiceServerImpl) GetNumscript(ctx context.Context, req *servicepb.GetNumscriptRequest) (*commonpb.NumscriptInfo, error) {
	read := req.GetRead()

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetNumscript(ctx, req.GetLedger(), req.GetName(), req.GetVersion())
}

// GetTemplateUsage returns the invocation counter + last-used timestamp for
// a Numscript template. Reads are served from the usagebuilder side-store,
// which is eventually consistent with the FSM. The usage projection is outside
// EN-1946's certified projection horizon, so this endpoint does not wait for a
// read-index or audit-index certificate.
func (impl *BucketServiceServerImpl) GetTemplateUsage(ctx context.Context, req *servicepb.GetTemplateUsageRequest) (*commonpb.TemplateUsage, error) {
	c, cleanup, err := impl.readController(ctx, 0)
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetTemplateUsage(ctx, req.GetLedger(), req.GetName())
}

func (impl *BucketServiceServerImpl) ListNumscripts(req *servicepb.ListNumscriptsRequest, stream servicepb.BucketService_ListNumscriptsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListNumscripts")
	defer span.End()

	opts := req.GetOptions()
	read := opts.GetRead()

	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true, CheckpointID: true}); err != nil {
		return err
	}

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return err
	}
	defer cleanup()

	scripts, err := c.ListNumscripts(ctx, req.GetLedger())
	if err != nil {
		return fmt.Errorf("listing numscripts: %w", err)
	}

	// Sort by name so the opaque cursor stays stable across requests; the
	// underlying store iteration order is not guaranteed.
	sort.Slice(scripts, func(i, j int) bool { return scripts[i].GetName() < scripts[j].GetName() })

	pageSize := ctrl.ClampPageSize(opts.GetPageSize())

	paginated, err := ApplyHandlerPagination(
		cursor.NewSliceCursor(scripts),
		skipByStringKey(opts.GetCursor(), opts.GetReverse(), func(item *commonpb.NumscriptInfo) string { return item.GetName() }),
		opts.GetReverse(),
	)
	if err != nil {
		return fmt.Errorf("paginating numscripts: %w", err)
	}

	return sendPagedToStream(ctx, paginated, stream, "numscript", pageSize, func(n *commonpb.NumscriptInfo) string {
		return n.GetName()
	})
}

func (impl *BucketServiceServerImpl) ListNumscriptVersions(ctx context.Context, req *servicepb.ListNumscriptVersionsRequest) (*servicepb.ListNumscriptVersionsResponse, error) {
	read := req.GetRead()

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	latest, versions, err := c.ListNumscriptVersions(ctx, req.GetLedger(), req.GetName())
	if err != nil {
		return nil, err
	}

	return &servicepb.ListNumscriptVersionsResponse{LatestVersion: latest, Versions: versions}, nil
}

func (impl *BucketServiceServerImpl) InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	if req.GetMetadataKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	c, cleanup, err := impl.readController(ctx, req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.InspectIndex(ctx, req)
}

func (impl *BucketServiceServerImpl) Barrier(ctx context.Context, _ *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	// Barrier proposes a no-op through Raft and waits for it to apply, so it
	// consumes consensus capacity like a write. Require an authenticated scope
	// (ledger:OpsRead) so it can't be used anonymously as a DoS amplifier or a
	// commit-index timing side channel. Leader-forwarded calls carry the
	// cluster secret, which grants all scopes.
	commitIndex, err := impl.ctrl.Barrier(ctx)
	if err != nil {
		return nil, err
	}

	return &servicepb.BarrierResponse{CommitIndex: commitIndex}, nil
}

func (impl *BucketServiceServerImpl) Discovery(_ context.Context, _ *servicepb.DiscoveryRequest) (*servicepb.DiscoveryResponse, error) {
	resp := &servicepb.DiscoveryResponse{
		ServerInfo: &servicepb.ServerInfo{
			Version:         impl.info.Version,
			Commit:          impl.info.Commit,
			BuildDate:       impl.info.BuildDate,
			GoVersion:       impl.info.GoVersion,
			ProtocolVersion: grpcprotocol.Version,
		},
	}
	if impl.responseSigner != nil {
		resp.ResponseSigning = &servicepb.ResponseSigningInfo{
			PublicKey: impl.responseSigner.PublicKey(),
			KeyId:     impl.responseSigner.KeyID(),
		}
	}

	return resp, nil
}

// emitProfile closes the request clock and publishes the profile: to the
// slow-query log / OTel span when the request exceeded the configured
// threshold, and to the gRPC trailer when the caller asked for it.
//
// Deferred at the top of every profiled handler, so a read that fails partway is
// still logged — a slow failure is at least as interesting as a slow success, and
// the previous placement after the happy path meant an error return produced no
// profile at all.
//
// The threshold compares WallDuration, not the execution total and not
// ServerDuration:
//
//   - the execution total was the pre-EN-1859 behaviour and could not see request
//     decode or filter compilation, which is the gap this work exists to close;
//   - ServerDuration excludes the delivery phase, which on a stream holds row
//     serialisation AND, for a forwarded read, the entire remote cost. A
//     threshold blind to both would still miss the slow requests it exists for.
//
// WallDuration therefore trades in consumer back-pressure: a slow client can trip
// the threshold. The logged breakdown says which side was slow, and a threshold
// that fires with an explanation beats one that never fires.
//
// A threshold of 0 disables the log entirely — every duration is >= 0, so
// comparing against 0 would otherwise log every single read.
func (impl *BucketServiceServerImpl) emitProfile(ctx context.Context, profile *query.QueryProfile) {
	emitQueryProfile(ctx, profile, impl.logger, impl.queryProfileThreshold)
}

func emitQueryProfile(ctx context.Context, profile *query.QueryProfile, logger logging.Logger, slowThreshold time.Duration) {
	if profile == nil {
		return
	}

	profile.Finish()

	if slowThreshold > 0 && profile.WallDuration() >= slowThreshold {
		profile.LogTo(logger)
		profile.EmitToSpan(trace.SpanFromContext(ctx))
	}

	if wantsProfile(ctx) {
		_ = ggrpc.SetTrailer(ctx, profileToMetadata(profile))
	}
}

func withTransportQueryProfile(ctx context.Context) (context.Context, *query.QueryProfile) {
	if clock, ok := ctx.Value(queryProfileClockKey{}).(*queryProfileClock); ok {
		clock.claimed = true

		return query.WithProfileStartingAt(ctx, clock.start)
	}

	return query.WithProfile(ctx)
}

func wantsProfile(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)

	return ok && len(md.Get(metadataKeyQueryProfile)) > 0
}

func profileToMetadata(profile *query.QueryProfile) metadata.MD {
	pb := profile.ToProto()

	data, err := proto.Marshal(pb)
	if err != nil {
		return nil
	}

	return metadata.Pairs(metadataKeyQueryProfileResult, string(data))
}

func RegisterBucketService(registrar ggrpc.ServiceRegistrar, ledgerServiceServer servicepb.BucketServiceServer) {
	servicepb.RegisterBucketServiceServer(registrar, ledgerServiceServer)
}
