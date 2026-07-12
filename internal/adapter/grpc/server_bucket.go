package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/receipt"
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
)

var bucketTracer = otel.Tracer("grpc.bucket")

const (
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
	receiptSigner         *receipt.Signer
	responseSigner        *signing.ResponseSigner
	authCfg               internalauth.AuthConfig
	queryProfileThreshold time.Duration
	clusterID             string
	idempotencyTTL        time.Duration
	info                  version.Info
	applyDuration         metric.Int64Histogram
	forwarder             nodeForwarder
}

func NewBucketServiceServer(logger logging.Logger, c ctrl.Controller, localCtrl *ctrl.DefaultController, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, sharedState *state.SharedState, receiptSigner *receipt.Signer, responseSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig, queryProfileThreshold time.Duration, clusterID string, idempotencyTTL time.Duration, meterProvider metric.MeterProvider, n *node.Node, servicePool *transport.ConnectionPool, info version.Info) servicepb.BucketServiceServer {
	meter := meterProvider.Meter("grpc")
	applyDuration, _ := meter.Int64Histogram("grpc.apply.duration",
		metric.WithUnit("us"),
		metric.WithDescription("Total duration of the gRPC Apply handler (auth + ctrl.Apply + signing)"),
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
		receiptSigner:         receiptSigner,
		responseSigner:        responseSigner,
		authCfg:               authCfg,
		queryProfileThreshold: queryProfileThreshold,
		clusterID:             clusterID,
		idempotencyTTL:        idempotencyTTL,
		info:                  info,
		applyDuration:         applyDuration,
		forwarder:             nodeForwarder{node: n, servicePool: servicePool},
	}
}

func (impl *BucketServiceServerImpl) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	start := time.Now()

	// Authenticate the token and expand scopes, but don't check a specific scope yet.
	ctx, err := internalauth.Authenticate(ctx, impl.authCfg)
	if err != nil {
		return nil, err
	}

	ctx, err = impl.adoptForwardedSnapshotIfTrusted(ctx, req)
	if err != nil {
		return nil, err
	}

	// Peek the batch (non-authoritative) for the empty check and the per-request
	// scope checks. Admission re-verifies the signature and unmarshals the same
	// bytes for processing.
	//
	// A signed payload is opaque until admission verifies the signature over its
	// raw bytes, so a tampered payload that fails to parse here must NOT be
	// rejected with InvalidArgument before the signature is checked — admission
	// is authoritative and rejects a bad signature with PermissionDenied. Skip
	// the peek-based checks when a signed batch won't parse; an unsigned batch
	// that won't parse is simply malformed.
	batch, peekErr := servicepb.PeekBatch(req)
	if peekErr != nil && req.GetSigned() == nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", peekErr)
	}

	if peekErr == nil {
		if len(batch.GetRequests()) == 0 {
			return nil, errEnvelopesRequired
		}

		// Per-request scope check: each request in the batch may require a
		// different granular scope. Peeking is non-authoritative; admission
		// re-verifies the batch signature and unmarshals the same bytes for
		// processing. Cluster-internal forwards (cluster-secret authenticated
		// peers) get the full scope set so this loop is a no-op by design.
		if impl.authCfg.Enabled {
			effective := internalauth.ExpandedScopesFromContext(ctx)
			authPresented := internalauth.AuthPresentedFromContext(ctx)

			for i, peeked := range batch.GetRequests() {
				required := internalauth.RequiredScopeForRequest(peeked)
				if internalauth.HasScope(effective, required) {
					continue
				}

				// 401 when no credentials were presented (the anonymous fallback
				// covers reads only); 403 when a valid token was presented but
				// lacks the required scope.
				if !authPresented {
					return nil, status.Errorf(codes.Unauthenticated,
						"request %d requires scope %s", i, required)
				}

				return nil, status.Errorf(codes.PermissionDenied,
					"request %d requires scope %s", i, required)
			}
		}
	}

	if impl.logger.Enabled(logging.TraceLevel) {
		impl.logger.Tracef("Apply request received with %d requests", len(batch.GetRequests()))
	}

	logs, err := impl.ctrl.Apply(ctx, req)
	if err != nil {
		return nil, err
	}

	skipResponse := req.GetSkipResponse()

	if !skipResponse {
		// Sign receipts for created transactions (outside FSM to avoid Raft nondeterminism)
		if impl.receiptSigner != nil {
			for _, log := range logs {
				impl.signReceiptIfNeeded(log)
			}
		}

		// Sign response logs with server Ed25519 key (after receipt signing, since receipt is cleared before signing)
		if impl.responseSigner != nil {
			for _, log := range logs {
				log.ResponseSignature = impl.responseSigner.SignLog(log)
			}
		}
	}

	impl.applyDuration.Record(ctx, time.Since(start).Microseconds(),
		metric.WithAttributes(attribute.Int("batch_size", len(batch.GetRequests()))))

	if skipResponse {
		for _, log := range logs {
			log.Payload = nil
			log.Receipt = ""
			log.ResponseSignature = nil
		}
	}

	return &servicepb.ApplyResponse{Logs: logs}, nil
}

// adoptForwardedSnapshotIfTrusted attaches the request's
// forwarded_caller_snapshot to the context when (and only when) the
// connection authenticated via the cluster-secret. This is the trust
// boundary that lets a follower forward a user's admission-time snapshot
// (identity + scopes + god) to the leader for audit purposes without
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

// signReceiptIfNeeded signs a JWT receipt for logs containing created transactions.
func (impl *BucketServiceServerImpl) signReceiptIfNeeded(log *commonpb.Log) {
	applyLog := log.GetPayload().GetApply()
	if applyLog == nil || applyLog.GetLog() == nil {
		return
	}

	created := applyLog.GetLog().GetData().GetCreatedTransaction()
	if created == nil || created.GetTransaction() == nil {
		return
	}

	tx := created.GetTransaction()

	receiptToken, err := impl.receiptSigner.Sign(
		applyLog.GetLedgerName(),
		tx.GetId(),
		tx.GetPostings(),
		tx.GetTimestamp(),
		created.GetChapterId(),
	)
	if err != nil {
		impl.logger.Errorf("Failed to sign receipt for tx %d: %v", tx.GetId(), err)

		return
	}

	log.Receipt = receiptToken
}

func (impl *BucketServiceServerImpl) ListChapters(req *servicepb.ListChaptersRequest, stream servicepb.BucketService_ListChaptersServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListChapters")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	opts := req.GetOptions()

	// Chapters are raft-state-backed; filter and checkpoint_id are not
	// applicable. Reverse is honored at the handler layer (drain + reverse).
	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true}); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(ctx, opts.GetRead().GetMinLogSequence()); err != nil {
		return err
	}

	c, err := impl.ctrl.ListChapters(ctx)
	if err != nil {
		return fmt.Errorf("listing chapters: %w", err)
	}

	cursorKey, err := parseUint64Cursor(opts.GetCursor())
	if err != nil {
		return err
	}

	pageSize := ctrl.ClampPageSize(opts.GetPageSize())
	reverse := opts.GetReverse()

	c, err = ApplyHandlerPagination(
		c,
		skipByUint64Key(cursorKey, reverse, func(item *commonpb.Chapter) uint64 { return item.GetId() }),
		reverse,
	)
	if err != nil {
		return fmt.Errorf("paginating chapters: %w", err)
	}

	return sendPagedToStream(ctx, c, stream, "chapter", pageSize, func(p *commonpb.Chapter) string {
		return strconv.FormatUint(p.GetId(), 10)
	})
}

func (impl *BucketServiceServerImpl) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	var (
		tx *commonpb.Transaction
		// reader is the store a locally-derived receipt is read from: the
		// checkpoint's fixed store, or (live path) a snapshot opened AFTER the
		// transaction read so it can't predate the creation log. fwdReceipt is
		// non-nil only when the read was forwarded to a signing node, which already
		// produced the (authoritative, possibly empty) receipt — then we use it
		// as-is rather than re-deriving from a possibly-stale local snapshot.
		reader     dal.PebbleGetter
		fwdReceipt *string
		err        error
	)

	checkpoint := req.GetCheckpointId() > 0
	if checkpoint {
		mainStore, readIdx, closeErr := impl.openCheckpointStores(ctx, req.GetCheckpointId())
		if closeErr != nil {
			return nil, closeErr
		}

		defer func() {
			_ = readIdx.Close()
			_ = mainStore.Close()
		}()

		reader = mainStore
		tx, err = impl.localCtrl.GetTransactionFrom(ctx, mainStore, req.GetLedger(), req.GetTransactionId())
	} else {
		tx, fwdReceipt, err = impl.ctrl.GetTransaction(ctx, req.GetLedger(), req.GetTransactionId())
	}

	if err != nil {
		return nil, err
	}

	resp := &servicepb.GetTransactionResponse{Transaction: tx}
	switch {
	case fwdReceipt != nil:
		// Forwarded read: relay the receipt the serving node signed (possibly
		// empty, e.g. a reversal). It is already an authoritative token, so this
		// node passes it through whether or not it can sign — re-deriving would
		// read a possibly-stale local snapshot.
		resp.Receipt = *fwdReceipt
	case impl.receiptSigner != nil:
		// Locally-served read: sign from a snapshot opened now — after the
		// transaction read barrier — so the receipt's ledger + creation-log reads
		// share one committed state at least as fresh as the transaction.
		if !checkpoint {
			handle, hErr := impl.store.NewReadHandle()
			if hErr != nil {
				return nil, fmt.Errorf("creating read handle: %w", hErr)
			}

			defer func() { _ = handle.Close() }()

			reader = handle
		}

		receiptToken, err := impl.computeTransactionReceipt(ctx, reader, req.GetLedger(), req.GetTransactionId(), tx)
		if err != nil {
			return nil, fmt.Errorf("computing transaction receipt: %w", err)
		}

		resp.Receipt = receiptToken
	}

	return resp, nil
}

// computeTransactionReceipt computes a JWT receipt for an existing transaction
// by looking up its creation log to extract the chapter ID. Ledger info and the
// creation log are read from the supplied reader — the same store the
// transaction was read from — so checkpoint reads stay self-consistent.
func (impl *BucketServiceServerImpl) computeTransactionReceipt(ctx context.Context, reader dal.PebbleGetter, ledger string, txID uint64, tx *commonpb.Transaction) (string, error) {
	ledgerInfo, err := query.GetLedgerByName(ctx, reader, ledger)
	if err != nil {
		return "", err
	}

	log, err := query.FindTransactionCreationLog(ctx, reader, impl.attrs.Transaction, ledgerInfo.GetName(), txID)
	if errors.Is(err, domain.ErrNotFound) {
		// No creation log for this transaction in this store (e.g. its log was
		// archived/purged). The transaction is still readable; it just has no
		// receipt. Not an error.
		return "", nil
	}

	if err != nil {
		return "", err
	}

	// Receipts are only issued for created transactions, matching the Apply path
	// (signReceiptIfNeeded skips non-created logs). A reversal transaction's
	// creation log is a RevertedTransaction log, so it legitimately has no
	// receipt — return empty rather than erroring.
	created := log.GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()
	if created == nil {
		return "", nil
	}

	return impl.receiptSigner.Sign(ledger, txID, tx.GetPostings(), tx.GetTimestamp(), created.GetChapterId())
}

// openCheckpointStores opens the checkpoint's main store and read index in read-only mode.
// The caller must close both stores when done.
func (impl *BucketServiceServerImpl) openCheckpointStores(ctx context.Context, checkpointID uint64) (*dal.Store, *readstore.Store, error) {
	mainPath := impl.store.QueryCheckpointMainDir(checkpointID)
	readIndexPath := impl.store.QueryCheckpointReadIndexDir(checkpointID)

	// A checkpoint has two independently-materialized directories on each replica:
	// the main store (created by the applier during apply) and the read index
	// (created by the index builder when it crosses the CreatedQueryCheckpoint
	// log). Either can lag the other, and both lag the cluster on a follower.
	// Opening a not-yet-materialized directory would surface an opaque,
	// non-retryable Unknown (EN-1460).
	//
	// The read-index .ready marker is written atomically last by the builder, so
	// it is a reliable readiness gate for the read index. The main store has no
	// such marker (the applier checkpoints straight into {id}/main), so we gate it
	// by attempting the read-only open: any failure on a checkpoint whose read
	// index is ready is treated as "main store not materialized here yet" and
	// routed through resolveMissingMarker, which returns a retryable
	// ErrCheckpointNotReady for a registered checkpoint (or NotFound after a
	// barrier confirms it does not exist).
	if !readstore.CheckpointDirReady(readIndexPath) {
		return nil, nil, impl.resolveMissingMarker(ctx, checkpointID)
	}

	mainStore, err := dal.OpenReadOnly(mainPath, impl.logger)
	if err != nil {
		// The read index is ready but the main store is not openable yet — most
		// likely the applier's main checkpoint has not landed on this replica.
		// Never surface a raw Unknown: classify as not-ready / not-found.
		return nil, nil, impl.resolveMissingMarker(ctx, checkpointID)
	}

	readIdx, err := readstore.OpenReadOnly(readIndexPath, impl.logger)
	if err != nil {
		_ = mainStore.Close()

		return nil, nil, fmt.Errorf("opening checkpoint read index: %w", err)
	}

	return mainStore, readIdx, nil
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

	mainStore, readIdx, err := impl.openCheckpointStores(ctx, checkpointID)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		_ = readIdx.Close()
		_ = mainStore.Close()
	}

	return impl.localCtrl.WithStores(mainStore, readIdx), cleanup, nil
}

// waitMinLogSequence blocks until the Pebble read index has processed at
// least the requested minimum log sequence, or the context is cancelled.
func (impl *BucketServiceServerImpl) waitMinLogSequence(ctx context.Context, minLogSequence uint64) error {
	if minLogSequence == 0 {
		return nil
	}

	return impl.readStore.WaitForSequence(ctx, minLogSequence)
}

// waitFilteredAuditConsistency gives a live, filtered ListAuditEntries read a
// read-your-writes guarantee against the async audit secondary index.
//
// min_log_sequence is a LOG sequence, but a filtered audit read resolves through
// the audit index whose cursor (ReadAuditProgress) is an AUDIT sequence. The two
// spaces diverge — the audit sequence advances on every proposal, including
// failures that emit no log — so waiting for audit_progress >= min_log_sequence
// would be incorrect. Instead, conservatively:
//
//  1. wait for the log index to reach min_log_sequence (waitMinLogSequence);
//  2. read the current live audit head from the main store;
//  3. wait for the audit index cursor to reach that observed head.
//
// This can over-wait (the head observed in step 2 may include audit entries
// produced after min_log_sequence), which is acceptable for v3.0 and avoids a
// new min_audit_sequence API. A minLogSeq of 0 means "no consistency bound
// requested", so this is a no-op.
//
// Only the LIVE filtered path calls this. Unfiltered reads scan the Cold/Audit
// zone directly and are always current, so they keep the direct fast path.
// Checkpoint reads are frozen snapshots and are handled (and documented) at the
// checkpoint-creation boundary, out of scope here.
func (impl *BucketServiceServerImpl) waitFilteredAuditConsistency(ctx context.Context, minLogSeq uint64) error {
	if minLogSeq == 0 {
		return nil
	}

	if err := impl.waitMinLogSequence(ctx, minLogSeq); err != nil {
		return err
	}

	handle, err := impl.store.NewDirectReadHandle()
	if err != nil {
		return status.Errorf(codes.Unavailable, "opening read handle for audit head: %v", err)
	}

	auditHead, err := query.ReadLastAuditSequence(handle)
	// Close before waiting: the handle holds dbMu.RLock for its lifetime, and the
	// wait below can block for a while. We only need the head value, not the
	// handle, past this point.
	_ = handle.Close()

	if err != nil {
		return status.Errorf(codes.Internal, "reading live audit head: %v", err)
	}

	return impl.readStore.WaitForAuditSequence(ctx, auditHead)
}

func (impl *BucketServiceServerImpl) ListTransactions(req *servicepb.ListTransactionsRequest, stream servicepb.BucketService_ListTransactionsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListTransactions",
		trace.WithAttributes(attribute.String("ledger", req.GetLedger())))
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return err
	}

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

	profileCtx, profile := query.WithProfile(ctx)

	var c cursor.Cursor[*commonpb.Transaction]

	if cpID := opts.GetRead().GetCheckpointId(); cpID > 0 {
		mainStore, readIdx, openErr := impl.openCheckpointStores(ctx, cpID)
		if openErr != nil {
			return openErr
		}

		defer func() {
			_ = readIdx.Close()
			_ = mainStore.Close()
		}()

		c, err = impl.localCtrl.ListTransactionsFrom(profileCtx, mainStore, readIdx, req.GetLedger(), fetchSize, afterTxID, opts.GetFilter(), opts.GetReverse())
	} else {
		if waitErr := impl.waitMinLogSequence(ctx, opts.GetRead().GetMinLogSequence()); waitErr != nil {
			return waitErr
		}

		c, err = impl.ctrl.ListTransactions(profileCtx, req.GetLedger(), fetchSize, afterTxID, opts.GetFilter(), opts.GetReverse())
	}

	if err != nil {
		return fmt.Errorf("listing transactions: %w", err)
	}

	err = sendPagedToStream(ctx, c, stream, "transaction", pageSize, txCursorOf)
	impl.emitProfile(ctx, profile)

	return err
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

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return err
	}

	opts := req.GetOptions()
	read := opts.GetRead()

	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true, CheckpointID: true}); err != nil {
		return err
	}

	if read.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, read.GetMinLogSequence()); err != nil {
			return err
		}
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

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	read := req.GetRead()

	if read.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, read.GetMinLogSequence()); err != nil {
			return nil, err
		}
	}

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetLedgerByName(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) GetAccount(ctx context.Context, req *servicepb.GetAccountRequest) (*commonpb.Account, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

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

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return err
	}

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

	// minLogSequence only gates live reads; a checkpoint is a fixed snapshot.
	if read.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, read.GetMinLogSequence()); err != nil {
			return err
		}
	}

	if impl.logger.Enabled(logging.TraceLevel) {
		impl.logger.Tracef("ListAccounts request received for ledger %s (pageSize=%d, cursor=%q, hasFilter=%v, reverse=%v)",
			req.GetLedger(), pageSize, opts.GetCursor(), opts.GetFilter() != nil, opts.GetReverse())
	}

	profileCtx, profile := query.WithProfile(ctx)

	cur, err := c.ListAccounts(profileCtx, req.GetLedger(), pageSizePlusOne(pageSize), opts.GetCursor(), opts.GetFilter(), opts.GetReverse())
	if err != nil {
		return fmt.Errorf("listing accounts: %w", err)
	}

	err = sendPagedToStream(ctx, cur, stream, "account", pageSize, accountCursorOf)
	impl.emitProfile(ctx, profile)

	return err
}

// accountCursorOf returns the opaque next-page cursor for an account (its
// address). Used as both ListAccounts cursorOf and exported for use by the
// Aggregate helper if it ever needs to paginate.
func accountCursorOf(a *commonpb.Account) string {
	return a.GetAddress()
}

func (impl *BucketServiceServerImpl) GetPrimaryMetrics(ctx context.Context, req *servicepb.GetPrimaryMetricsRequest) (*servicepb.GetPrimaryMetricsResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	ledgerFilter := req.GetLedger()

	lastIndexed, err := impl.readStore.LastIndexedSequence()
	if err != nil {
		return nil, fmt.Errorf("reading last indexed sequence: %w", err)
	}

	handle, err := impl.store.NewDirectReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	lastLog, err := query.ReadLastSequence(handle)
	if err != nil {
		return nil, fmt.Errorf("reading last log sequence: %w", err)
	}

	var lag uint64
	if lastLog > lastIndexed {
		lag = lastLog - lastIndexed
	}

	var fileSize uint64
	if info, err := os.Stat(impl.readStore.Path()); err == nil {
		fileSize = uint64(info.Size())
	}

	// Read per-index backfill cursors keyed by (ledger name, IndexID canonical).
	backfillEntries, err := impl.readStore.ListBackfillProgress()
	if err != nil {
		return nil, fmt.Errorf("reading backfill progress: %w", err)
	}

	type cursorKey struct {
		ledger    string
		canonical string
	}

	cursors := make(map[cursorKey]uint64, len(backfillEntries))

	for _, e := range backfillEntries {
		id := indexIDFromBackfillEntry(e)
		if id == nil {
			continue
		}

		cursors[cursorKey{ledger: e.LedgerName, canonical: indexes.Canonical(id)}] = e.Cursor
	}

	// Build a name → id map for active ledgers so the streaming scan of the
	// index registry can attach the ledger ID needed for the backfill-cursor
	// lookup and skip orphans from tombstoned ledgers in one pass.
	ledgerCursor, err := query.ReadLedgers(ctx, handle)
	if err != nil {
		return nil, fmt.Errorf("reading ledgers: %w", err)
	}
	defer func() { _ = ledgerCursor.Close() }()

	ledgerNameToID := make(map[string]uint32)

	for {
		info, lErr := ledgerCursor.Next()
		if lErr != nil {
			if errors.Is(lErr, io.EOF) {
				break
			}

			return nil, fmt.Errorf("iterating ledgers: %w", lErr)
		}

		if info.GetDeletedAt() != nil {
			continue
		}

		ledgerNameToID[info.GetName()] = info.GetId()
	}

	idxIter, err := impl.attrs.Index.NewStreamingIter(handle, nil)
	if err != nil {
		return nil, fmt.Errorf("opening index registry iterator: %w", err)
	}
	defer func() { _ = idxIter.Close() }()

	var entries []*servicepb.IndexEntry

	for idxIter.Next() {
		idx := idxIter.Entry().Value
		if idx == nil || idx.GetId() == nil {
			continue
		}

		name := idx.GetLedger()

		if name != "" {
			if _, ok := ledgerNameToID[name]; !ok {
				continue // orphan or tombstoned ledger — skip
			}
		}

		if ledgerFilter != "" && name != ledgerFilter {
			continue
		}

		canonical := indexes.Canonical(idx.GetId())
		entry := &servicepb.IndexEntry{
			Ledger: name,
			Index:  idx,
			Cursor: cursors[cursorKey{ledger: name, canonical: canonical}],
		}

		// Per-replica forward-encoding state. (0, 0) is the default
		// zero value when the cache has no record — equivalent to
		// "not yet built on this replica" so clients reading
		// current_version == 0 keep the same semantics regardless of
		// whether the entry exists or not. A real Pebble I/O failure
		// surfaces as a logged warning + zero state — the status RPC
		// is informational, so degrading to "BUILDING-looking" beats
		// failing the whole response, but we log so operators can
		// correlate.
		state, ok, stateErr := impl.readStore.ReadIndexVersionState(name, canonical)
		if stateErr != nil {
			impl.logger.WithFields(map[string]any{
				"ledger":    name,
				"canonical": canonical,
				"error":     stateErr,
			}).Errorf("Reading IndexVersionState for GetIndexStatus")
		} else if ok {
			entry.CurrentVersion = state.CurrentVersion
			entry.PendingVersion = state.PendingVersion
		}

		entries = append(entries, entry)
	}

	if err := idxIter.Err(); err != nil {
		return nil, fmt.Errorf("iterating index registry: %w", err)
	}

	return &servicepb.GetIndexStatusResponse{
		LastIndexedSequence: lastIndexed,
		LastLogSequence:     lastLog,
		Lag:                 lag,
		IndexFileSize:       fileSize,
		Indexes:             entries,
	}, nil
}

// ListIndexes streams the bucket-scoped index registry, optionally filtered
// to a ledger (or bucket-scoped entries only) via the request Scope field.
// Filtering happens at the iteration layer: orphan entries belonging to
// deleted ledgers are skipped, but ALL entries pass to the client when the
// caller asks for SCOPE_ALL.
func (impl *BucketServiceServerImpl) ListIndexes(req *servicepb.ListIndexesRequest, stream servicepb.BucketService_ListIndexesServer) error {
	ctx := stream.Context()

	// Per-ledger callers previously read indexes through GetLedger under
	// ledger:LedgerRead, so we keep that scope for SCOPE_LEDGER to preserve
	// granular-auth tokens that don't carry ledger:OpsRead. SCOPE_BUCKET and
	// SCOPE_ALL surface cross-ledger / operator-grade visibility and stay
	// under ledger:OpsRead (PR #453 review).
	requiredScope := internalauth.ScopeOpsRead
	if req.GetScope() == servicepb.ListIndexesRequest_SCOPE_LEDGER {
		requiredScope = internalauth.ScopeLedgersRead
	}

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, requiredScope); err != nil {
		return err
	}

	if req.GetScope() == servicepb.ListIndexesRequest_SCOPE_LEDGER && req.GetLedger() == "" {
		return status.Error(codes.InvalidArgument, "scope SCOPE_LEDGER requires a non-empty ledger name")
	}

	handle, err := impl.store.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	// For SCOPE_LEDGER, probe ledger existence up front. A missing or
	// soft-deleted ledger surfaces as NotFound — callers migrating from the
	// previous LedgerInfo-embedded view (ledgerctl, WaitFor*IndexReady
	// helpers) used to receive that error via GetLedger and must keep being
	// able to distinguish "no indexes" from "bad/deleted ledger name"
	// (PR #453 review). Filtering also avoids surfacing orphan SubAttrIndex
	// entries that survive in Pebble until the deferred ledger-data purge.
	if req.GetScope() == servicepb.ListIndexesRequest_SCOPE_LEDGER {
		if _, err := query.GetLedgerByName(ctx, handle, req.GetLedger()); err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return status.Errorf(codes.NotFound, "ledger %q not found", req.GetLedger())
			}

			return fmt.Errorf("checking ledger %q: %w", req.GetLedger(), err)
		}
	}

	// For SCOPE_LEDGER, bound the Pebble iterator to entries whose canonical
	// key starts with `appendLedgerName(req.Ledger)` (the 64-byte zero-padded
	// block). Without the bound, the iterator scans every entry in the
	// SubAttrIndex zone and filters in memory — O(total indexes) instead of
	// O(per-ledger indexes), which matters on buckets with thousands of
	// ledgers. SCOPE_BUCKET and SCOPE_ALL keep the unbounded scan: bucket-
	// scoped entries share the all-zero 64-byte prefix, and ALL needs the
	// whole zone.
	var canonicalPrefix []byte
	if req.GetScope() == servicepb.ListIndexesRequest_SCOPE_LEDGER {
		canonicalPrefix = domain.IndexKey{LedgerName: req.GetLedger()}.Bytes()
	}

	idxIter, err := impl.attrs.Index.NewStreamingIter(handle, canonicalPrefix)
	if err != nil {
		return fmt.Errorf("opening index registry iterator: %w", err)
	}
	defer func() { _ = idxIter.Close() }()

	// Memoize ledger existence across the stream — SCOPE_ALL would otherwise
	// pay one Pebble Get per index entry. Bucket-scoped entries (Ledger == "")
	// skip the lookup entirely.
	activeLedger := make(map[string]bool)

	for idxIter.Next() {
		idx := idxIter.Entry().Value
		if idx == nil || idx.GetId() == nil {
			continue
		}

		switch req.GetScope() {
		case servicepb.ListIndexesRequest_SCOPE_BUCKET:
			if idx.GetLedger() != "" {
				continue
			}
		case servicepb.ListIndexesRequest_SCOPE_LEDGER:
			if idx.GetLedger() != req.GetLedger() {
				continue
			}
		case servicepb.ListIndexesRequest_SCOPE_ALL:
			// Drop entries whose owning ledger no longer exists — see the
			// SCOPE_LEDGER short-circuit above for the orphan rationale.
			name := idx.GetLedger()
			if name != "" {
				alive, cached := activeLedger[name]
				if !cached {
					_, err := query.GetLedgerByName(ctx, handle, name)
					switch {
					case err == nil:
						alive = true
					case errors.Is(err, domain.ErrNotFound):
						alive = false
					default:
						return fmt.Errorf("checking ledger %q: %w", name, err)
					}

					activeLedger[name] = alive
				}

				if !alive {
					continue
				}
			}
		}

		if err := stream.Send(idx); err != nil {
			return err
		}
	}

	return idxIter.Err()
}

// indexIDFromBackfillEntry rebuilds the IndexID associated with a persisted
// backfill cursor, given the BB-key encoding used by the indexbuilder.
func indexIDFromBackfillEntry(e readstore.BackfillEntry) *commonpb.IndexID {
	switch e.Kind {
	case readstore.BackfillKindTxBuiltin:
		if len(e.Details) < 1 {
			return nil
		}

		return &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{
			TxBuiltin: commonpb.TransactionBuiltinIndex(e.Details[0]),
		}}
	case readstore.BackfillKindTxMetadata:
		return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
			Target: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
			Key:    string(e.Details),
		}}}
	case readstore.BackfillKindAcctBuiltin:
		if len(e.Details) < 1 {
			return nil
		}

		return &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{
			AccountBuiltin: commonpb.AccountBuiltinIndex(e.Details[0]),
		}}
	case readstore.BackfillKindAcctMetadata:
		return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
			Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:    string(e.Details),
		}}}
	case readstore.BackfillKindLogBuiltin:
		if len(e.Details) < 1 {
			return nil
		}

		return &commonpb.IndexID{Kind: &commonpb.IndexID_LogBuiltin{
			LogBuiltin: commonpb.LogBuiltinIndex(e.Details[0]),
		}}
	}

	return nil
}

func (impl *BucketServiceServerImpl) CheckStore(_ *servicepb.CheckStoreRequest, stream servicepb.BucketService_CheckStoreServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	checker := check.NewChecker(impl.store, impl.attrs, impl.clusterID, impl.localCtrl.ColdReader(), &impl.idempotencyTTL, impl.logger)

	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		_ = stream.Send(event)
	})
}

func (impl *BucketServiceServerImpl) GetAuditEntry(ctx context.Context, req *servicepb.GetAuditEntryRequest) (*auditpb.AuditEntry, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAuditRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetAuditEntry(ctx, req.GetSequence())
}

func (impl *BucketServiceServerImpl) ListAuditEntries(req *servicepb.ListAuditEntriesRequest, stream servicepb.BucketService_ListAuditEntriesServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListAuditEntries")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAuditRead); err != nil {
		return err
	}

	opts := req.GetOptions()

	// Audit listing is fully under the shared contract: filter (audit[...]
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
		mainStore, readIdx, openErr := impl.openCheckpointStores(ctx, cpID)
		if openErr != nil {
			return openErr
		}

		defer func() {
			_ = readIdx.Close()
			_ = mainStore.Close()
		}()

		// Known limitation (tracked as a follow-up): CreateQueryCheckpoint waits
		// for the log-index builder (readStore.WaitForSequence) before snapshotting
		// the readstore, but NOT for the separate async audit indexer. If the
		// audit index lagged its zone at snapshot time, a *filtered* checkpoint
		// read can omit audit entries that do exist in the checkpoint's audit
		// zone, and — the checkpoint being frozen — it never catches up. The
		// unfiltered checkpoint read is unaffected (it scans the zone directly).
		// The proper fix belongs in the checkpoint-creation path (make the audit
		// indexer catch up before the readstore checkpoint, mirroring the
		// log-index WaitForSequence); it is out of scope here.
		c, err = impl.localCtrl.ListAuditEntriesFrom(ctx, mainStore, readIdx, fetchSize, afterSeq, opts.GetFilter(), opts.GetReverse())
	} else {
		minLogSeq := opts.GetRead().GetMinLogSequence()

		// A filtered live read resolves through the async audit secondary index,
		// which lags the audit zone independently of the log index. Gate it on the
		// audit-index progress so the requested consistency bound actually covers
		// the index this read consults. An unfiltered read scans the Cold/Audit
		// zone directly and is always current, so it keeps the plain log-index wait
		// (its fast path is unchanged).
		if opts.GetFilter() != nil {
			if waitErr := impl.waitFilteredAuditConsistency(ctx, minLogSeq); waitErr != nil {
				return waitErr
			}
		} else if waitErr := impl.waitMinLogSequence(ctx, minLogSeq); waitErr != nil {
			return waitErr
		}

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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

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

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

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

	// minLogSequence only gates live reads; a checkpoint is a fixed snapshot.
	if read.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, read.GetMinLogSequence()); err != nil {
			return err
		}
	}

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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	sinks, err := impl.ctrl.GetEventsSinks(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading sink configs: %w", err)
	}

	handle, err := impl.store.NewDirectReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	// Build statuses by merging error statuses (SubGlobSinkStatus) with cursors (SubGlobSinkCursor).
	errorStatuses, err := query.ReadAllSinkStatuses(handle)
	if err != nil {
		return nil, fmt.Errorf("loading sink statuses: %w", err)
	}

	statusBySink := make(map[string]*commonpb.SinkStatus, len(errorStatuses))
	for _, s := range errorStatuses {
		statusBySink[s.GetSinkName()] = s
	}

	// Enrich with cursor values for every configured sink.
	for _, sink := range sinks {
		cursor, err := query.ReadSinkCursor(handle, sink.GetName())
		if err != nil {
			return nil, fmt.Errorf("loading sink cursor for %q: %w", sink.GetName(), err)
		}

		if existing, ok := statusBySink[sink.GetName()]; ok {
			existing.Cursor = cursor
		} else {
			statusBySink[sink.GetName()] = &commonpb.SinkStatus{
				SinkName: sink.GetName(),
				Cursor:   cursor,
			}
		}
	}

	statuses := make([]*commonpb.SinkStatus, 0, len(statusBySink))
	for _, s := range statusBySink {
		statuses = append(statuses, s)
	}

	return &servicepb.GetEventsSinksResponse{
		Sinks:        sinks,
		SinkStatuses: statuses,
	}, nil
}

func (impl *BucketServiceServerImpl) GetChapterSchedule(ctx context.Context, _ *servicepb.GetChapterScheduleRequest) (*servicepb.GetChapterScheduleResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	cronExpr, err := impl.ctrl.GetChapterSchedule(ctx)
	if err != nil {
		return nil, fmt.Errorf("loading chapter schedule: %w", err)
	}

	return &servicepb.GetChapterScheduleResponse{Cron: cronExpr}, nil
}

func (impl *BucketServiceServerImpl) ListSigningKeys(req *servicepb.ListSigningKeysRequest, stream servicepb.BucketService_ListSigningKeysServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListSigningKeys")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return err
	}

	opts := req.GetOptions()

	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true}); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(ctx, opts.GetRead().GetMinLogSequence()); err != nil {
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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	return impl.ctrl.GetMetadataSchemaStatus(ctx, req.GetLedger())
}

func (impl *BucketServiceServerImpl) AnalyzeAccounts(req *servicepb.AnalyzeAccountsRequest, stream servicepb.BucketService_AnalyzeAccountsServer) error {
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return err
	}

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
	if _, err := internalauth.Authenticate(stream.Context(), impl.authCfg, internalauth.ScopeTransactionsRead); err != nil {
		return err
	}

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

func (impl *BucketServiceServerImpl) CreatePreparedQuery(ctx context.Context, req *servicepb.CreatePreparedQueryRequest) (*servicepb.CreatePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesWrite); err != nil {
		return nil, err
	}

	_, err := impl.ctrl.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_CreatePreparedQuery{
			CreatePreparedQuery: req,
		},
	}))
	if err != nil {
		return nil, err
	}

	return &servicepb.CreatePreparedQueryResponse{}, nil
}

func (impl *BucketServiceServerImpl) UpdatePreparedQuery(ctx context.Context, req *servicepb.UpdatePreparedQueryRequest) (*servicepb.UpdatePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesWrite); err != nil {
		return nil, err
	}

	_, err := impl.ctrl.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: req,
		},
	}))
	if err != nil {
		return nil, err
	}

	return &servicepb.UpdatePreparedQueryResponse{}, nil
}

func (impl *BucketServiceServerImpl) DeletePreparedQuery(ctx context.Context, req *servicepb.DeletePreparedQueryRequest) (*servicepb.DeletePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesWrite); err != nil {
		return nil, err
	}

	_, err := impl.ctrl.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
		Type: &servicepb.Request_DeletePreparedQuery{
			DeletePreparedQuery: req,
		},
	}))
	if err != nil {
		return nil, err
	}

	return &servicepb.DeletePreparedQueryResponse{}, nil
}

func (impl *BucketServiceServerImpl) ListPreparedQueries(ctx context.Context, req *servicepb.ListPreparedQueriesRequest) (*servicepb.ListPreparedQueriesResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return nil, err
	}

	queries, err := impl.ctrl.ListPreparedQueries(ctx, req.GetLedger())
	if err != nil {
		return nil, err
	}

	return &servicepb.ListPreparedQueriesResponse{Queries: queries}, nil
}

func (impl *BucketServiceServerImpl) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return nil, err
	}

	profileCtx, profile := query.WithProfile(ctx)

	resp, err := impl.ctrl.ExecutePreparedQuery(profileCtx, req)
	impl.emitProfile(ctx, profile)

	return resp, err
}

func (impl *BucketServiceServerImpl) GetLedgerStats(ctx context.Context, req *servicepb.GetLedgerStatsRequest) (*commonpb.LedgerStats, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeAccountsRead); err != nil {
		return nil, err
	}

	if req.GetLedger() == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	c, cleanup, err := impl.readController(ctx, req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	// minLogSequence only gates live reads; a checkpoint is a fixed snapshot.
	if req.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, req.GetMinLogSequence()); err != nil {
			return nil, err
		}
	}

	profileCtx, profile := query.WithProfile(ctx)

	result, err := c.AggregateVolumes(profileCtx, req.GetLedger(), req.GetFilter(), query.AggregateOptions{
		UseMaxPrecision: req.GetUseMaxPrecision(),
		GroupByPrefixes: req.GetGroupByPrefixes(),
		CollapseColors:  req.GetCollapseColors(),
	})
	impl.emitProfile(ctx, profile)

	return result, err
}

func (impl *BucketServiceServerImpl) GetNumscript(ctx context.Context, req *servicepb.GetNumscriptRequest) (*commonpb.NumscriptInfo, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return nil, err
	}

	read := req.GetRead()

	if read.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, read.GetMinLogSequence()); err != nil {
			return nil, err
		}
	}

	c, cleanup, err := impl.readController(ctx, read.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetNumscript(ctx, req.GetLedger(), req.GetName(), req.GetVersion())
}

func (impl *BucketServiceServerImpl) ListNumscripts(req *servicepb.ListNumscriptsRequest, stream servicepb.BucketService_ListNumscriptsServer) error {
	ctx, span := bucketTracer.Start(stream.Context(), "grpc.ListNumscripts")
	defer span.End()

	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeQueriesRead); err != nil {
		return err
	}

	opts := req.GetOptions()
	read := opts.GetRead()

	if err := ValidateListOptions(opts, ListOptionsSupport{Reverse: true, CheckpointID: true}); err != nil {
		return err
	}

	if read.GetCheckpointId() == 0 {
		if err := impl.waitMinLogSequence(ctx, read.GetMinLogSequence()); err != nil {
			return err
		}
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

func (impl *BucketServiceServerImpl) InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeLedgersRead); err != nil {
		return nil, err
	}

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
	if _, err := internalauth.Authenticate(ctx, impl.authCfg, internalauth.ScopeOpsRead); err != nil {
		return nil, err
	}

	commitIndex, err := impl.ctrl.Barrier(ctx)
	if err != nil {
		return nil, err
	}

	return &servicepb.BarrierResponse{CommitIndex: commitIndex}, nil
}

func (impl *BucketServiceServerImpl) Discovery(_ context.Context, _ *servicepb.DiscoveryRequest) (*servicepb.DiscoveryResponse, error) {
	resp := &servicepb.DiscoveryResponse{
		ServerInfo: &servicepb.ServerInfo{
			Version:   impl.info.Version,
			Commit:    impl.info.Commit,
			BuildDate: impl.info.BuildDate,
			GoVersion: impl.info.GoVersion,
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

func (impl *BucketServiceServerImpl) emitProfile(ctx context.Context, profile *query.QueryProfile) {
	if profile == nil {
		return
	}

	if profile.TotalDuration() >= impl.queryProfileThreshold {
		profile.LogTo(impl.logger)
		profile.EmitToSpan(trace.SpanFromContext(ctx))
	}

	if wantsProfile(ctx) {
		_ = ggrpc.SetTrailer(ctx, profileToMetadata(profile))
	}
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
