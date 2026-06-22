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
	applyDuration         metric.Int64Histogram
	forwarder             nodeForwarder
}

func NewBucketServiceServer(logger logging.Logger, c ctrl.Controller, localCtrl *ctrl.DefaultController, s *dal.Store, rs *readstore.Store, attrs *attributes.Attributes, sharedState *state.SharedState, receiptSigner *receipt.Signer, responseSigner *signing.ResponseSigner, authCfg internalauth.AuthConfig, queryProfileThreshold time.Duration, clusterID string, meterProvider metric.MeterProvider, n *node.Node, servicePool *transport.ConnectionPool) servicepb.BucketServiceServer {
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

	ctx = adoptForwardedSnapshotIfTrusted(ctx, req)

	if len(req.GetEnvelopes()) == 0 {
		return nil, errEnvelopesRequired
	}

	// Per-envelope scope check: each request in the batch may require a
	// different granular scope. For signed envelopes we peek into the
	// payload to determine the request type — peeking is non-authoritative,
	// admission re-verifies the signature and unmarshals the same bytes for
	// processing. Cluster-internal forwards (cluster-secret authenticated
	// peers) get the full scope set so this loop is a no-op by design.
	if impl.authCfg.Enabled {
		effective := internalauth.ExpandedScopesFromContext(ctx)
		authPresented := internalauth.AuthPresentedFromContext(ctx)

		for i, env := range req.GetEnvelopes() {
			peeked, err := servicepb.PeekRequest(env)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument,
					"envelope %d: %v", i, err)
			}

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

	if impl.logger.Enabled(logging.TraceLevel) {
		impl.logger.Tracef("Apply request received with %d envelopes", len(req.GetEnvelopes()))
	}

	logs, err := impl.ctrl.Apply(ctx, req.GetEnvelopes()...)
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
		metric.WithAttributes(attribute.Int("batch_size", len(req.GetEnvelopes()))))

	if skipResponse {
		for _, log := range logs {
			log.Payload = nil
			log.Idempotency = nil
			log.Signature = nil
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
func adoptForwardedSnapshotIfTrusted(ctx context.Context, req *servicepb.ApplyRequest) context.Context {
	if !internalauth.IsClusterInternal(ctx) {
		return ctx
	}

	fc := req.GetForwardedCallerSnapshot()
	if fc == nil {
		return ctx
	}

	return internalauth.WithForwardedSnapshot(ctx, fc)
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
		// reader is the store the transaction was read from. The receipt is
		// computed from the SAME store so a historical checkpoint read produces
		// a receipt consistent with the checkpoint's ledger/log data, not live
		// data that may have since changed or been purged.
		reader dal.PebbleGetter
		err    error
	)

	if cpID := req.GetCheckpointId(); cpID > 0 {
		mainStore, readIdx, closeErr := impl.openCheckpointStores(cpID)
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
		reader = impl.store
		tx, err = impl.ctrl.GetTransaction(ctx, req.GetLedger(), req.GetTransactionId())
	}

	if err != nil {
		return nil, err
	}

	resp := &servicepb.GetTransactionResponse{Transaction: tx}
	if impl.receiptSigner != nil {
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
func (impl *BucketServiceServerImpl) openCheckpointStores(checkpointID uint64) (*dal.Store, *readstore.Store, error) {
	mainPath := impl.store.QueryCheckpointMainDir(checkpointID)
	readIndexPath := impl.store.QueryCheckpointReadIndexDir(checkpointID)

	mainStore, err := dal.OpenReadOnly(mainPath, impl.logger)
	if err != nil {
		return nil, nil, fmt.Errorf("opening checkpoint main store: %w", err)
	}

	readIdx, err := readstore.OpenReadOnly(readIndexPath, impl.logger)
	if err != nil {
		_ = mainStore.Close()

		return nil, nil, fmt.Errorf("opening checkpoint read index: %w", err)
	}

	return mainStore, readIdx, nil
}

// readController selects the controller to serve a read from. When
// checkpointID is non-zero it opens the query checkpoint's stores and returns
// a checkpoint-scoped local controller plus a cleanup that closes them; reads
// then reflect the checkpoint's point-in-time state. When zero it returns the
// live (routed) controller and a no-op cleanup. The caller must always defer
// the returned cleanup.
func (impl *BucketServiceServerImpl) readController(checkpointID uint64) (ctrl.Controller, func(), error) {
	if checkpointID == 0 {
		return impl.ctrl, func() {}, nil
	}

	mainStore, readIdx, err := impl.openCheckpointStores(checkpointID)
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
		mainStore, readIdx, openErr := impl.openCheckpointStores(cpID)
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

	listingCtrl, cleanup, err := impl.readController(read.GetCheckpointId())
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

	c, cleanup, err := impl.readController(read.GetCheckpointId())
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

	c, cleanup, err := impl.readController(req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.GetAccount(ctx, req.GetLedger(), req.GetAddress())
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

	c, cleanup, err := impl.readController(read.GetCheckpointId())
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

	// Enumerate ledgers and join their LedgerInfo.indexes with the backfill cursors.
	ledgerCursor, err := query.ReadLedgers(ctx, handle)
	if err != nil {
		return nil, fmt.Errorf("reading ledgers: %w", err)
	}
	defer func() { _ = ledgerCursor.Close() }()

	var entries []*servicepb.IndexEntry

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

		if ledgerFilter != "" && info.GetName() != ledgerFilter {
			continue
		}

		for _, idx := range info.GetIndexes() {
			entries = append(entries, &servicepb.IndexEntry{
				Ledger: info.GetName(),
				Index:  idx,
				Cursor: cursors[cursorKey{ledger: info.GetName(), canonical: indexes.Canonical(idx.GetId())}],
			})
		}
	}

	return &servicepb.GetIndexStatusResponse{
		LastIndexedSequence: lastIndexed,
		LastLogSequence:     lastLog,
		Lag:                 lag,
		IndexFileSize:       fileSize,
		Indexes:             entries,
	}, nil
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

	checker := check.NewChecker(impl.store, impl.attrs, impl.clusterID, impl.logger)

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

	// The audit controller does not yet honor filter / reverse /
	// checkpoint_id — see #436 for the alignment work.
	if err := ValidateListOptions(opts, ListOptionsSupport{}); err != nil {
		return err
	}

	if err := impl.waitMinLogSequence(ctx, opts.GetRead().GetMinLogSequence()); err != nil {
		return err
	}

	afterSeq, err := parseUint64Cursor(opts.GetCursor())
	if err != nil {
		return err
	}

	pageSize := ctrl.ClampPageSize(opts.GetPageSize())

	var afterSeqPtr *uint64
	if opts.GetCursor() != "" {
		afterSeqPtr = &afterSeq
	}

	c, err := impl.ctrl.ListAuditEntries(ctx, afterSeqPtr, req.GetFailuresOnly(), pageSizePlusOne(pageSize), req.GetLedger())
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

	c, cleanup, err := impl.readController(req.GetCheckpointId())
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

	c, cleanup, err := impl.readController(read.GetCheckpointId())
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

	_, err := impl.ctrl.Apply(ctx, servicepb.UnsignedEnvelope(&servicepb.Request{
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

	_, err := impl.ctrl.Apply(ctx, servicepb.UnsignedEnvelope(&servicepb.Request{
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

	_, err := impl.ctrl.Apply(ctx, servicepb.UnsignedEnvelope(&servicepb.Request{
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

	c, cleanup, err := impl.readController(req.GetCheckpointId())
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

	c, cleanup, err := impl.readController(req.GetCheckpointId())
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

	c, cleanup, err := impl.readController(read.GetCheckpointId())
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

	c, cleanup, err := impl.readController(read.GetCheckpointId())
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

	c, cleanup, err := impl.readController(req.GetCheckpointId())
	if err != nil {
		return nil, err
	}
	defer cleanup()

	return c.InspectIndex(ctx, req)
}

func (impl *BucketServiceServerImpl) Barrier(ctx context.Context, _ *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	// Barrier proposes a no-op through Raft and waits for it to apply, so it
	// consumes consensus capacity like a write. Require an authenticated scope
	// (ops:read) so it can't be used anonymously as a DoS amplifier or a
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
	resp := &servicepb.DiscoveryResponse{}
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
