package ctrl

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/analysis"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

const (
	// DefaultPageSize is used when callers pass pageSize=0 (unspecified).
	DefaultPageSize uint32 = 100
	// MaxPageSize is the hard server-side upper bound for user-visible list
	// queries (number of items rendered per page).
	MaxPageSize uint32 = 1000
	// MaxFetchSize is the internal cap on what the controller can fetch from
	// the read store. It is MaxPageSize+1 to give the gRPC handler one extra
	// item for peek-ahead — sendPagedToStream uses that extra item to decide
	// whether to publish an x-next-cursor trailer without ever sending it to
	// the client.
	MaxFetchSize uint32 = MaxPageSize + 1
)

// ClampPageSize applies default and maximum bounds to a USER-VISIBLE page
// size value. gRPC handlers feed it the value the client sent in
// ListOptions.page_size and use the result as the rendered-page limit.
func ClampPageSize(pageSize uint32) uint32 {
	if pageSize == 0 {
		return DefaultPageSize
	}

	if pageSize > MaxPageSize {
		return MaxPageSize
	}

	return pageSize
}

// ClampFetchSize is the internal variant used by controllers to size their
// actual read-store query. It allows one extra item past MaxPageSize so the
// gRPC handler can peek-ahead: sendPagedToStream uses the extra item to
// decide whether to publish an x-next-cursor trailer without ever sending it
// to the client.
func ClampFetchSize(pageSize uint32) uint32 {
	if pageSize == 0 {
		return DefaultPageSize
	}

	if pageSize > MaxFetchSize {
		return MaxFetchSize
	}

	return pageSize
}

var (
	tracer         = otel.Tracer("ctrl")
	base64Encoding = base64.RawURLEncoding
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source controller_default.go -destination controller_default_generated_test.go -package ctrl . Admission
type Admission interface {
	Admit(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error)
	Barrier(ctx context.Context) (uint64, error)
}

// DefaultController is the default implementation of the Controller interface.
// It is responsible for forwarding requests to the Raft admission layer.
// The FSM is responsible for interpreting requests, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency in the Raft log.
type DefaultController struct {
	logger     logging.Logger
	admission  Admission
	store      *dal.Store
	attrs      *attributes.Attributes
	readStore  *readstore.Store
	coldReader *coldstorage.ColdReader

	applyDuration metric.Int64Histogram
}

// NewDefaultController creates a new default controller.
func NewDefaultController(
	admission Admission,
	store *dal.Store,
	logger logging.Logger,
	attrs *attributes.Attributes,
	readStore *readstore.Store,
	coldReader *coldstorage.ColdReader,
	meter metric.Meter,
) *DefaultController {
	applyDuration, err := meter.Int64Histogram(
		"ctrl.apply.duration",
		metric.WithDescription("End-to-end duration of a batch Apply call"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(
			0, 100, 500, 2000, 10000, 50000, 200000, 1000000,
		),
	)
	if err != nil {
		panic(err)
	}

	return &DefaultController{
		logger:        logger,
		admission:     admission,
		store:         store,
		attrs:         attrs,
		readStore:     readStore,
		coldReader:    coldReader,
		applyDuration: applyDuration,
	}
}

// ColdReader returns the cold-storage reader, or nil when cold storage is not
// configured. The gRPC layer passes it to the store checker so the idempotency
// pass can re-derive frozen outcomes from archived audit entries.
func (ctrl *DefaultController) ColdReader() *coldstorage.ColdReader {
	return ctrl.coldReader
}

// ListLedgers returns a cursor over all active (non-deleted) ledgers.
func (ctrl *DefaultController) ListLedgers(ctx context.Context) (cursor.Cursor[*commonpb.LedgerInfo], error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	c, err := query.ReadLedgers(ctx, handle)
	if err != nil {
		_ = handle.Close()

		return nil, err
	}
	// Filter out soft-deleted ledgers, enrich with metadata, close handle when cursor closes
	filtered := cursor.NewFilteredCursor(c, func(ledger *commonpb.LedgerInfo) bool {
		if ledger.GetDeletedAt() != nil {
			return false
		}

		// Best-effort enrichment — metadata is decorative, not critical for listing
		_ = query.EnrichLedgerMetadata(handle, ctrl.attrs, ledger)

		return true
	})

	return cursor.NewClosingCursor(filtered, handle), nil
}

func (ctrl *DefaultController) GetTransaction(ctx context.Context, ledgerName string, transactionID uint64) (*commonpb.Transaction, *string, error) {
	// No receipt signer at this layer: a locally-served read returns a nil receipt
	// so the gRPC layer signs it from the same snapshot.
	tx, err := ctrl.GetTransactionFrom(ctx, ctrl.store, ledgerName, transactionID)

	return tx, nil, err
}

// WithStores returns a shallow copy of the controller whose reads are served
// from the given main store and read index instead of the live ones. It is
// intended for read-only query-checkpoint access: callers open a checkpoint's
// stores and route GetAccount/ListAccounts/GetLedgerStats/etc. through the
// returned controller. Write paths must not be used on the result.

func (ctrl *DefaultController) WithStores(store *dal.Store, readStore *readstore.Store) *DefaultController {
	clone := *ctrl
	clone.store = store
	clone.readStore = readStore

	return &clone
}

// GetTransactionFrom reads a transaction using the provided store (live or checkpoint).
func (ctrl *DefaultController) GetTransactionFrom(ctx context.Context, store *dal.Store, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	_, span := tracer.Start(ctx, "ctrl.get_transaction",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int64("transaction_id", int64(transactionID)),
		))
	defer span.End()

	handle, err := store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	return ctrl.buildTransaction(ctx, handle, ledgerInfo.GetName(), transactionID)
}

// buildTransaction builds a transaction from its stored state and creation log.
func (ctrl *DefaultController) buildTransaction(ctx context.Context, reader dal.PebbleReader, ledgerName string, transactionID uint64) (*commonpb.Transaction, error) {
	state, err := query.ReadTransactionState(ctx, reader, ctrl.attrs.Transaction, ledgerName, transactionID)
	if err != nil {
		return nil, fmt.Errorf("reading transaction state for %d: %w", transactionID, err)
	}

	if state == nil || state.GetCreatedByLog() == 0 {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	return assembleTransactionFromState(ctx, reader, transactionID, state)
}

// assembleTransactionFromState builds a transaction from its TransactionState and the creation log.
// Metadata values are returned verbatim — declared_type is an index hint, not
// an API contract, so reads do not coerce.
func assembleTransactionFromState(ctx context.Context, reader dal.PebbleReader, transactionID uint64, state *commonpb.TransactionState) (*commonpb.Transaction, error) {
	log, err := query.ReadLogBySequence(ctx, reader, state.GetCreatedByLog())
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", state.GetCreatedByLog(), err)
	}

	if log == nil {
		return nil, commonpb.NewNotFoundError("transaction %d not found", transactionID)
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok || applyLog.Apply == nil || applyLog.Apply.GetLog() == nil {
		return nil, fmt.Errorf("log %d does not contain an apply log", state.GetCreatedByLog())
	}

	ledgerLog := applyLog.Apply.GetLog()

	var tx *commonpb.Transaction

	switch payload := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if payload.CreatedTransaction == nil || payload.CreatedTransaction.GetTransaction() == nil {
			return nil, errors.New("invalid log payload: missing transaction")
		}

		tx = payload.CreatedTransaction.GetTransaction()
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if payload.RevertedTransaction == nil || payload.RevertedTransaction.GetRevertTransaction() == nil {
			return nil, errors.New("invalid log payload: missing revert transaction")
		}

		tx = payload.RevertedTransaction.GetRevertTransaction()
	default:
		return nil, fmt.Errorf("ledger log %d does not contain a transaction", ledgerLog.GetId())
	}

	tx.Reverted = state.GetRevertedByTransaction() > 0

	// The revert relationship is tracked structurally on the state, not in the
	// create log: reverted_by_transaction + reverted_at on the reverted original,
	// reverts_transaction on the compensating transaction.
	tx.RevertedByTransaction = state.GetRevertedByTransaction()
	tx.RevertedAt = state.GetRevertedAt()
	tx.RevertsTransaction = state.GetRevertsTransaction()

	// The state carries the current metadata (the create-time snapshot plus
	// every applied add/delete) and is authoritative even when empty; the
	// create log only holds the create-time snapshot.
	tx.Metadata = state.GetMetadata()

	return tx, nil
}

// ListTransactions returns a cursor over transactions for a ledger.
// API convention: reverse=false means newest-first (descending), reverse=true means oldest-first.
// Internally listEntities uses reverse=true for descending, so we invert the flag here.
func (ctrl *DefaultController) ListTransactions(ctx context.Context, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error) {
	return ctrl.ListTransactionsFrom(ctx, ctrl.store, ctrl.readStore, ledgerName, pageSize, afterTxID, filter, reverse)
}

// ListTransactionsFrom returns a cursor over transactions using the provided stores (live or checkpoint).
func (ctrl *DefaultController) ListTransactionsFrom(ctx context.Context, store *dal.Store, rs *readstore.Store, ledgerName string, pageSize uint32, afterTxID uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Transaction], error) {
	ctx, span := tracer.Start(ctx, "ctrl.list_transactions",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int("page_size", int(pageSize)),
			attribute.Bool("reverse", reverse),
		))
	defer span.End()

	profile := query.ProfileFromContext(ctx)

	// Create a Pebble snapshot first so that GetLedgerByName and the listing
	// read from the same consistent point-in-time view.
	handle, err := store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		_ = handle.Close()

		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	// Defense in depth: server callers already clamp, but a missed call
	// site (or a non-public caller) must not be able to ask for
	// math.MaxUint32 here and force the cursor to materialise an
	// unbounded slice.
	pageSize = ClampFetchSize(pageSize)

	schemaFields := query.SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)

	indexStart := time.Now()

	result, err := listEntities(rs, entityListParams[uint64]{
		target:        commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		ledgerName:    ledgerInfo.GetName(),
		pageSize:      pageSize,
		after:         afterTxID,
		filter:        filter,
		reverse:       !reverse, // API: reverse=false → newest-first (desc); listEntities: reverse=true → desc
		schema:        schemaFields,
		info:          ledgerInfo,
		profile:       profile,
		pebbleReader:  handle,
		indexRegistry: query.NewPebbleIndexReader(ctrl.attrs.Index, handle),
		// indexVersionFor deliberately omitted — listEntities binds
		// it to its own iteration snapshot.
		afterToBytes: func(id uint64) []byte {
			b := make([]byte, 8)
			binary.BigEndian.PutUint64(b, id)

			return b
		},
	})

	if profile != nil {
		profile.IndexDuration = time.Since(indexStart)
	}

	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing transactions from index: %w", err)
	}

	// Enrich each transaction ID from Pebble
	enrichStart := time.Now()

	txns, err := query.EnrichTransactions(ctx, result.entityIDs, ctrl.entityEnricher(), handle, ledgerInfo.GetName())
	if err != nil {
		_ = handle.Close()

		return nil, err
	}

	if profile != nil {
		profile.EnrichmentDuration = time.Since(enrichStart)
		profile.EnrichedCount = len(txns)
		profile.ItemsCollected = len(result.entityIDs)
	}

	return cursor.NewClosingCursor(cursor.NewSliceCursor(txns), handle), nil
}

// ListAccounts returns a cursor over accounts for a ledger.
// Default order (reverse=false) is ascending (A→Z); reverse=true gives reverse-alphabetical (Z→A).
// Uses the Pebble read index for entity discovery and Pebble for enrichment.
func (ctrl *DefaultController) ListAccounts(ctx context.Context, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*commonpb.Account], error) {
	ctx, span := tracer.Start(ctx, "ctrl.list_accounts",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int("page_size", int(pageSize)),
			attribute.Bool("reverse", reverse),
		))
	defer span.End()

	profile := query.ProfileFromContext(ctx)

	// Create a Pebble snapshot first so that GetLedgerByName and the listing
	// read from the same consistent point-in-time view.
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		_ = handle.Close()

		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	// Defense in depth — see ListTransactionsFrom for rationale.
	pageSize = ClampFetchSize(pageSize)

	schemaFields := query.SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)

	indexStart := time.Now()

	result, err := listEntities(ctrl.readStore, entityListParams[string]{
		target:        commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		ledgerName:    ledgerInfo.GetName(),
		pageSize:      pageSize,
		after:         afterAddress,
		filter:        filter,
		reverse:       reverse,
		schema:        schemaFields,
		info:          ledgerInfo,
		profile:       profile,
		pebbleReader:  handle,
		indexRegistry: query.NewPebbleIndexReader(ctrl.attrs.Index, handle),
		// indexVersionFor deliberately omitted — listEntities binds
		// it to its own iteration snapshot.
		afterToBytes: func(addr string) []byte {
			return []byte(addr)
		},
	})

	if profile != nil {
		profile.IndexDuration = time.Since(indexStart)
	}

	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing accounts from index: %w", err)
	}

	// Enrich each account from Pebble
	enrichStart := time.Now()

	accounts, err := query.EnrichAccounts(result.entityIDs, ctrl.entityEnricher(), handle, ledgerInfo.GetName())
	if err != nil {
		_ = handle.Close()

		return nil, err
	}

	if profile != nil {
		profile.EnrichmentDuration = time.Since(enrichStart)
		profile.EnrichedCount = len(accounts)
		profile.ItemsCollected = len(result.entityIDs)
	}

	return cursor.NewClosingCursor(cursor.NewSliceCursor(accounts), handle), nil
}

func (ctrl *DefaultController) GetAccount(ctx context.Context, ledgerName string, address string, opts GetAccountOptions) (*commonpb.Account, error) {
	_, span := tracer.Start(ctx, "ctrl.get_account",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.String("address", address),
		))
	defer span.End()

	// One snapshot so the existence check and the account scan observe the same
	// committed state.
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	return scanAccount(handle, ctrl.attrs, ledgerInfo.GetName(), address, opts.CollapseColors, ctrl.logger)
}

// GetLedgerStats returns aggregate statistics for a ledger.
// All counters are O(1) reads from the LedgerBoundaries attribute.
func (ctrl *DefaultController) GetLedgerStats(ctx context.Context, ledgerName string) (*commonpb.LedgerStats, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	if _, err := query.GetLedgerByName(ctx, handle, ledgerName); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	boundaries, err := ctrl.attrs.Boundary.Get(handle, domain.LedgerKey{Name: ledgerName}.Bytes())
	if err != nil {
		return nil, fmt.Errorf("reading boundaries for stats: %w", err)
	}

	var stats commonpb.LedgerStats
	if boundaries != nil {
		if nextTxID := boundaries.GetNextTransactionId(); nextTxID > 0 {
			stats.TransactionCount = nextTxID - 1
		}

		stats.VolumeCount = boundaries.GetVolumeCount()
		stats.MetadataCount = boundaries.GetMetadataCount()
		stats.ReferenceCount = boundaries.GetReferenceCount()
		stats.PostingCount = boundaries.GetPostingCount()
		stats.EphemeralEvictedCount = boundaries.GetEphemeralEvictedCount()
		stats.TransientUsedCount = boundaries.GetTransientUsedCount()
		stats.RevertCount = boundaries.GetRevertCount()
		stats.NumscriptExecutionCount = boundaries.GetNumscriptExecutionCount()

		if nextLogID := boundaries.GetNextLogId(); nextLogID > 0 {
			stats.LogCount = nextLogID - 1
		}
	}

	return &stats, nil
}

func (ctrl *DefaultController) GetLedgerByName(ctx context.Context, name string) (*commonpb.LedgerInfo, error) {
	_, span := tracer.Start(ctx, "ctrl.get_ledger",
		trace.WithAttributes(attribute.String("ledger", name)))
	defer span.End()

	// The response stitches together the LedgerInfo (account types), optional mirror
	// sync progress, and ledger metadata. Must all be read from the same handle.
	handle, handleErr := ctrl.store.NewReadHandle()
	if handleErr != nil {
		return nil, fmt.Errorf("creating read handle: %w", handleErr)
	}

	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, name)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", name)
		}

		return nil, err
	}

	if ledgerInfo.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		progress, err := query.ReadMirrorSyncProgress(ctx, handle, name)
		if err != nil {
			return nil, fmt.Errorf("reading mirror sync progress: %w", err)
		}

		ledgerInfo.MirrorSyncProgress = progress
	}

	if err := query.EnrichLedgerMetadata(handle, ctrl.attrs, ledgerInfo); err != nil {
		return nil, fmt.Errorf("enriching ledger metadata: %w", err)
	}

	return ledgerInfo, nil
}

// GetMetadataSchemaStatus returns the conversion status of all declared metadata fields.
func (ctrl *DefaultController) GetMetadataSchemaStatus(ctx context.Context, ledgerName string) (*servicepb.GetMetadataSchemaStatusResponse, error) {
	ledgerInfo, err := query.GetLedgerByName(ctx, ctrl.store, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	resp := &servicepb.GetMetadataSchemaStatusResponse{
		AccountFields:     make(map[string]*servicepb.MetadataFieldStatus),
		TransactionFields: make(map[string]*servicepb.MetadataFieldStatus),
		LedgerFields:      make(map[string]*servicepb.MetadataFieldStatus),
	}

	if ledgerInfo.GetMetadataSchema() != nil {
		for key, field := range ledgerInfo.GetMetadataSchema().GetAccountFields() {
			resp.AccountFields[key] = &servicepb.MetadataFieldStatus{DeclaredType: field.GetType()}
		}

		for key, field := range ledgerInfo.GetMetadataSchema().GetTransactionFields() {
			resp.TransactionFields[key] = &servicepb.MetadataFieldStatus{DeclaredType: field.GetType()}
		}

		for key, field := range ledgerInfo.GetMetadataSchema().GetLedgerFields() {
			resp.LedgerFields[key] = &servicepb.MetadataFieldStatus{DeclaredType: field.GetType()}
		}
	}

	return resp, nil
}

// AnalyzeAccounts scans all accounts in a ledger and suggests a Chart of Accounts.
// Uses a direct Pebble key scan to extract account addresses, asset names, and
// metadata key names without reading values or going through the read index.
func (ctrl *DefaultController) AnalyzeAccounts(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, fmt.Errorf("validating ledger for analysis: %w", err)
	}

	it, err := query.NewCompactAccountIterator(handle, ledgerInfo.GetName())
	if err != nil {
		return nil, err
	}

	defer func() { _ = it.Close() }()

	return analysis.AnalyzeFromIterator(it.Next, variableThreshold, onProgress)
}

// AnalyzeTransactions scans all transactions in a ledger and discovers flow patterns.
// Uses two sequential Pebble log scans with streaming processing to avoid loading
// all transactions into memory (O(unique addresses + unique signatures) instead of O(N)).
func (ctrl *DefaultController) AnalyzeTransactions(ctx context.Context, ledgerName string, variableThreshold uint32, onProgress func(processed, total uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	if _, err := query.GetLedgerByName(ctx, handle, ledgerName); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, fmt.Errorf("validating ledger for analysis: %w", err)
	}

	// makeStreamIter returns an iterator function that streams CompactTransactions
	// from a fresh log scan. Each call creates a new scan cursor.
	var (
		totalReverted uint64
		pass1Done     bool
	)

	makeStreamIter := func() (func() (analysis.CompactTransaction, error), func()) {
		var (
			cursor cursor.Cursor[*commonpb.Log]
			done   bool
		)

		next := func() (analysis.CompactTransaction, error) {
			if done {
				return analysis.CompactTransaction{}, io.EOF
			}
			// Lazy cursor creation on first call
			if cursor == nil {
				var err error

				cursor, err = query.ReadLogsSince(ctx, handle, 0, dal.WithReuse())
				if err != nil {
					return analysis.CompactTransaction{}, fmt.Errorf("creating log cursor for analysis: %w", err)
				}
			}

			for {
				log, err := cursor.Next()
				if errors.Is(err, io.EOF) {
					done = true

					return analysis.CompactTransaction{}, io.EOF
				}

				if err != nil {
					return analysis.CompactTransaction{}, fmt.Errorf("reading log for analysis: %w", err)
				}

				if log.GetPayload() == nil {
					continue
				}

				applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
				if !ok {
					continue
				}

				if applyLog.Apply.GetLedgerName() != ledgerName {
					continue
				}

				ledgerLog := applyLog.Apply.GetLog()
				if ledgerLog == nil || ledgerLog.GetData() == nil {
					continue
				}

				switch p := ledgerLog.GetData().GetPayload().(type) {
				case *commonpb.LedgerLogPayload_CreatedTransaction:
					if p.CreatedTransaction.GetTransaction() == nil {
						continue
					}

					return analysis.ExtractCompactTransaction(p.CreatedTransaction.GetTransaction()), nil

				case *commonpb.LedgerLogPayload_RevertedTransaction:
					// Count reverted during pass 1 only
					if !pass1Done {
						totalReverted++
					}

					if p.RevertedTransaction.GetRevertTransaction() != nil {
						return analysis.ExtractCompactTransaction(p.RevertedTransaction.GetRevertTransaction()), nil
					}

					continue
				default:
					continue
				}
			}
		}
		cleanup := func() {
			if cursor != nil {
				_ = cursor.Close()
			}
		}

		return next, cleanup
	}

	pass1, cleanup1 := makeStreamIter()
	defer cleanup1()

	pass2Fn, cleanup2 := makeStreamIter()
	defer cleanup2()

	// After pass1 completes inside AnalyzeTransactionsFromIterators, totalReverted
	// will have been counted. Mark pass1Done to avoid double counting during pass2.
	wrappedPass1 := func() (analysis.CompactTransaction, error) {
		ct, err := pass1()
		if errors.Is(err, io.EOF) {
			pass1Done = true
		}

		return ct, err
	}

	return analysis.AnalyzeTransactionsFromIterators(wrappedPass1, pass2Fn, func() uint64 { return totalReverted }, variableThreshold, onProgress)
}

// AggregateVolumes returns per-asset aggregated volumes for filtered accounts.
func (ctrl *DefaultController) AggregateVolumes(ctx context.Context, ledgerName string, filter *commonpb.QueryFilter, opts query.AggregateOptions) (*commonpb.AggregateResult, error) {
	ctx, span := tracer.Start(ctx, "ctrl.aggregate_volumes",
		trace.WithAttributes(attribute.String("ledger", ledgerName)))
	defer span.End()

	profile := query.ProfileFromContext(ctx)

	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	// Fast path: unfiltered aggregation scans Pebble volumes in a single pass.
	// No index interaction needed — Pebble snapshot is the source of truth.
	if filter == nil {
		enrichStart := time.Now()

		result, aggErr := query.AggregateAllVolumes(handle, ctrl.attrs.Volume, ledgerInfo.GetName(), opts)
		if aggErr != nil {
			return nil, fmt.Errorf("aggregating volumes: %w", aggErr)
		}

		if profile != nil {
			profile.EnrichmentDuration = time.Since(enrichStart)
		}

		return result, nil
	}

	schemaFields := query.SchemaFieldsForTarget(ledgerInfo.GetMetadataSchema(), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)

	snap := ctrl.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	kb := dal.NewKeyBuilder()

	indexStart := time.Now()

	iter, err := query.Compile(snap, kb, filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, ledgerInfo.GetName(), nil, schemaFields, ledgerInfo, query.NewPebbleIndexReader(ctrl.attrs.Index, handle), readstore.SnapshotVersionResolver(snap, ledgerInfo.GetName()), profile, handle)
	if err != nil {
		return nil, domain.WrapCompileError(err)
	}
	defer iter.Close()

	if profile != nil {
		profile.IndexDuration = time.Since(indexStart)
	}

	enrichStart := time.Now()

	result, err := query.AggregateVolumes(handle, ctrl.attrs.Volume, ledgerInfo.GetName(), iter, opts)
	if err != nil {
		return nil, fmt.Errorf("aggregating volumes: %w", err)
	}

	if profile != nil {
		profile.EnrichmentDuration = time.Since(enrichStart)
	}

	return result, nil
}

// InspectIndex scans a metadata index and returns distinct values, facets, or a summary.
func (ctrl *DefaultController) InspectIndex(ctx context.Context, req *servicepb.InspectIndexRequest) (*servicepb.InspectIndexResponse, error) {
	ctx, span := tracer.Start(ctx, "ctrl.inspect_index",
		trace.WithAttributes(
			attribute.String("ledger", req.GetLedger()),
			attribute.String("metadata_key", req.GetMetadataKey()),
		))
	defer span.End()

	// One snapshot for the schema gate and the index-registry lookup so they
	// observe the same committed state.
	handleForIndex, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle for index lookup: %w", err)
	}
	defer func() { _ = handleForIndex.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handleForIndex, req.GetLedger())
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", req.GetLedger())
		}

		return nil, err
	}

	var (
		fields    map[string]*commonpb.MetadataFieldSchema
		namespace string
	)

	switch req.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = ledgerInfo.GetMetadataSchema().GetTransactionFields()
		namespace = readstore.NamespaceTransaction
	default:
		fields = ledgerInfo.GetMetadataSchema().GetAccountFields()
		namespace = readstore.NamespaceAccount
	}

	metaKey := req.GetMetadataKey()

	if _, ok := fields[metaKey]; !ok {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{
			Index: fmt.Sprintf("metadata[%q] on %s", metaKey, req.GetTargetType()),
		}}
	}

	indexReader := query.NewPebbleIndexReader(ctrl.attrs.Index, handleForIndex)

	indexID := indexes.MetadataID(req.GetTargetType(), metaKey)
	idx, err := indexes.Find(indexReader, ledgerInfo.GetName(), indexID)
	if err != nil {
		return nil, fmt.Errorf("looking up index for inspect: %w", err)
	}

	if idx == nil {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexNotFound{
			Index: fmt.Sprintf("metadata[%q] on %s", metaKey, req.GetTargetType()),
		}}
	}

	// Per-replica readiness: BuildStatus on the FSM Index entry is
	// informational only since EN-1323; the local replica's
	// IndexVersionState.CurrentVersion is what decides whether queries
	// can be served. We take the snapshot FIRST and read the version
	// state through it so the gate and the subsequent Inspect scan
	// observe the same point-in-time view — without this the atomic
	// version switch could promote CurrentVersion between the gate
	// and the iteration, leaving the scan looking at a keyspace that
	// has already been GC'd in this snapshot.
	snap := ctrl.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	state, _, err := readstore.ReadIndexVersionStateFrom(snap, ledgerInfo.GetName(), indexes.Canonical(indexID))
	if err != nil {
		return nil, fmt.Errorf("reading index version state: %w", err)
	}

	if state.CurrentVersion == 0 {
		return nil, &domain.BusinessError{Err: &domain.ErrIndexBuilding{
			Index: fmt.Sprintf("metadata[%q] on %s", metaKey, req.GetTargetType()),
		}}
	}

	var cursorBytes []byte
	if c := req.GetCursor(); c != "" {
		cursorBytes, err = decodeCursor(c)
		if err != nil {
			return nil, fmt.Errorf("invalid cursor: %w", err)
		}
	}

	var mode readstore.InspectMode
	switch req.GetMode() {
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS:
		mode = readstore.InspectFacetsMode
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY:
		mode = readstore.InspectSummaryMode
	default:
		mode = readstore.InspectDistinctValuesMode
	}

	inspectResult, err := readstore.InspectIndex(readstore.InspectParams{
		Reader:      snap,
		KB:          dal.NewKeyBuilder(),
		LedgerName:  ledgerInfo.GetName(),
		Namespace:   namespace,
		MetadataKey: metaKey,
		Version:     state.CurrentVersion,
		Mode:        mode,
		PageSize:    req.GetPageSize(),
		CursorBytes: cursorBytes,
	})
	if err != nil {
		return nil, fmt.Errorf("inspecting index: %w", err)
	}

	return toInspectIndexResponse(inspectResult), nil
}

func toInspectIndexResponse(r *readstore.InspectResult) *servicepb.InspectIndexResponse {
	if r.Values != nil {
		var nextCursor string
		if r.HasMore && len(r.NextCursor) > 0 {
			nextCursor = encodeCursor(r.NextCursor)
		}

		return &servicepb.InspectIndexResponse{
			Result: &servicepb.InspectIndexResponse_DistinctValues{
				DistinctValues: &servicepb.InspectDistinctValues{
					Values:     r.Values,
					HasMore:    r.HasMore,
					NextCursor: nextCursor,
				},
			},
		}
	}

	if r.Facets != nil {
		facets := make([]*servicepb.InspectFacet, len(r.Facets))
		for i, f := range r.Facets {
			facets[i] = &servicepb.InspectFacet{
				Value: f.Value,
				Count: f.Count,
			}
		}

		var nextCursor string
		if r.HasMore && len(r.NextCursor) > 0 {
			nextCursor = encodeCursor(r.NextCursor)
		}

		return &servicepb.InspectIndexResponse{
			Result: &servicepb.InspectIndexResponse_Facets{
				Facets: &servicepb.InspectFacets{
					Facets:     facets,
					HasMore:    r.HasMore,
					NextCursor: nextCursor,
				},
			},
		}
	}

	return &servicepb.InspectIndexResponse{
		Result: &servicepb.InspectIndexResponse_Summary{
			Summary: &servicepb.InspectSummary{
				Cardinality:      r.Cardinality,
				Min:              r.Min,
				Max:              r.Max,
				EntitiesWithKey:  r.EntitiesWithKey,
				EntitiesWithNull: r.EntitiesWithNull,
			},
		},
	}
}

func decodeCursor(s string) ([]byte, error) {
	return base64Encoding.DecodeString(s)
}

func encodeCursor(b []byte) string {
	return base64Encoding.EncodeToString(b)
}

// ListLogs returns a cursor over logs for a specific ledger, ordered by
// ledger-local log ID. The per-ledger log index is unconditionally maintained
// by the indexbuilder, so every read uses the Compile framework — boolean
// filters and date ranges are honored on the single code path.
func (ctrl *DefaultController) ListLogs(ctx context.Context, ledgerName string, afterSequence uint64, pageSize uint32, filter *commonpb.QueryFilter) (cursor.Cursor[*commonpb.Log], error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledgerName)
	if err != nil {
		_ = handle.Close()

		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("ledger %s not found", ledgerName)
		}

		return nil, err
	}

	pageSize = ClampFetchSize(pageSize)

	// Translate afterSequence into a LogId filter so the Compile framework
	// respects the cursor position. LogId with min=afterSequence, min_exclusive=true
	// excludes the entry at afterSequence and returns only newer entries.
	if afterSequence > 0 {
		afterFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogId{
				LogId: &commonpb.LogIdCondition{
					Cond: &commonpb.UintCondition{
						Min:          &afterSequence,
						MinExclusive: true,
					},
				},
			},
		}
		if filter != nil {
			filter = &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_And{
					And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{filter, afterFilter}},
				},
			}
		} else {
			filter = afterFilter
		}
	}

	snap := ctrl.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	kb := dal.NewKeyBuilder()

	iter, err := query.Compile(
		snap, kb, filter,
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
		ledgerInfo.GetName(), nil, nil,
		ledgerInfo, query.NewPebbleIndexReader(ctrl.attrs.Index, handle), readstore.SnapshotVersionResolver(snap, ledgerInfo.GetName()), nil, handle,
	)
	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("compiling log filter: %w", err)
	}
	defer iter.Close()

	logIDs, _, paginateErr := readstore.PaginateForward(iter, pageSize, nil)
	if paginateErr != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("paginating log filter: %w", paginateErr)
	}

	c, err := query.ReadLedgerLogsCompiled(handle, snap, ledgerInfo.GetName(), logIDs)
	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("reading ledger logs: %w", err)
	}

	return cursor.NewClosingCursor(c, handle), nil
}

// ListAuditEntries returns a cursor over audit entries against the live store,
// honoring the shared list contract (filter, reverse, cursor, page size). It
// delegates to ListAuditEntriesFrom bound to the controller's own stores.
//
// Audit has no dedicated top-level filters: ledger scope and outcome selection
// are expressed entirely through filter (audit[ledger], audit[outcome], …).
//
// Ordering: the audit trail is chronological, so the default (reverse=false)
// iterates ascending by sequence (oldest first) — this is the audit trail's
// natural read order and is preserved from the pre-ListOptions behavior.
// reverse=true iterates descending (newest first).
func (ctrl *DefaultController) ListAuditEntries(ctx context.Context, pageSize uint32, afterSequence uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
	return ctrl.ListAuditEntriesFrom(ctx, ctrl.store, ctrl.readStore, pageSize, afterSequence, filter, reverse)
}

// ListAuditEntriesFrom returns a cursor over audit entries using the provided
// stores (live or query checkpoint). The audit trail is queried exclusively
// through the readstore audit secondary index (EN-1339) plus an audit-zone
// sequence bound — there is no scan-time predicate fallback, so an expression
// the index cannot answer is rejected upstream with InvalidArgument.
func (ctrl *DefaultController) ListAuditEntriesFrom(ctx context.Context, store *dal.Store, rs *readstore.Store, pageSize uint32, afterSequence uint64, filter *commonpb.QueryFilter, reverse bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
	ctx, span := tracer.Start(ctx, "ctrl.list_audit_entries",
		trace.WithAttributes(
			attribute.Int("page_size", int(pageSize)),
			attribute.Bool("reverse", reverse),
		))
	defer span.End()

	// Defense in depth: server callers already clamp, but a missed call site
	// must not be able to force an unbounded materialization.
	pageSize = ClampFetchSize(pageSize)

	seqs, loSeq, hiSeq, narrowed, err := query.CompileAuditFilter(rs, filter)
	if err != nil {
		return nil, err
	}

	handle, err := store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	// Audit default is chronological (ascending); ReadAuditEntriesPage takes
	// reverse=false as ascending, so pass reverse through directly.
	c, err := query.ReadAuditEntriesPage(ctx, handle, seqs, narrowed, loSeq, hiSeq, afterSequence, reverse, pageSize)
	if err != nil {
		_ = handle.Close()

		return nil, fmt.Errorf("listing audit entries: %w", err)
	}

	return cursor.NewClosingCursor(c, handle), nil
}

// GetLog returns a single system log by sequence number.
// Falls back to cold storage if the log has been archived.
func (ctrl *DefaultController) GetLog(ctx context.Context, sequence uint64) (*commonpb.Log, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	log, err := query.ReadLogBySequenceWithCold(ctx, handle, ctrl.coldReader, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting log %d: %w", sequence, err)
	}

	if log == nil {
		return nil, commonpb.NewNotFoundError("log %d not found", sequence)
	}

	return log, nil
}

// GetAuditEntry returns a single audit entry by sequence number, with items populated.
func (ctrl *DefaultController) GetAuditEntry(ctx context.Context, sequence uint64) (*auditpb.AuditEntry, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	entry, err := query.ReadAuditEntry(ctx, handle, sequence)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, commonpb.NewNotFoundError("audit entry %d not found", sequence)
		}

		return nil, fmt.Errorf("getting audit entry %d: %w", sequence, err)
	}

	items, err := query.ReadAuditItems(ctx, handle, sequence)
	if err != nil {
		return nil, fmt.Errorf("reading audit items for entry %d: %w", sequence, err)
	}

	entry.Items = items

	return entry, nil
}

// ListChapters returns a cursor over all non-purged chapters from the store.
func (ctrl *DefaultController) ListChapters(ctx context.Context) (cursor.Cursor[*commonpb.Chapter], error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	c, err := query.ReadChapters(ctx, handle)
	if err != nil {
		_ = handle.Close()

		return nil, err
	}

	return cursor.NewClosingCursor(c, handle), nil
}

// ListSigningKeys returns a cursor over all registered signing keys.
func (ctrl *DefaultController) ListSigningKeys(ctx context.Context) (cursor.Cursor[*commonpb.SigningKey], error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	c, err := query.ReadSigningKeysCursor(ctx, handle)
	if err != nil {
		_ = handle.Close()

		return nil, err
	}

	return cursor.NewClosingCursor(c, handle), nil
}

// ListPreparedQueries returns all prepared queries for a ledger.
func (ctrl *DefaultController) ListPreparedQueries(ctx context.Context, ledger string) ([]*commonpb.PreparedQuery, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledger)
	if err != nil {
		return nil, err
	}

	return query.ReadPreparedQueries(ctx, ctrl.attrs.PreparedQuery, handle, ledgerInfo.GetName())
}

// entityEnricher returns an EntityEnricher that uses the controller's attributes
// and transaction assembly logic to hydrate raw entity IDs into full objects.
func (ctrl *DefaultController) entityEnricher() *query.EntityEnricher {
	return &query.EntityEnricher{
		EnrichAccount: func(reader dal.PebbleReader, ledgerName string, address string) (*commonpb.Account, error) {
			// List-style enrichment paths do not surface a per-call collapse
			// flag; entries are returned color-segregated and the caller can
			// collapse client-side if needed. Per-account GetAccount honors
			// the flag through the GetAccountOptions path.
			return scanAccount(reader, ctrl.attrs, ledgerName, address, false, ctrl.logger)
		},
		EnrichTransaction: func(ctx context.Context, reader dal.PebbleReader, ledgerName string, txID uint64) (*commonpb.Transaction, error) {
			return ctrl.buildTransaction(ctx, reader, ledgerName, txID)
		},
	}
}

// ExecutePreparedQuery executes a prepared query against the read index store.
func (ctrl *DefaultController) ExecutePreparedQuery(ctx context.Context, req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	profile := query.ProfileFromContext(ctx)

	return query.Execute(ctx, ctrl.readStore, ctrl.store, ctrl.attrs.Volume, ctrl.attrs.PreparedQuery, ctrl.attrs.Index, req, profile, ctrl.entityEnricher())
}

// GetNumscript returns a numscript by ledger, name and optional version ("" = latest).
func (ctrl *DefaultController) GetNumscript(ctx context.Context, ledger, name string, version string) (*commonpb.NumscriptInfo, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledger)
	if err != nil {
		return nil, err
	}

	info, err := query.ReadNumscript(ctrl.attrs.NumscriptVersion, ctrl.attrs.NumscriptContent, handle, ledgerInfo.GetName(), name, version)
	if err != nil {
		return nil, fmt.Errorf("reading numscript %q: %w", name, err)
	}

	if info == nil {
		return nil, commonpb.NewNotFoundError("numscript %q not found", name)
	}

	return info, nil
}

// ListNumscripts returns the latest version of all numscripts for a ledger.
func (ctrl *DefaultController) ListNumscripts(ctx context.Context, ledger string) ([]*commonpb.NumscriptInfo, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	ledgerInfo, err := query.GetLedgerByName(ctx, handle, ledger)
	if err != nil {
		return nil, err
	}

	return query.ReadAllNumscripts(ctrl.attrs.NumscriptVersion, ctrl.attrs.NumscriptContent, handle, ledgerInfo.GetName())
}

func (ctrl *DefaultController) GetChapterSchedule(_ context.Context) (string, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return "", fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	return query.ReadChapterSchedule(handle)
}

func (ctrl *DefaultController) GetEventsSinks(_ context.Context) ([]*commonpb.SinkConfig, error) {
	handle, err := ctrl.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = handle.Close() }()

	return query.ReadAllSinkConfigs(ctrl.attrs.SinkConfig, handle)
}

// Barrier proposes a no-op through Raft consensus. When it returns, all
// previously proposed entries are guaranteed to have been applied.
// Returns the Raft commit index at which the barrier was applied.
func (ctrl *DefaultController) Barrier(ctx context.Context) (uint64, error) {
	return ctrl.admission.Barrier(ctx)
}

// Apply applies a list of envelopes and returns the resulting logs.
// The controller forwards envelopes to the Raft admission layer, which
// verifies signatures (for signed envelopes) and unwraps them into Requests.
// The FSM is responsible for interpreting orders, validating, and applying changes.
// Idempotency is handled in the FSM to ensure consistency.
func (ctrl *DefaultController) Apply(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
	// Non-authoritative peek for metrics and the empty-batch guard. A signed
	// payload is opaque until admission verifies its signature over the raw
	// bytes, so a peek failure here (e.g. a tampered payload) must defer to
	// admission — which rejects a bad signature with PermissionDenied — rather
	// than reject early. Only guard an empty batch when the peek succeeded.
	peeked, peekErr := servicepb.PeekBatch(req)

	batchSize := 0
	if peekErr == nil {
		batchSize = len(peeked.GetRequests())
	}

	ctx, span := tracer.Start(ctx, "ctrl.apply",
		trace.WithAttributes(attribute.Int("request_count", batchSize)))
	defer span.End()

	start := time.Now()

	if peekErr == nil && batchSize == 0 {
		return nil, errors.New("at least one request is required")
	}

	logs, err := ctrl.admission.Admit(ctx, req)

	ctrl.applyDuration.Record(ctx, time.Since(start).Microseconds(),
		metric.WithAttributes(attribute.Int("batch_size", batchSize)))

	if err != nil {
		return nil, fmt.Errorf("applying raft requests: %w", err)
	}

	return logs, nil
}

var _ Controller = (*DefaultController)(nil)
