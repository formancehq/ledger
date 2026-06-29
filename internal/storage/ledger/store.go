package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/storage/migrations"
	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/tracing"
)

// TransactionListConfig configures the adaptive fallback for the
// transactions-list SELECT.
//
// Background: with ORDER BY id DESC LIMIT N and JSONB @> predicates, Postgres
// tends to choose an Index Scan Backward on the id B-tree. For a wallet whose
// matching rows are sparse or old that scan walks a large fraction of the table
// before collecting N results (observed: ~50 s at production scale with sparse wallets).
// For a wallet whose matches are dense/recent the backward scan is the FAST
// plan; forcing GIN bitmap scan instead would hurt it.
//
// The adaptive fallback solves the dilemma automatically:
//   - Run the query normally with a short probe timeout (FirstAttemptTimeoutMs).
//   - If Postgres cancels with SQLSTATE 57014 (our timeout, not client disconnect),
//     the plan is pathological — retry once with the GIN planner override.
//   - Dense wallets never hit the probe timeout, so they never pay the retry cost.
//
// This is a stopgap. The real fix is a composite/denormalised index that serves
// both the wallet filter and the id ordering without a full sort step.
type TransactionListConfig struct {
	// EnableAdaptiveFallback turns on the probe-then-retry strategy described
	// above. Default true — the untriggered path (dense wallets, fast queries)
	// is identical to today's behaviour except for the explicit transaction
	// wrapping around the SELECT. Call this out explicitly during review.
	EnableAdaptiveFallback bool

	// FirstAttemptTimeoutMs is the SET LOCAL statement_timeout for the probe
	// attempt (milliseconds). If the query finishes within this budget the
	// fallback is never triggered. Must leave enough headroom for the retry
	// before the upstream client disconnects (~50 s worst case at production scale).
	// Default 5000 ms.
	FirstAttemptTimeoutMs int64

	// RetryTimeoutMs is the SET LOCAL statement_timeout for the retry attempt
	// that runs with the GIN planner override. Should be ≤ upstream client
	// timeout minus FirstAttemptTimeoutMs. Default 40000 ms.
	RetryTimeoutMs int64
}

// isStatementTimeout returns true when err carries SQLSTATE 57014
// (query_canceled due to statement_timeout). ResolveError leaves this code
// untouched so errors.As still reaches the underlying *pgconn.PgError.
func isStatementTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgerrcode.QueryCanceled
}

type Store struct {
	db     bun.IDB
	bucket bucket.Bucket
	ledger ledger.Ledger

	// aloneInBucket is a shared optimization hint (per bucket) indicating whether
	// this ledger is the only one in its bucket. The pointer is shared across all
	// stores in the same bucket via the Factory, so updating it from any store
	// (e.g. when a new ledger is created) immediately affects all stores.
	aloneInBucket *atomic.Bool

	// txListConfig carries the optional planner overrides for the transactions-list
	// SELECT path. See TransactionListConfig for the full rationale.
	txListConfig TransactionListConfig

	tracer                             trace.Tracer
	meter                              metric.Meter
	checkBucketSchemaHistogram         metric.Int64Histogram
	checkLedgerSchemaHistogram         metric.Int64Histogram
	updateAccountsMetadataHistogram    metric.Int64Histogram
	deleteAccountMetadataHistogram     metric.Int64Histogram
	upsertAccountsHistogram            metric.Int64Histogram
	getBalancesHistogram               metric.Int64Histogram
	insertLogHistogram                 metric.Int64Histogram
	readLogWithIdempotencyKeyHistogram metric.Int64Histogram
	insertMovesHistogram               metric.Int64Histogram
	insertTransactionHistogram         metric.Int64Histogram
	revertTransactionHistogram         metric.Int64Histogram
	updateTransactionMetadataHistogram metric.Int64Histogram
	deleteTransactionMetadataHistogram metric.Int64Histogram
	updateBalancesHistogram            metric.Int64Histogram
	getVolumesWithBalancesHistogram    metric.Int64Histogram
	beginTXHistogram                   metric.Int64Histogram
	commitTXHistogram                  metric.Int64Histogram
	rollbackTXHistogram                metric.Int64Histogram

	// Adaptive-fallback observability. Incremented/recorded only when a
	// transactions-list probe attempt times out and triggers a retry.
	txListFallbackCounter        metric.Int64Counter   // total fallback events
	txListFirstAttemptDurationMs metric.Int64Histogram // probe duration on fallback
	txListRetryDurationMs        metric.Int64Histogram // retry duration on fallback

	// testHookBeforePaginateSelect is called inside paginateInTx after SET LOCAL
	// is issued but before the main SELECT. Nil in production. Tests set this to
	// inject artificial latency (e.g. SELECT pg_sleep(0.005)) so that
	// statement_timeout fires deterministically regardless of query execution speed.
	testHookBeforePaginateSelect func(context.Context, bun.Tx) error

	// indexedMetadataKeys is the subset of INDEXED_METADATA_KEYS that have been
	// confirmed to have a matching functional index in pg_indexes. Set by
	// ResolveIndexedMetadataKeys; nil means unresolved (falls back to the full
	// feature-flag list, which is the behaviour for direct test store construction).
	indexedMetadataKeys []string
	indexedKeysResolved bool
}

func (store *Store) Volumes() common.PaginatedResource[
	ledger.VolumesWithBalanceByAssetByAccount,
	ledger.GetVolumesOptions] {
	return common.NewPaginatedResourceRepository[
		ledger.VolumesWithBalanceByAssetByAccount,
		ledger.GetVolumesOptions,
	](&volumesResourceHandler{store: store}, "account", paginate.OrderAsc)
}

func (store *Store) AggregatedVolumes() common.Resource[ledger.AggregatedVolumes, ledger.GetAggregatedVolumesOptions] {
	return common.NewResourceRepository[ledger.AggregatedVolumes, ledger.GetAggregatedVolumesOptions](&aggregatedBalancesResourceRepositoryHandler{
		store: store,
	})
}

// transactionsBase returns the plain paginated repository for transactions with
// no planner overrides. Called by the adaptive paginator and by GetOne/Count
// which never need a plan override (no ORDER BY + LIMIT, planner naturally
// prefers GIN for bare JSONB predicates).
func (store *Store) transactionsBase() common.PaginatedResource[ledger.Transaction, any] {
	return common.NewPaginatedResourceRepository[ledger.Transaction, any](
		&transactionsResourceHandler{store: store}, "id", paginate.OrderDesc,
	)
}

// Transactions returns a PaginatedResource for transactions.
// When EnableAdaptiveFallback is true (the default), Paginate uses a
// probe-then-retry strategy to detect the pathological slow plan at runtime
// and recover within the same request. See transactionsAdaptivePaginator.
func (store *Store) Transactions() common.PaginatedResource[ledger.Transaction, any] {
	if !store.txListConfig.EnableAdaptiveFallback {
		return store.transactionsBase()
	}
	return &transactionsAdaptivePaginator{store: store}
}

// SetTestHookBeforePaginateSelect sets a hook invoked inside paginateInTx after
// SET LOCAL statements but before the main SELECT, sharing the same transaction.
// The hook is subject to the same statement_timeout as the SELECT, so setting it
// to SELECT pg_sleep(0.005) guarantees SQLSTATE 57014 fires within 1 ms regardless
// of actual query speed. For tests only; nil in production.
func (store *Store) SetTestHookBeforePaginateSelect(hook func(context.Context, bun.Tx) error) {
	store.testHookBeforePaginateSelect = hook
}

// transactionsAdaptivePaginator is the heart of the sparse-wallet mitigation.
//
// Problem recap: SELECT … ORDER BY id DESC LIMIT N with JSONB @> predicates
// leads Postgres to choose an Index Scan Backward. For sparse wallets (few
// matching rows scattered across a large id range) that scan inspects most of
// the table before accumulating N results — observed: ~50 s at production scale with sparse-wallet workloads.
// For dense wallets (recent rows all match) the same backward scan is fast;
// forcing GIN would hurt them.
//
// Strategy (exactly one retry, no loop):
//
//  1. Run the SELECT inside a read-only transaction with
//     SET LOCAL statement_timeout = FirstAttemptTimeoutMs.
//     Dense wallets finish well inside the budget → no fallback, no overhead.
//
//  2. If Postgres cancels with SQLSTATE 57014 AND the request context is still
//     alive (meaning our timeout fired, not the client disconnecting), roll back
//     and retry once with SET LOCAL enable_indexscan = off, giving the retry
//     its own SET LOCAL statement_timeout = RetryTimeoutMs.
//
//  3. Any other error, or a client-context cancellation, is returned as-is.
//
// The SET LOCAL statements are inside explicit transactions so they cannot leak
// to subsequent queries on the same pooled connection.
type transactionsAdaptivePaginator struct {
	store *Store
}

// GetOne delegates straight to the base repository — no adaptive logic needed.
func (a *transactionsAdaptivePaginator) GetOne(ctx context.Context, q common.ResourceQuery[any]) (*ledger.Transaction, error) {
	return a.store.transactionsBase().GetOne(ctx, q)
}

// Count delegates straight to the base repository — no adaptive logic needed.
func (a *transactionsAdaptivePaginator) Count(ctx context.Context, q common.ResourceQuery[any]) (int, error) {
	return a.store.transactionsBase().Count(ctx, q)
}

// Paginate runs the adaptive probe-then-retry logic described on the type.
func (a *transactionsAdaptivePaginator) Paginate(
	ctx context.Context,
	q common.PaginatedQuery[any],
) (*paginate.Cursor[ledger.Transaction], error) {

	cfg := a.store.txListConfig

	// ── first attempt ──────────────────────────────────────────────────────
	probeStart := time.Now()
	cursor, err := a.paginateInTx(ctx, q, cfg.FirstAttemptTimeoutMs, false)
	if err == nil {
		return cursor, nil
	}

	// ── classify the error ─────────────────────────────────────────────────
	// Only retry when Postgres itself cancelled the query (SQLSTATE 57014 ==
	// our SET LOCAL statement_timeout fired) AND the client is still waiting
	// (ctx.Err() == nil). Any other error — including client disconnect — is
	// returned unchanged.
	if !isStatementTimeout(err) || ctx.Err() != nil {
		return nil, err
	}

	probeMs := time.Since(probeStart).Milliseconds()

	// ── observability ──────────────────────────────────────────────────────
	logging.FromContext(ctx).WithFields(map[string]any{
		"probe_duration_ms":        probeMs,
		"first_attempt_timeout_ms": cfg.FirstAttemptTimeoutMs,
	}).Errorf("transactions list probe timed out; retrying with GIN plan override")

	a.store.txListFallbackCounter.Add(ctx, 1)
	a.store.txListFirstAttemptDurationMs.Record(ctx, probeMs)

	// ── retry ──────────────────────────────────────────────────────────────
	retryStart := time.Now()
	cursor, err = a.paginateInTx(ctx, q, cfg.RetryTimeoutMs, true)
	retryMs := time.Since(retryStart).Milliseconds()

	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	a.store.txListRetryDurationMs.Record(ctx, retryMs,
		metric.WithAttributes(attribute.String("outcome", outcome)),
	)

	if err != nil {
		logging.FromContext(ctx).WithFields(map[string]any{
			"retry_duration_ms": retryMs,
			"outcome":           outcome,
		}).Errorf("transactions list retry with GIN override failed: %v", err)
	} else {
		logging.FromContext(ctx).WithFields(map[string]any{
			"retry_duration_ms": retryMs,
		}).Infof("transactions list retry with GIN override succeeded")
	}

	return cursor, err
}

// paginateInTx runs one Paginate attempt inside a dedicated read-only
// transaction. This ensures that the SET LOCAL statements it emits are strictly
// scoped to this SELECT and cannot leak to the next query on the same pooled
// connection when the transaction commits or rolls back.
//
// timeoutMs: value for SET LOCAL statement_timeout (0 = no timeout).
// planOverride: when true, also issues SET LOCAL enable_indexscan = off so
// Postgres is forced off the Index Scan Backward and onto the GIN bitmap path.
func (a *transactionsAdaptivePaginator) paginateInTx(
	ctx context.Context,
	q common.PaginatedQuery[any],
	timeoutMs int64,
	planOverride bool,
) (*paginate.Cursor[ledger.Transaction], error) {

	// If the caller is already inside a bun.Tx, skip the adaptive machinery
	// and delegate directly to the base paginator.  We cannot safely
	// probe-and-retry within someone else's transaction: SET LOCAL would scope
	// to the OUTER transaction, and a 57014 timeout would put that outer
	// transaction into an error state.  The caller would then receive a
	// spurious "current transaction is aborted" error on the retry attempt and
	// their transaction would be unusable.
	if _, ok := a.store.db.(bun.Tx); ok {
		return a.store.transactionsBase().Paginate(ctx, q)
	}

	var cursor *paginate.Cursor[ledger.Transaction]
	err := a.store.db.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		if err := a.issueSetLocal(ctx, tx, timeoutMs, planOverride); err != nil {
			return err
		}
		// testHookBeforePaginateSelect runs inside the same transaction as the
		// SELECT, so it is subject to the same SET LOCAL statement_timeout. In tests
		// this is set to SELECT pg_sleep(N) to force SQLSTATE 57014 regardless of
		// how fast the real query executes on the CI runner.
		if a.store.testHookBeforePaginateSelect != nil {
			if err := a.store.testHookBeforePaginateSelect(ctx, tx); err != nil {
				return err
			}
		}
		// Bind the base repository to this transaction connection so the SELECT
		// shares the connection on which SET LOCAL was issued.
		txStore := a.store.WithDB(tx)
		var err error
		cursor, err = txStore.transactionsBase().Paginate(ctx, q)
		return err
	})
	if err != nil {
		// Do NOT call postgres.ResolveError here: isStatementTimeout in the
		// caller needs to inspect the raw *pgconn.PgError. ResolveError leaves
		// 57014 untouched (it's not in its switch), so it would be safe, but
		// returning the raw error is clearer about intent.
		return nil, err
	}
	return cursor, nil
}

// issueSetLocal emits the configured SET LOCAL statements onto tx.
// Values are config-controlled integers, not user input, so direct interpolation
// is safe. Postgres interprets a bare integer for statement_timeout as ms.
func (a *transactionsAdaptivePaginator) issueSetLocal(
	ctx context.Context,
	tx bun.Tx,
	timeoutMs int64,
	planOverride bool,
) error {
	// statement_timeout first so that even a surprisingly slow GIN scan is
	// still bounded. Order doesn't affect correctness; this is logical sequencing.
	if timeoutMs > 0 {
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf("SET LOCAL statement_timeout = %d", timeoutMs),
		); err != nil {
			return fmt.Errorf("SET LOCAL statement_timeout: %w", err)
		}
	}
	if planOverride {
		// Disabling index scans forces the planner off the Index Scan Backward
		// and onto the GIN bitmap path for the JSONB @> predicates.
		// Bitmap index scans, sequential scans, and hash joins are unaffected.
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_indexscan = off"); err != nil {
			return fmt.Errorf("SET LOCAL enable_indexscan: %w", err)
		}
	}
	return nil
}

func (store *Store) Logs() common.PaginatedResource[
	ledger.Log,
	any] {
	return common.NewPaginatedResourceRepositoryMapper[ledger.Log, Log, any](&logsResourceHandler{
		store: store,
	}, "id", paginate.OrderDesc)
}

func (store *Store) Accounts() common.PaginatedResource[
	ledger.Account,
	any] {
	return common.NewPaginatedResourceRepository[ledger.Account, any](&accountsResourceHandler{
		store: store,
	}, "address", paginate.OrderAsc)
}

func (store *Store) Schemas() common.PaginatedResource[
	ledger.Schema,
	any] {
	return common.NewPaginatedResourceRepository[ledger.Schema, any](&schemasResourceHandler{
		store: store,
	}, "created_at", paginate.OrderDesc)
}

func (store *Store) BeginTX(ctx context.Context, options *sql.TxOptions) (*Store, *bun.Tx, error) {

	tx, err := tracing.TraceWithMetric(ctx, "BeginTX", store.tracer, store.beginTXHistogram, func(ctx context.Context) (bun.Tx, error) {
		return store.db.BeginTx(ctx, options)
	})
	if err != nil {
		return nil, nil, postgres.ResolveError(err)
	}
	cp := *store
	cp.db = tx

	return &cp, &tx, nil
}

func (store *Store) Commit(ctx context.Context) error {
	switch db := store.db.(type) {
	case bun.Tx:
		_, err := tracing.TraceWithMetric(ctx, "Commit", store.tracer, store.commitTXHistogram, tracing.NoResult(func(ctx context.Context) error {
			return db.Commit()
		}))
		return err
	default:
		return errors.New("cannot commit transaction: not in a transaction")
	}
}

func (store *Store) Rollback(ctx context.Context) error {
	switch db := store.db.(type) {
	case bun.Tx:
		_, err := tracing.TraceWithMetric(ctx, "Rollback", store.tracer, store.rollbackTXHistogram, tracing.NoResult(func(ctx context.Context) error {
			return db.Rollback()
		}))
		return err
	default:
		return errors.New("cannot rollback transaction: not in a transaction")
	}
}

func (store *Store) GetLedger() ledger.Ledger {
	return store.ledger
}

// IndexedMetadataKeys returns the set of metadata keys for which the query
// builder should use the functional-index rewrite (metadata ->> 'key' = ?).
// After ResolveIndexedMetadataKeys has been called this is the pg_indexes-
// confirmed subset; before that call it falls back to the raw feature flag
// list (used in direct test construction where no driver is involved).
func (store *Store) IndexedMetadataKeys() []string {
	if store.indexedKeysResolved {
		return store.indexedMetadataKeys
	}
	return store.ledger.GetIndexedMetadataKeys()
}

// ResolveIndexedMetadataKeys validates each key listed in the ledger's
// INDEXED_METADATA_KEYS feature against pg_indexes and retains only those that
// have a matching functional index. Keys without an index fall back silently to
// the @> containment predicate; a warning is logged for each missing index.
// Call this once after the store is created (the driver does this automatically).
func (store *Store) ResolveIndexedMetadataKeys(ctx context.Context) error {
	store.indexedKeysResolved = true
	requested := store.ledger.GetIndexedMetadataKeys()
	if len(requested) == 0 {
		return nil
	}

	schema := store.ledger.Bucket
	confirmed := make([]string, 0, len(requested))
	for _, key := range requested {
		var count int
		// Check pg_indexes for an index whose definition contains the functional
		// expression for this key. Escape LIKE metacharacters so underscores and
		// percent signs in key names match literally.
		escapedKey := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`).Replace(key)
		err := store.db.NewSelect().
			TableExpr("pg_indexes").
			ColumnExpr("COUNT(*)").
			Where("schemaname = ?", schema).
			Where("tablename = ?", "transactions").
			Where("indexdef LIKE ? ESCAPE '\\'", "%metadata ->> '"+escapedKey+"'%").
			Scan(ctx, &count)
		if err != nil {
			return fmt.Errorf("checking pg_indexes for key %q: %w", key, err)
		}
		if count > 0 {
			confirmed = append(confirmed, key)
		} else {
			logging.FromContext(ctx).WithFields(map[string]any{
				"key":    key,
				"ledger": store.ledger.Name,
			}).Infof("INDEXED_METADATA_KEYS: no functional index found for key — rewrite disabled, falling back to @>")
		}
	}
	store.indexedMetadataKeys = confirmed
	return nil
}

func (store *Store) GetDB() bun.IDB {
	return store.db
}

func (store *Store) GetBucket() bucket.Bucket {
	return store.bucket
}

func (store *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, store.ledger.Bucket, v)
}

func (store *Store) LockLedger(ctx context.Context) (*Store, bun.IDB, func() error, error) {
	storeCp := *store
	switch db := store.db.(type) {
	case *bun.DB:
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, nil, nil, err
		}

		_, err = conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext(?))`, fmt.Sprintf("ledger:%d", store.ledger.ID))
		if err != nil {
			_ = conn.Close()
			return nil, nil, nil, err
		}
		storeCp.db = conn

		return &storeCp, storeCp.db, func() error {
			_, err := conn.ExecContext(ctx, `SELECT pg_advisory_unlock(hashtext(?))`, fmt.Sprintf("ledger:%d", store.ledger.ID))
			if err != nil {
				return err
			}
			return conn.Close()
		}, nil
	case bun.Tx:
		_, err := db.ExecContext(ctx, `SELECT pg_advisory_xact_lock(hashtext(?))`, fmt.Sprintf("ledger:%d", store.ledger.ID))
		if err != nil {
			return nil, nil, nil, err
		}

		return store, db, func() error {
			// xact-scoped advisory locks are released automatically – nothing to do
			return nil
		}, nil
	default:
		panic(fmt.Errorf("invalid db type: %T", store.db))
	}
}

// newScopedSelect creates a new select query scoped to the current ledger.
// When the ledger is alone in its bucket, we skip the WHERE clause to avoid
// a degraded seq scan plan (selectivity ~100%). Otherwise, we filter by ledger
// name to use the composite index (ledger, id) efficiently.
//
// This relies on aloneInBucket being up to date (shared across the bucket).
func (store *Store) newScopedSelect() *bun.SelectQuery {
	q := store.db.NewSelect()
	if store.aloneInBucket == nil || !store.aloneInBucket.Load() {
		q = q.Where("ledger = ?", store.ledger.Name)
	}
	return q
}

func (store *Store) SetAloneInBucket(alone bool) {
	if store.aloneInBucket != nil {
		store.aloneInBucket.Store(alone)
	}
}

func New(db bun.IDB, bucket bucket.Bucket, l ledger.Ledger, opts ...Option) *Store {
	ret := &Store{
		db:     db,
		ledger: l,
		bucket: bucket,
	}
	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	var err error
	ret.beginTXHistogram, err = ret.meter.Int64Histogram("store.begin_tx")
	if err != nil {
		panic(err)
	}

	ret.commitTXHistogram, err = ret.meter.Int64Histogram("store.commit_tx")
	if err != nil {
		panic(err)
	}

	ret.rollbackTXHistogram, err = ret.meter.Int64Histogram("store.rollback_tx")
	if err != nil {
		panic(err)
	}

	ret.checkBucketSchemaHistogram, err = ret.meter.Int64Histogram("store.check_bucket_schema", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.checkLedgerSchemaHistogram, err = ret.meter.Int64Histogram("store.check_ledger_schema", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.updateAccountsMetadataHistogram, err = ret.meter.Int64Histogram("store.update_accounts_metadata", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.deleteAccountMetadataHistogram, err = ret.meter.Int64Histogram("store.delete_account_metadata", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.upsertAccountsHistogram, err = ret.meter.Int64Histogram("store.upsert_accounts", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.getBalancesHistogram, err = ret.meter.Int64Histogram("store.get_balances", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.insertLogHistogram, err = ret.meter.Int64Histogram("store.insert_log", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.readLogWithIdempotencyKeyHistogram, err = ret.meter.Int64Histogram("store.read_log_with_idempotency_key", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.insertMovesHistogram, err = ret.meter.Int64Histogram("store.insert_moves", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.insertTransactionHistogram, err = ret.meter.Int64Histogram("store.insert_transaction", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.revertTransactionHistogram, err = ret.meter.Int64Histogram("store.revert_transaction", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.updateTransactionMetadataHistogram, err = ret.meter.Int64Histogram("store.update_transaction_metadata", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.deleteTransactionMetadataHistogram, err = ret.meter.Int64Histogram("store.delete_transaction_metadata", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.updateBalancesHistogram, err = ret.meter.Int64Histogram("store.update_balances", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	ret.getVolumesWithBalancesHistogram, err = ret.meter.Int64Histogram("store.get_volumes_with_balances", metric.WithUnit("ms"))
	if err != nil {
		panic(err)
	}

	// Adaptive-fallback metrics. These are only incremented/recorded when a
	// probe attempt times out and triggers a retry, so they are effectively
	// zero-cost on deployments that never hit the sparse-wallet path.
	ret.txListFallbackCounter, err = ret.meter.Int64Counter("store.tx_list_fallback_total",
		metric.WithDescription("Number of transactions-list queries that triggered the adaptive GIN fallback"))
	if err != nil {
		panic(err)
	}
	ret.txListFirstAttemptDurationMs, err = ret.meter.Int64Histogram("store.tx_list_first_attempt_duration",
		metric.WithUnit("ms"),
		metric.WithDescription("Duration of the probe attempt that triggered the fallback"))
	if err != nil {
		panic(err)
	}
	ret.txListRetryDurationMs, err = ret.meter.Int64Histogram("store.tx_list_retry_duration",
		metric.WithUnit("ms"),
		metric.WithDescription("Duration of the retry attempt after the fallback was triggered"))
	if err != nil {
		panic(err)
	}

	return ret
}

func (store *Store) HasMinimalVersion(ctx context.Context) (bool, error) {
	return store.bucket.HasMinimalVersion(ctx, store.db)
}

func (store *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return store.bucket.GetMigrationsInfo(ctx, store.db)
}

func (store *Store) WithDB(db bun.IDB) *Store {
	ret := *store
	ret.db = db

	return &ret
}

type Option func(s *Store)

func WithMeter(meter metric.Meter) Option {
	return func(s *Store) {
		s.meter = meter
	}
}

func WithTracer(tracer trace.Tracer) Option {
	return func(s *Store) {
		s.tracer = tracer
	}
}

// DefaultTransactionListConfig returns a TransactionListConfig with the
// production-safe defaults: fallback enabled, 5 s probe, 40 s retry.
func DefaultTransactionListConfig() TransactionListConfig {
	return TransactionListConfig{
		EnableAdaptiveFallback: true,
		FirstAttemptTimeoutMs:  5_000,
		RetryTimeoutMs:         40_000,
	}
}

// WithTransactionListConfig wires the adaptive-fallback configuration for the
// transactions-list SELECT path. Use DefaultTransactionListConfig() to get the
// recommended starting values, then override individual fields as needed.
func WithTransactionListConfig(cfg TransactionListConfig) Option {
	return func(s *Store) {
		s.txListConfig = cfg
	}
}

var defaultOptions = []Option{
	WithMeter(noopmetrics.Meter{}),
	WithTracer(nooptracer.Tracer{}),
}
