package ledger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/uptrace/bun"
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

// TransactionListConfig configures the hedged-request strategy for the
// transactions-list SELECT.
//
// Background: with ORDER BY id DESC LIMIT N and JSONB @> predicates, Postgres
// tends to choose an Index Scan Backward on the id B-tree. For a wallet whose
// matching rows are sparse or old that scan walks a large fraction of the table
// before collecting N results (observed: ~50 s at production scale with sparse wallets).
// For a wallet whose matches are dense/recent the backward scan is the FAST
// plan; forcing GIN bitmap scan instead would hurt it.
//
// The hedged request solves the dilemma without penalising either case:
//   - Run the original query with no timeout and no plan override.
//   - After ChaserDelayMs, if the original hasn't returned, fire a "chaser"
//     query in parallel with SET LOCAL enable_indexscan = off (forcing GIN).
//   - Return whichever query finishes first, cancel the other.
//   - Dense wallets finish well within the delay and never trigger a chaser.
//   - Sparse wallets get rescued by the chaser without killing and restarting
//     a query that was 90% done.
//
// This is a stopgap. The real fix is a composite/denormalised index that serves
// both the wallet filter and the id ordering without a full sort step.
type TransactionListConfig struct {
	// EnableAdaptiveFallback turns on the hedged-request strategy described
	// above. Default true — when no chaser fires (dense wallets, fast queries)
	// the only overhead is the explicit read-only transaction wrapping.
	EnableAdaptiveFallback bool

	// ChaserDelayMs is the delay in milliseconds before firing the chaser
	// query. If the original finishes within this budget the chaser never
	// fires. Default 5000 ms.
	ChaserDelayMs int64

	// ChaserTimeoutMs is the SET LOCAL statement_timeout for the chaser query
	// (milliseconds). The original query has no timeout. Default 40000 ms.
	ChaserTimeoutMs int64
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

	// Hedged-request observability. Incremented only when a chaser query
	// fires (original slower than ChaserDelayMs) or wins the race.
	txListChaserFiredCounter metric.Int64Counter // chaser was launched
	txListChaserWonCounter   metric.Int64Counter // chaser beat the original

	// testHookBeforePaginateSelect is called inside paginateInTx after SET LOCAL
	// is issued but before the main SELECT. Nil in production. Tests set this to
	// inject artificial latency (e.g. SELECT pg_sleep(0.005)) so that
	// statement_timeout fires deterministically regardless of query execution speed.
	// Stored as an atomic.Value to avoid a data race between
	// SetTestHookBeforePaginateSelect and the read on the Paginate hot path.
	// The underlying type, when non-nil, is paginateSelectHookWrapper.
	testHookBeforePaginateSelect atomic.Value

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
// When EnableAdaptiveFallback is true (the default), Paginate uses a hedged-
// request strategy: the original query races against a delayed chaser with a
// GIN plan override. See transactionsAdaptivePaginator.
func (store *Store) Transactions() common.PaginatedResource[ledger.Transaction, any] {
	if !store.txListConfig.EnableAdaptiveFallback {
		return store.transactionsBase()
	}
	return &transactionsAdaptivePaginator{store: store}
}

// paginateSelectHookWrapper wraps a test hook function so it can be stored in
// an atomic.Value (which requires a concrete type for the first Store).
type paginateSelectHookWrapper struct {
	fn func(ctx context.Context, tx bun.Tx, isChaser bool) error
}

// SetTestHookBeforePaginateSelect sets a hook invoked inside paginateInTx after
// SET LOCAL statements but before the main SELECT, sharing the same transaction.
// The isChaser parameter is true when the call is from the chaser goroutine
// (planOverride=true) and false for the original. For tests only; nil in production.
func (store *Store) SetTestHookBeforePaginateSelect(hook func(ctx context.Context, tx bun.Tx, isChaser bool) error) {
	store.testHookBeforePaginateSelect.Store(paginateSelectHookWrapper{fn: hook})
}

// transactionsAdaptivePaginator is the heart of the sparse-wallet mitigation.
//
// Problem recap: SELECT … ORDER BY id DESC LIMIT N with JSONB @> predicates
// leads Postgres to choose an Index Scan Backward. For sparse wallets (few
// matching rows scattered across a large id range) that scan inspects most of
// the table before accumulating N results — observed: ~50 s at production scale.
// For dense wallets (recent rows all match) the same backward scan is fast;
// forcing GIN would hurt them.
//
// Strategy (hedged request — race original vs chaser):
//
//  1. Fire the original query with no timeout, no plan override.
//
//  2. After ChaserDelayMs, if the original hasn't returned, fire a "chaser"
//     query in a second transaction with SET LOCAL enable_indexscan = off and
//     SET LOCAL statement_timeout = ChaserTimeoutMs.
//
//  3. Return whichever query finishes first; cancel the other via context.
//
//  4. Dense wallets finish well within the delay — no chaser, no overhead
//     beyond the explicit transaction wrapping.
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

// Paginate runs the hedged-request logic described on the type.
func (a *transactionsAdaptivePaginator) Paginate(
	ctx context.Context,
	q common.PaginatedQuery[any],
) (*paginate.Cursor[ledger.Transaction], error) {

	// If the caller is already inside a bun.Tx, skip the hedging machinery
	// and delegate directly to the base paginator. We cannot safely open
	// nested transactions or race goroutines within someone else's transaction.
	if _, ok := a.store.db.(bun.Tx); ok {
		return a.store.transactionsBase().Paginate(ctx, q)
	}

	cfg := a.store.txListConfig

	type raceResult struct {
		cursor *paginate.Cursor[ledger.Transaction]
		err    error
		source string // "original" or "chaser"
	}

	raceCtx, raceCancel := context.WithCancel(ctx)
	defer raceCancel()

	ch := make(chan raceResult, 2)

	// ── original query ─────────────────────────────────────────────────────
	// No timeout, no plan override — let Postgres use whichever plan it picks.
	go func() {
		cursor, err := a.paginateInTx(raceCtx, q, 0, false)
		ch <- raceResult{cursor, err, "original"}
	}()

	// ── chaser query ───────────────────────────────────────────────────────
	// Fires after ChaserDelayMs with GIN override. If the original finishes
	// before the delay, the chaser goroutine sees raceCtx.Done() and exits.
	go func() {
		delay := time.Duration(cfg.ChaserDelayMs) * time.Millisecond
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			a.store.txListChaserFiredCounter.Add(ctx, 1)
			logging.FromContext(ctx).WithFields(map[string]any{
				"chaser_delay_ms": cfg.ChaserDelayMs,
			}).Infof("transactions list chaser fired")
			cursor, err := a.paginateInTx(raceCtx, q, cfg.ChaserTimeoutMs, true)
			ch <- raceResult{cursor, err, "chaser"}
		case <-raceCtx.Done():
			ch <- raceResult{nil, raceCtx.Err(), "chaser"}
		}
	}()

	// ── drain both results ─────────────────────────────────────────────────
	var origErr error
	for i := 0; i < 2; i++ {
		r := <-ch
		if r.err == nil {
			raceCancel()
			if r.source == "chaser" {
				a.store.txListChaserWonCounter.Add(ctx, 1)
				logging.FromContext(ctx).Infof("transactions list chaser won the race (GIN override)")
			}
			return r.cursor, nil
		}
		if r.source == "original" {
			origErr = r.err
		}
	}

	// Both failed — return the original's error as it's most informative.
	if origErr != nil {
		return nil, origErr
	}
	return nil, fmt.Errorf("both original and chaser queries failed")
}

// paginateInTx runs one query attempt inside a dedicated read-only
// transaction. This ensures that the SET LOCAL statements it emits are strictly
// scoped to this SELECT and cannot leak to the next query on the same pooled
// connection when the transaction commits or rolls back.
//
// timeoutMs: value for SET LOCAL statement_timeout (0 = no timeout).
// planOverride: when true, also issues SET LOCAL enable_indexscan = off so
// Postgres is forced off the Index Scan Backward and onto the GIN bitmap path.
// In the hedged pattern, planOverride=true identifies the chaser query.
func (a *transactionsAdaptivePaginator) paginateInTx(
	ctx context.Context,
	q common.PaginatedQuery[any],
	timeoutMs int64,
	planOverride bool,
) (*paginate.Cursor[ledger.Transaction], error) {

	var cursor *paginate.Cursor[ledger.Transaction]
	err := a.store.db.RunInTx(ctx, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx bun.Tx) error {
		if err := a.issueSetLocal(ctx, tx, timeoutMs, planOverride); err != nil {
			return err
		}
		if v := a.store.testHookBeforePaginateSelect.Load(); v != nil {
			if err := v.(paginateSelectHookWrapper).fn(ctx, tx, planOverride); err != nil {
				return err
			}
		}
		txStore := a.store.WithDB(tx)
		var err error
		cursor, err = txStore.transactionsBase().Paginate(ctx, q)
		return err
	})
	if err != nil {
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
	// Always issue SET LOCAL statement_timeout so that any inherited session or
	// role-level timeout is explicitly overridden. 0 = no timeout (disables
	// any inherited value); positive values set the probe/retry budget in ms.
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf("SET LOCAL statement_timeout = %d", timeoutMs),
	); err != nil {
		return fmt.Errorf("SET LOCAL statement_timeout: %w", err)
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
func (store *Store) ResolveIndexedMetadataKeys(ctx context.Context) {
	store.indexedKeysResolved = true
	requested := store.ledger.GetIndexedMetadataKeys()
	if len(requested) == 0 {
		return
	}

	schema := store.ledger.Bucket
	logger := logging.FromContext(ctx).WithFields(map[string]any{
		"ledger": store.ledger.Name,
	})
	confirmed := make([]string, 0, len(requested))
	escaper := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	for _, key := range requested {
		var count int
		escapedKey := escaper.Replace(key)
		escapedLedger := escaper.Replace(store.ledger.Name)
		err := store.db.NewSelect().
			TableExpr("pg_indexes").
			ColumnExpr("COUNT(*)").
			Where("schemaname = ?", schema).
			Where("tablename = ?", "transactions").
			Where("indexdef LIKE ? ESCAPE '\\'", "%metadata ->> '"+escapedKey+"'%").
			Where("(indexdef NOT LIKE '%WHERE%' ESCAPE '\\' OR indexdef LIKE ? ESCAPE '\\')", "%'"+escapedLedger+"'%").
			Scan(ctx, &count)
		if err != nil {
			logger.Errorf("INDEXED_METADATA_KEYS: pg_indexes query failed for key %q, all keys fall back to @>: %s", key, err)
			store.indexedMetadataKeys = nil
			return
		}
		if count > 0 {
			confirmed = append(confirmed, key)
		} else {
			logger.Infof("INDEXED_METADATA_KEYS: no functional index found for key %q — rewrite disabled, falling back to @>", key)
		}
	}
	store.indexedMetadataKeys = confirmed
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

	// Hedged-request metrics. Only incremented when the chaser fires or wins.
	ret.txListChaserFiredCounter, err = ret.meter.Int64Counter("store.tx_list_chaser_fired_total",
		metric.WithDescription("Number of transactions-list queries where the chaser query was launched"))
	if err != nil {
		panic(err)
	}
	ret.txListChaserWonCounter, err = ret.meter.Int64Counter("store.tx_list_chaser_won_total",
		metric.WithDescription("Number of transactions-list queries where the chaser beat the original"))
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
// production-safe defaults: hedging enabled, 5 s chaser delay, 40 s chaser timeout.
func DefaultTransactionListConfig() TransactionListConfig {
	return TransactionListConfig{
		EnableAdaptiveFallback: true,
		ChaserDelayMs:          5_000,
		ChaserTimeoutMs:        40_000,
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
