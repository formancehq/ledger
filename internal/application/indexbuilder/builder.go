package indexbuilder

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/metric"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// DefaultBatchSize is the default number of log entries per Pebble batch
// commit. Can be overridden via --read-index-batch-size.
const DefaultBatchSize = 1000

// Builder tails the system log and populates the Pebble read store indexes.
// It runs as a background goroutine on ALL nodes (not just the leader).
// Progress is stored locally in Pebble (no Raft needed). Index promotion
// (BUILDING → effectively READY) is fully local: backfill and rewrite
// tasks atomically flip IndexVersionState.CurrentVersion in their
// final batch — no cluster-wide IndexReady proposal is involved.
type Builder struct {
	pebbleStore   *dal.Store
	readStore     *readstore.Store
	attrs         *attributes.Attributes
	logger        logging.Logger
	meter         metric.Meter
	notifications *signal.Notifications
	w             worker.Worker

	lastIndexedSeq      atomic.Uint64
	pebbleLastSeq       atomic.Uint64
	logsIndexed         atomic.Uint64
	metricsRegistration metric.Registration

	// Per-ledger index configuration cache.
	indexConfig map[string]*ledgerIndexConfig

	// Bucket-scoped index configuration (Index.Ledger == "" in the registry).
	// Reserved for audit-style indexes (#436); nil until the first entry is
	// loaded so single-ledger deployments don't carry an unused cfg.
	bucketIndexConfig *ledgerIndexConfig

	// Active backfill tasks for BUILDING indexes.
	backfillTasks []*backfillTask

	// Active schema rewrite tasks for deferred SetMetadataFieldType processing.
	schemaRewriteTasks []*schemaRewriteTask

	// Batch size for normal index processing and backfill.
	batchSize int

	// Max time budget per tick for backfill processing (default 50ms).
	backfillBudget time.Duration

	// Round-robin index for fair scheduling across backfill tasks.
	nextBackfillIdx int

	// AppliedProposal sync for transient-account filtering.
	lastAppliedProposalSeq uint64

	// Reusable scratch objects to reduce allocations in the hot loop.
	kb       *dal.KeyBuilder
	wb       *readstore.WriteBatch
	accounts map[string]struct{}

	// seenAcctAsset deduplicates account-by-asset index writes within the
	// in-flight batch: it holds the AccountByAssetKey bytes (as string) already
	// written, so a repeated (account, assetBase, precision) cell in the same
	// batch skips a redundant Get + Put. Reset per batch (initBatch) so it does
	// not grow unbounded across a long backfill.
	seenAcctAsset map[string]struct{}

	// deletedThisBatch holds the names of ledgers whose read indexes were
	// range-deleted earlier in the in-flight batch (DeleteLedger). The
	// account-by-asset dedup must NOT consult committed state for these
	// ledgers: readstoreKeyExists reads committed Pebble directly and cannot
	// see the pending range delete, so a stale committed row would suppress the
	// recreated ledger's Put — which the range delete then wipes at commit,
	// silently dropping the row. Reset per batch (initBatch).
	deletedThisBatch map[string]struct{}

	// batchSchema is the per-batch memoization layer over FSM Pebble
	// LedgerInfo lookups. Set at the top of each indexer batch
	// (processLogs / processBackfill), reset to nil at the end via defer.
	// Accessed via b.coerceForLedger from the per-write hot path; no
	// concurrent access — the indexer loop is single-threaded.
	batchSchema *schemaResolver

	// indexVersions caches per-(ledger, canonicalID) forward encoding
	// state for this replica. Boot loads it from
	// readstore.ReadAllIndexVersionStates; the indexer mutates it when
	// a SetMetadataFieldType / CreateIndex log lands or when a local
	// rewrite finishes. Inner map is keyed by indexes.Canonical(id),
	// always interpreted as string(canonical) for map lookups.
	indexVersions map[string]map[string]readstore.IndexVersionState
}

// versionFor returns (current, pending) for an indexed (ledger, canonicalID).
// current == 0 means the index has not been built locally yet (no v_n
// keyspace populated). pending > 0 means a local rewrite to v_pending is
// in progress. Defaults: when the cache has no entry, current == 0 and
// pending == 0 — the caller (write/query path) typically promotes
// (0, 0) to "version 1" via effectiveCurrentVersion below because
// Index.ForwardEncodingVersion is initialised to 1 at CreateIndex /
// first SetMetadataFieldType apply.
func (b *Builder) versionFor(ledgerName, canonicalID string) (current uint32, pending uint32) {
	if b.indexVersions == nil {
		return 0, 0
	}

	inner, ok := b.indexVersions[ledgerName]
	if !ok {
		return 0, 0
	}

	state, ok := inner[canonicalID]
	if !ok {
		return 0, 0
	}

	return state.CurrentVersion, state.PendingVersion
}

// putVersionState writes a per-index version state into the in-memory
// cache. Persistence is the caller's responsibility (via
// readStore.WriteIndexVersionState in the current batch).
func (b *Builder) putVersionState(ledgerName, canonicalID string, state readstore.IndexVersionState) {
	if b.indexVersions == nil {
		b.indexVersions = make(map[string]map[string]readstore.IndexVersionState, 4)
	}

	inner, ok := b.indexVersions[ledgerName]
	if !ok {
		inner = make(map[string]readstore.IndexVersionState, 4)
		b.indexVersions[ledgerName] = inner
	}

	inner[canonicalID] = state
}

// dropVersionState removes a per-index version state from the cache —
// used when an index is dropped via RemoveMetadataFieldType / DropIndex.
func (b *Builder) dropVersionState(ledgerName, canonicalID string) {
	if b.indexVersions == nil {
		return
	}

	inner, ok := b.indexVersions[ledgerName]
	if !ok {
		return
	}

	delete(inner, canonicalID)
}

// effectiveCurrentVersion returns the forward-encoding version live
// writes and queries should currently target on this replica. The
// indexer hot path calls this for every metadata index touched.
//
// Promotion of 0 → 1: a never-seen index defaults to v1, matching the
// FSM-side initialisation in processCreateIndex (and the version=1
// embedded by the non-V key helpers). The actual switch to higher
// versions happens in the rewrite-completion path (atomicSwitch).
func (b *Builder) effectiveCurrentVersion(ledgerName, canonicalID string) uint32 {
	current, _ := b.versionFor(ledgerName, canonicalID)
	if current == 0 {
		return 1
	}

	return current
}

// pendingVersion returns the in-flight rewrite target for an index on
// this replica, or 0 when no rewrite is running. Live writes consult
// it to decide whether to dual-write.
func (b *Builder) pendingVersion(ledgerName, canonicalID string) uint32 {
	_, pending := b.versionFor(ledgerName, canonicalID)

	return pending
}

// metadataIndexVersions returns the (current, pending) encoding versions a
// live write must target for a metadata index on (ledger, target, key).
// current is always >= 1 — effectiveCurrentVersion promotes the
// never-built-yet 0 to 1 to match the non-versioned key helpers.
// pending == 0 means no rewrite is in flight.
func (b *Builder) metadataIndexVersions(ledger string, target commonpb.TargetType, key string) (current uint32, pending uint32) {
	canonical := indexes.Canonical(indexes.MetadataID(target, key))

	return b.effectiveCurrentVersion(ledger, canonical), b.pendingVersion(ledger, canonical)
}

// reverseKeyForVersion is a typed adapter to the namespace-specific
// AccountReverseMapKeyV / TransactionReverseMapKeyV helpers. The
// dual-write code below takes a function value of this shape so it can
// stay namespace-agnostic.
type reverseKeyForVersion func(version uint32) []byte

// dualWriteMetadataIndex applies a metadata index write to v_current on
// this replica, plus a mirrored write at v_pending when a rewrite is
// in flight. rmapKeyAtVersion returns the reverse-map key the namespace
// uses for a given version.
//
// On the encoding side, the value (newEncoded) is identical across
// versions — the live path always coerces to the *current* declared
// type. The rmap rows differ only by the version embedded in the key;
// having distinct rows means a query at v_pending sees only entries
// that were either backfilled by the rewrite OR mirrored here. Once
// the rewrite finishes and the atomic switch flips current←pending,
// the v_pending rows become the authoritative view.
func (b *Builder) dualWriteMetadataIndex(
	kb *dal.KeyBuilder,
	ledger, ns, metaKey string,
	target commonpb.TargetType,
	newEncoded, entityID []byte,
	rmapKeyAtVersion reverseKeyForVersion,
) error {
	current, pending := b.metadataIndexVersions(ledger, target, metaKey)

	if err := b.writeMetadataIndexAtVersion(kb, ledger, ns, metaKey, current, newEncoded, entityID, rmapKeyAtVersion(current)); err != nil {
		return err
	}

	if pending != 0 && pending != current {
		if err := b.writeMetadataIndexAtVersion(kb, ledger, ns, metaKey, pending, newEncoded, entityID, rmapKeyAtVersion(pending)); err != nil {
			return err
		}
	}

	return nil
}

// writeMetadataIndexAtVersion resolves the version-scoped reverse-map
// value (committed state + uncommitted overlay) and replaces the
// metadata index entry at the given version. Centralises the
// reverseMapValue lookup so the dual-write loop never reads from the
// wrong rmap row.
func (b *Builder) writeMetadataIndexAtVersion(
	kb *dal.KeyBuilder,
	ledger, ns, metaKey string,
	version uint32,
	newEncoded, entityID, reverseKey []byte,
) error {
	oldEncoded, err := b.reverseMapValue(reverseKey)
	if err != nil {
		return err
	}

	return b.wb.ReplaceMetadataIndexV(kb, reverseKey, ledger, ns, metaKey, version, newEncoded, oldEncoded, entityID)
}

// dualDeleteMetadataEntry mirrors dualWriteMetadataIndex for delete
// operations (DeletedMetadata logs). Same shape: delete at v_current,
// and at v_pending too when a rewrite is in flight.
func (b *Builder) dualDeleteMetadataEntry(
	kb *dal.KeyBuilder,
	ledger, ns, metaKey string,
	target commonpb.TargetType,
	entityID []byte,
	rmapKeyAtVersion reverseKeyForVersion,
) error {
	current, pending := b.metadataIndexVersions(ledger, target, metaKey)

	if err := b.deleteMetadataEntryAtVersion(kb, ledger, ns, metaKey, current, entityID, rmapKeyAtVersion(current)); err != nil {
		return err
	}

	if pending != 0 && pending != current {
		if err := b.deleteMetadataEntryAtVersion(kb, ledger, ns, metaKey, pending, entityID, rmapKeyAtVersion(pending)); err != nil {
			return err
		}
	}

	return nil
}

func (b *Builder) deleteMetadataEntryAtVersion(
	kb *dal.KeyBuilder,
	ledger, ns, metaKey string,
	version uint32,
	entityID, reverseKey []byte,
) error {
	oldEncoded, err := b.reverseMapValue(reverseKey)
	if err != nil {
		return err
	}

	return b.wb.DeleteMetadataEntryWithPreviousV(kb, reverseKey, ledger, ns, metaKey, version, oldEncoded, entityID)
}

// coerceForLedger looks up the declared type for (ledger, target, key) via
// the current batch's schema resolver and returns v coerced to that type.
// Returns an error on schema lookup failure (Pebble I/O error, unmarshal
// failure): the caller MUST propagate it to abort the batch — silently
// encoding the entry under the raw client type tag would commit miscoded
// data to the forward index that no downstream path repairs.
//
// When b.batchSchema is nil (caller forgot to seed it), this is a
// programming error rather than a runtime condition; we panic to surface
// the missing setup loudly per CLAUDE.md invariant #7. Tests that exercise
// indexer write helpers directly must seed b.batchSchema explicitly.
func (b *Builder) coerceForLedger(ledger string, target commonpb.TargetType, key string, v *commonpb.MetadataValue) (*commonpb.MetadataValue, error) {
	if b.batchSchema == nil {
		panic("indexbuilder: coerceForLedger called outside an active batch (batchSchema not seeded)")
	}

	return b.batchSchema.coerceFor(ledger, target, key, v)
}

// NewBuilder creates a new index builder.
// batchSize controls how many log entries are buffered per Pebble batch commit.
// Use 0 for the default (DefaultBatchSize).
func NewBuilder(
	pebbleStore *dal.Store,
	readStore *readstore.Store,
	attrs *attributes.Attributes,
	logger logging.Logger,
	meter metric.Meter,
	batchSize int,
) *Builder {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize
	}

	return &Builder{
		pebbleStore:    pebbleStore,
		readStore:      readStore,
		attrs:          attrs,
		logger:         logger.WithFields(map[string]any{"cmp": "index-builder"}),
		meter:          meter,
		batchSize:      batchSize,
		backfillBudget: 50 * time.Millisecond,
		indexConfig:    make(map[string]*ledgerIndexConfig),
		kb:             dal.NewKeyBuilder(),
		wb:             readstore.NewWriteBatch(),
		accounts:       make(map[string]struct{}, 64),
	}
}

// initBatch binds the WriteBatch to a fresh dal.WriteSession and resets the
// per-batch account-by-asset dedup set. This is the single place a batch is
// bound for index processing, so the dedup set can never be left stale (or
// grow unbounded) relative to the batch it tracks.
func (b *Builder) initBatch(batch *dal.WriteSession) {
	b.wb.Init(batch)
	b.seenAcctAsset = make(map[string]struct{})
	b.deletedThisBatch = make(map[string]struct{})
}

// SetNotifications sets the dedicated Notifications signal for the builder.
func (b *Builder) SetNotifications(n *signal.Notifications) {
	b.notifications = n
}

// Start begins the background index-building loop and registers OTEL metrics.
func (b *Builder) Start() {
	if reg, err := b.registerMetrics(); err == nil {
		b.metricsRegistration = reg
	}

	b.w = worker.New()
	b.w.RunCtx(b.loop)
}

// Stop gracefully stops the background loop and unregisters OTEL metrics.
func (b *Builder) Stop() {
	b.w.Stop()

	if b.metricsRegistration != nil {
		_ = b.metricsRegistration.Unregister()
	}
}

// LastIndexedSequence returns the last indexed sequence (from the atomic cache).
func (b *Builder) LastIndexedSequence() uint64 {
	return b.lastIndexedSeq.Load()
}

// PebbleLastSequence returns the last known Pebble sequence (from the atomic cache).
func (b *Builder) PebbleLastSequence() uint64 {
	return b.pebbleLastSeq.Load()
}

// registerMetrics registers observable gauges for index builder progress.
func (b *Builder) registerMetrics() (metric.Registration, error) {
	lastIndexedGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.last_indexed_sequence",
		metric.WithDescription("Last log sequence indexed in Pebble read store"),
	)
	if err != nil {
		return nil, err
	}

	pebbleLastGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.pebble_last_sequence",
		metric.WithDescription("Last log sequence in Pebble"),
	)
	if err != nil {
		return nil, err
	}

	lagGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.lag",
		metric.WithDescription("Number of logs the index builder is behind Pebble"),
	)
	if err != nil {
		return nil, err
	}

	logsIndexedGauge, err := b.meter.Int64ObservableGauge(
		"index.builder.logs_indexed_total",
		metric.WithDescription("Total number of logs indexed since process start"),
	)
	if err != nil {
		return nil, err
	}

	return b.meter.RegisterCallback(
		func(_ context.Context, o metric.Observer) error {
			indexed := int64(b.lastIndexedSeq.Load())
			pebbleLast := int64(b.pebbleLastSeq.Load())

			lag := max(pebbleLast-indexed, 0)

			o.ObserveInt64(lastIndexedGauge, indexed)
			o.ObserveInt64(pebbleLastGauge, pebbleLast)
			o.ObserveInt64(lagGauge, lag)
			o.ObserveInt64(logsIndexedGauge, int64(b.logsIndexed.Load()))

			return nil
		},
		lastIndexedGauge,
		pebbleLastGauge,
		lagGauge,
		logsIndexedGauge,
	)
}

func (b *Builder) loop(ctx context.Context) {
	// ctx is supplied by Worker.RunCtx and is cancelled by Stop(). It
	// flows to all blocking operations (Pebble reads, iterators) so they
	// are interrupted promptly on shutdown instead of blocking until the
	// operation completes naturally. For internal helpers that still
	// take a stop <-chan struct{}, ctx.Done() is passed at the boundary
	// (same signal, same semantics).
	stop := ctx.Done()

	// Boot init: rebuild the index-config cache and seed cursors. Any
	// transient Pebble/read-store failure here must NOT advance the
	// persisted cursor against an incomplete config, so retry with
	// backoff until it succeeds or shutdown is requested. initIndexConfig
	// resets its own state, so re-running it on retry is idempotent.
	var (
		cursor     uint64
		pebbleLast uint64
		err        error
	)
	worker.RetryWithBackoff(stop, b.logger, func() error {
		cursor, pebbleLast, err = b.bootInit(ctx)

		return err
	})

	// RetryWithBackoff returns only on success or when stop is closed. If
	// the context was cancelled we never got a good init — return without
	// processing any log.
	if ctx.Err() != nil {
		return
	}

	b.lastIndexedSeq.Store(cursor)
	b.pebbleLastSeq.Store(pebbleLast)

	b.logger.WithFields(map[string]any{
		"cursor":     cursor,
		"pebbleLast": pebbleLast,
		"gap":        int64(pebbleLast) - int64(cursor),
		"backfills":  len(b.backfillTasks),
	}).Infof("Index builder started")

	// Initial catch-up: process all pending logs before entering the main loop.
	// Use a larger batch size to reduce fsync overhead, and strip BUILDING
	// indexes from the config since their ranges will be covered by backfill
	// tasks. This avoids millions of redundant Pebble writes.
	//
	// The catch-up is split into time-bounded iterations (catchUpBudget) so
	// that the Pebble ReadHandle (snapshot) is released between iterations.
	// Without this, a single unbounded processLogs call holds a snapshot for
	// the entire catch-up duration (potentially hours on large stores),
	// preventing Pebble from garbage-collecting obsolete SSTs and causing
	// zombie files and pinned keys to accumulate.
	const catchUpBudget = 5 * time.Second
	prevCursor := cursor
	savedBatchSize := b.batchSize
	b.batchSize = max(b.batchSize, 10_000)
	restoreIndexes := b.stripBuildingIndexes()

	for {
		select {
		case <-ctx.Done():
			restoreIndexes()
			b.batchSize = savedBatchSize

			return
		default:
		}

		before := cursor
		deadline := time.Now().Add(catchUpBudget)

		if cursor, err = b.processLogs(ctx, cursor, deadline); err != nil {
			b.logger.Errorf("Error during initial catch-up: %v", err)

			break
		}

		if cursor == before {
			break
		}
	}

	restoreIndexes()
	b.batchSize = savedBatchSize

	if cursor > prevCursor {
		b.logger.WithFields(map[string]any{
			"from": prevCursor,
			"to":   cursor,
			"logs": cursor - prevCursor,
		}).Infof("Initial catch-up complete")
	}

	b.processBackgroundTasks(ctx, stop, cursor)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			b.logger.Infof("Index builder stopped")

			return
		case <-b.notifications.LogCommitted.C():
		case <-ticker.C:
		}

		// Fast path: skip Pebble iterator + batch commit when the FSM
		// hasn't advanced past our cursor.
		logsProcessed := false

		if cached := b.notifications.LastSequence.Load(); cached == 0 || cached > cursor {
			// When background tasks are active, cap normal processing so they
			// get their fair share of each tick.
			var logDeadline time.Time
			if len(b.backfillTasks) > 0 || len(b.schemaRewriteTasks) > 0 {
				logDeadline = time.Now().Add(b.backfillBudget)
			}

			prevCursor := cursor
			if cursor, err = b.processLogs(ctx, cursor, logDeadline); err != nil {
				b.logger.Errorf("Error processing logs: %v", err)
			}

			logsProcessed = cursor > prevCursor
		}

		// When processLogs had nothing to do (cluster idle), give backfills
		// a much larger budget — the full tick interval instead of just 50ms.
		if !logsProcessed {
			b.backfillBudget = 90 * time.Millisecond
		} else {
			b.backfillBudget = 50 * time.Millisecond
		}

		b.processBackgroundTasks(ctx, stop, cursor)

		// Always wake WaitForSequence waiters so they can re-check progress.
		// Without this, a waiter that enters Wait() between the last
		// NotifyProgress (inside processLogs) and the next tick would miss
		// the broadcast and block until new logs arrive.
		b.readStore.NotifyProgress()
	}
}

// bootInit runs the index builder's boot prologue as a single retryable unit:
// rebuild the index-config cache from Pebble/the read store, then read the
// persisted indexed cursor and seed the last-known Pebble sequence. It returns
// the recovered cursor and pebbleLast, or an error if any required read failed
// — the caller (loop) retries with backoff so a transient failure never
// advances the cursor against an incomplete config. ReadAppliedProposalProgress
// and query.ReadLastSequence stay best-effort (they tolerate failure today);
// only initIndexConfig, LastIndexedSequence, and NewDirectReadHandle are fatal.
func (b *Builder) bootInit(ctx context.Context) (cursor uint64, pebbleLast uint64, err error) {
	if err := b.initIndexConfig(ctx); err != nil {
		return 0, 0, fmt.Errorf("initializing index config: %w", err)
	}

	cursor, err = b.readStore.LastIndexedSequence()
	if err != nil {
		return 0, 0, fmt.Errorf("reading last indexed sequence: %w", err)
	}

	// Recover AppliedProposal sync progress (best-effort: a miss leaves
	// lastAppliedProposalSeq at 0, matching prior behavior).
	if seq, err := b.readStore.ReadAppliedProposalProgress(); err == nil {
		b.lastAppliedProposalSeq = seq
	}

	// Seed pebble last sequence. The handle is closed immediately after use
	// to release the RLock — keeping it open would deadlock with
	// RestoreCheckpoint (write lock) when processLogs tries to take a new RLock.
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return 0, 0, fmt.Errorf("creating read handle: %w", err)
	}

	if v, err := query.ReadLastSequence(handle); err == nil {
		pebbleLast = v
	}

	_ = handle.Close()

	return cursor, pebbleLast, nil
}
