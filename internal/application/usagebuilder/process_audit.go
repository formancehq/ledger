package usagebuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/usagestore"
)

// templateKey identifies a per-ledger, per-template aggregation slot.
type templateKey struct {
	ledger   string
	template string
}

// templateDelta accumulates a per-batch increment for one template.
type templateDelta struct {
	count    uint64
	lastUsed *commonpb.Timestamp // most recent timestamp seen this batch
}

// counterDelta is a signed delta for a per-ledger event counter. Deltas are
// almost always non-negative (event counters are monotonically increasing) but
// the volume counter can decrement (a draining eviction subtracts 1), so the
// underlying type is signed. Defined as a distinct type — not a type alias,
// which the repository conventions forbid — because the signed-delta semantics
// are load-bearing (see applyDelta's underflow clamp).
type counterDelta int64

// batchState holds the in-flight aggregation for one batch: per-ledger
// counter deltas, per-template usage deltas, and the set of ledger names
// dropped by DeleteLedgerOrder entries in this batch. Reset by newBatchState.
type batchState struct {
	counters       map[string]map[byte]counterDelta
	templates      map[templateKey]templateDelta
	deletedLedgers map[string]struct{}
}

func newBatchState() *batchState {
	return &batchState{
		counters:       make(map[string]map[byte]counterDelta),
		templates:      make(map[templateKey]templateDelta),
		deletedLedgers: make(map[string]struct{}),
	}
}

// markLedgerDeleted flags a ledger as dropped by this batch and drops any
// in-batch counter / template deltas already accumulated for it (they are
// for the pre-delete incarnation and must not survive the DeleteLedger).
//
// commitBatch runs the DeleteRange cascade FIRST inside the Pebble batch,
// then stages the counter / template Puts on top — later batch ops shadow
// earlier ones at commit, so if the same audit batch contains a delete
// followed by a same-name recreate + writes, the post-recreate Puts survive
// while every pre-batch row for the old incarnation is wiped. Combined with
// the accumulator reset here, that yields the same net semantics as the
// FSM's own DeleteLedger cascade (which unconditionally purges the ledger's
// Pebble rows regardless of earlier orders in the same proposal).
func (s *batchState) markLedgerDeleted(ledger string) {
	s.deletedLedgers[ledger] = struct{}{}
	delete(s.counters, ledger)

	for k := range s.templates {
		if k.ledger == ledger {
			delete(s.templates, k)
		}
	}
}

// addCounter accumulates a delta on the (ledger, counterID) slot.
func (s *batchState) addCounter(ledger string, counterID byte, delta counterDelta) {
	inner, ok := s.counters[ledger]
	if !ok {
		inner = make(map[byte]counterDelta, 4)
		s.counters[ledger] = inner
	}

	inner[counterID] += delta
}

// addTemplateUsage bumps the template usage aggregation. When multiple
// invocations of the same template land in one batch the max timestamp wins
// (matches the "lastUsed = most recent invocation" semantics).
func (s *batchState) addTemplateUsage(ledger, template string, ts *commonpb.Timestamp) {
	k := templateKey{ledger: ledger, template: template}
	cur := s.templates[k]
	cur.count++

	if ts != nil && (cur.lastUsed == nil || timestampGreater(ts, cur.lastUsed)) {
		cur.lastUsed = ts
	}

	s.templates[k] = cur
}

// timestampGreater reports whether a > b in wall-clock ordering. Both
// operands are non-nil. commonpb.Timestamp encodes microseconds-since-epoch
// as a single uint64 field (data), so ordering is direct integer compare.
func timestampGreater(a, b *commonpb.Timestamp) bool {
	return a.GetData() > b.GetData()
}

// empty reports whether the batch has no writes queued.
func (s *batchState) empty() bool {
	return len(s.counters) == 0 && len(s.templates) == 0 && len(s.deletedLedgers) == 0
}

// RebuildAll replays every audit entry from sequence 0, materialising the
// usagestore projections from scratch. Intended for offline use via
// `ledgerctl store rebuild-usage`. Returns the last processed audit sequence.
func (b *Builder) RebuildAll() (uint64, error) {
	return b.processAuditEntries(context.Background(), 0, time.Time{})
}

// processAuditEntries iterates audit entries after cursor, dispatches each
// item, and commits per-ledger counter deltas + template usage updates
// atomically alongside the cursor advance. Returns the new cursor.
//
// When deadline is non-zero, processing stops once the deadline has passed
// so the caller (initial catch-up loop) can release the Pebble snapshot
// between iterations.
func (b *Builder) processAuditEntries(ctx context.Context, cursor uint64, deadline time.Time) (uint64, error) {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return cursor, fmt.Errorf("creating read handle for audit processing: %w", err)
	}

	defer func() { _ = handle.Close() }()

	// afterSequence is passed by pointer to opt into the ">= cursor+1" filter;
	// nil would stream from the very first entry.
	afterSeq := cursor

	entriesCursor, err := query.ReadAuditEntries(ctx, handle, &afterSeq)
	if err != nil {
		return cursor, fmt.Errorf("opening audit cursor: %w", err)
	}

	defer func() { _ = entriesCursor.Close() }()

	startCursor := cursor
	lastProgressLog := time.Now()

	for {
		select {
		case <-ctx.Done():
			return cursor, ctx.Err()
		default:
		}

		state := newBatchState()

		var (
			batchCount   int
			lastAuditSeq uint64
			eof          bool
		)

		for batchCount < b.batchSize {
			entry, err := entriesCursor.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof = true

					break
				}

				return cursor, fmt.Errorf("reading audit entry: %w", err)
			}

			lastAuditSeq = entry.GetSequence()
			batchCount++

			// Failed proposals: no state change, nothing to project. The
			// hash chain still binds them (compareIdempotencyOutcomes)
			// but the usagebuilder is only interested in state deltas.
			if entry.GetSuccess() == nil {
				continue
			}

			// TransientVolumes live on the AppliedProposal projection (keyed by
			// audit sequence — 1:1 with AuditEntry). Batch-scoped, not per-log:
			// transientness depends on the WriteSet's final aggregate state,
			// so it cannot be split per item.
			proposal, err := query.ReadAppliedProposal(ctx, handle, entry.GetSequence())
			if err != nil {
				return cursor, fmt.Errorf("reading applied proposal for seq %d: %w", entry.GetSequence(), err)
			}

			if proposal != nil {
				for ledger, tvList := range proposal.GetTransientVolumes() {
					if n := len(tvList.GetVolumes()); n > 0 {
						state.addCounter(ledger, usagestore.CounterTransientUsed, counterDelta(n))
					}
				}
			}

			items, err := query.ReadAuditItems(ctx, handle, entry.GetSequence())
			if err != nil {
				return cursor, fmt.Errorf("reading audit items for seq %d: %w", entry.GetSequence(), err)
			}

			// Fresh dedup sets per audit entry — the entry is the natural
			// boundary at which a (ledger, account, asset) key can change
			// persistence class at most once. See applyVolumeAnnotations.
			entryState := newEntryVolumeState()

			for _, item := range items {
				// LogSequence == 0 → idempotent replay or non-log-producing
				// order (metadata schema changes, etc.). Skip: no work.
				if item.GetLogSequence() == 0 {
					continue
				}

				order := &raftcmdpb.Order{}
				if err := proto.Unmarshal(item.GetSerializedOrder(), order); err != nil {
					return cursor, fmt.Errorf("unmarshaling audit item order (audit_seq=%d, idx=%d): %w",
						entry.GetSequence(), item.GetOrderIndex(), err)
				}

				if err := b.dispatchOrder(ctx, handle, order, item.GetLogSequence(), state, entryState); err != nil {
					return cursor, err
				}
			}
		}

		if batchCount == 0 {
			break
		}

		if err := b.commitBatch(state, lastAuditSeq); err != nil {
			return cursor, err
		}

		cursor = lastAuditSeq
		b.lastProcessedAuditSeq.Store(cursor)

		// Periodic progress logging for long catch-up runs.
		if now := time.Now(); now.Sub(lastProgressLog) >= 10*time.Second {
			b.logger.WithFields(map[string]any{
				"cursor":    cursor,
				"from":      startCursor,
				"processed": cursor - startCursor,
			}).Infof("processAuditEntries progress")

			lastProgressLog = now
		}

		if eof {
			break
		}

		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
	}

	return cursor, nil
}

// dispatchOrder inspects a raw Order and accumulates counter / template
// deltas into the batch state. Fetches the produced log when the resolved
// posting count is required (revert txs, script-backed create txs, mirror
// ingests). entry carries the per-audit-entry dedup scratchpad — see
// applyVolumeAnnotations.
func (b *Builder) dispatchOrder(
	ctx context.Context,
	handle dal.PebbleGetter,
	order *raftcmdpb.Order,
	logSeq uint64,
	state *batchState,
	entry *entryVolumeState,
) error {
	scoped := order.GetLedgerScoped()
	if scoped == nil {
		// System-scoped orders (cluster config, chapter close, …) do not
		// contribute to per-ledger usage counters. Skip.
		return nil
	}

	ledger := scoped.GetLedger()
	if ledger == "" {
		return nil
	}

	// DeleteLedger orders MUST be projected — otherwise the usagestore
	// keeps stale rows for the dropped ledger until an operator runs
	// rebuild-usage. Same story as the readstore's DeleteLedgerIndexes.
	if scoped.GetDeleteLedger() != nil {
		state.markLedgerDeleted(ledger)

		return nil
	}

	// Mirror ingests produce Created/Reverted transaction logs the same
	// shape as the direct write path, so posting / revert / volume /
	// ephemeral counters all apply. References ARE carried across the
	// mirror wire (MirrorCreatedTransaction.reference) and counted the
	// same as native creates. Numscript templates are not — v2 sources
	// do not carry per-template invocation metadata, so we skip
	// CounterNumscriptExecution and template usage for mirrored logs.
	if mirror := scoped.GetMirrorIngest(); mirror != nil {
		return b.dispatchMirrorIngest(ctx, handle, ledger, mirror.GetEntry(), logSeq, state, entry)
	}

	apply := scoped.GetApply()
	if apply == nil {
		// CreateLedger / PromoteLedger — no state deltas that concern
		// usage counters.
		return nil
	}

	switch data := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		return b.dispatchCreateTransaction(ctx, handle, ledger, data.CreateTransaction, logSeq, state, entry)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		return b.dispatchRevertTransaction(ctx, handle, ledger, logSeq, state, entry)
	}

	return nil
}

// dispatchMirrorIngest projects a single MirrorLogEntry — the mirror worker
// replays these on the destination ledger, producing the same CreatedTx /
// RevertedTx logs the direct write path emits. We contribute the
// downstream-observable counters (postings, reverts, volumes, ephemeral) but
// skip client-driven metadata (reference / numscript) since it doesn't
// travel across the mirror wire.
func (b *Builder) dispatchMirrorIngest(
	ctx context.Context,
	handle dal.PebbleGetter,
	ledger string,
	mle *raftcmdpb.MirrorLogEntry,
	logSeq uint64,
	state *batchState,
	entry *entryVolumeState,
) error {
	if mle == nil {
		return nil
	}

	switch data := mle.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		ann, err := b.readLog(ctx, handle, logSeq)
		if err != nil {
			return err
		}
		if ann.postings > 0 {
			state.addCounter(ledger, usagestore.CounterPosting, counterDelta(ann.postings))
		}
		if data.CreatedTransaction.GetReference() != "" {
			state.addCounter(ledger, usagestore.CounterReference, 1)
		}
		applyVolumeAnnotations(ledger, ann, state, entry)
	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		state.addCounter(ledger, usagestore.CounterRevert, 1)
		ann, err := b.readLog(ctx, handle, logSeq)
		if err != nil {
			return err
		}
		if ann.postings > 0 {
			state.addCounter(ledger, usagestore.CounterPosting, counterDelta(ann.postings))
		}
		applyVolumeAnnotations(ledger, ann, state, entry)
	}

	return nil
}

// entryVolumeState is the per-audit-entry deduplication scratchpad. Each set
// records the (account, asset) tuples already applied to their respective
// counter for the current audit entry. VolumeCount, CounterEphemeralEvicted
// and CounterTransientUsed are per-batch cardinality deltas: a tuple that
// appears in multiple orders of the same batch (e.g. a shared "bank:main"
// account touched by three transactions) must contribute at most once to
// each counter. Postings / references / reverts / numscript executions are
// per-event and don't need this dedup.
type entryVolumeState struct {
	seenNewKept   map[volumeSetKey]struct{}
	seenPurged    map[volumeSetKey]struct{}
	seenEphemeral map[volumeSetKey]struct{}
}

// volumeSetKey mirrors state.volumeSetKey — kept local to avoid crossing
// package boundaries just for a triple of strings.
type volumeSetKey struct {
	ledger  string
	account string
	asset   string
}

func newEntryVolumeState() *entryVolumeState {
	return &entryVolumeState{
		seenNewKept:   make(map[volumeSetKey]struct{}),
		seenPurged:    make(map[volumeSetKey]struct{}),
		seenEphemeral: make(map[volumeSetKey]struct{}),
	}
}

// applyVolumeAnnotations folds the three disjoint per-log volume lists into
// the batch counter state, deduplicating each tuple within the current audit
// entry. The audit entry maps 1:1 to an FSM apply batch, so a tuple can only
// change persistence class (new → kept, draining → evicted, ephemeral in-out)
// at most once inside it. Without this dedup, an account touched by N orders
// of the same batch would be counted N times.
func applyVolumeAnnotations(ledger string, ann logVolumeAnnotations, state *batchState, entry *entryVolumeState) {
	for _, v := range ann.newKept {
		k := volumeSetKey{ledger: ledger, account: v.GetAccount(), asset: v.GetAsset()}
		if _, ok := entry.seenNewKept[k]; ok {
			continue
		}
		entry.seenNewKept[k] = struct{}{}
		state.addCounter(ledger, usagestore.CounterVolume, 1)
	}

	// Draining evictions: a volume that persisted with a non-zero balance
	// and now goes back to zero. Both the volume counter (–1, it was
	// counted before) and the eviction counter (+1, this is an eviction
	// event) contribute. The pre-EN-1420 EphemeralEvictedCount tallied
	// every log-level eviction, not just pure ephemeral tuples.
	for _, v := range ann.purged {
		k := volumeSetKey{ledger: ledger, account: v.GetAccount(), asset: v.GetAsset()}
		if _, ok := entry.seenPurged[k]; ok {
			continue
		}
		entry.seenPurged[k] = struct{}{}
		state.addCounter(ledger, usagestore.CounterVolume, -1)
		state.addCounter(ledger, usagestore.CounterEphemeralEvicted, 1)
	}

	for _, v := range ann.ephemeral {
		k := volumeSetKey{ledger: ledger, account: v.GetAccount(), asset: v.GetAsset()}
		if _, ok := entry.seenEphemeral[k]; ok {
			continue
		}
		entry.seenEphemeral[k] = struct{}{}
		state.addCounter(ledger, usagestore.CounterEphemeralEvicted, 1)
	}
}

// dispatchCreateTransaction increments posting, reference, numscript-exec,
// ephemeral-evicted, volume and template usage counters for a create-tx
// order.
func (b *Builder) dispatchCreateTransaction(
	ctx context.Context,
	handle dal.PebbleGetter,
	ledger string,
	order *raftcmdpb.CreateTransactionOrder,
	logSeq uint64,
	state *batchState,
	entry *entryVolumeState,
) error {
	// Resolved posting count and volume annotations live on the log — the
	// order carries raw postings only for the non-scripted path, and never
	// carries purge info.
	ann, err := b.readLog(ctx, handle, logSeq)
	if err != nil {
		return err
	}

	if ann.postings > 0 {
		state.addCounter(ledger, usagestore.CounterPosting, counterDelta(ann.postings))
	}

	applyVolumeAnnotations(ledger, ann, state, entry)

	if order.GetReference() != "" {
		state.addCounter(ledger, usagestore.CounterReference, 1)
	}

	isScripted := order.GetNumscriptReference() != nil ||
		(order.GetScript() != nil && order.GetScript().GetPlain() != "")

	if isScripted {
		state.addCounter(ledger, usagestore.CounterNumscriptExecution, 1)
	}

	if ref := order.GetNumscriptReference(); ref != nil {
		// Prefer the order's client-supplied timestamp so template usage
		// tracks the wall clock the client cares about; when omitted, fall
		// back to the effective timestamp the FSM stamped on the produced
		// log (either the client value or the proposal date resolved by
		// processor_transaction.go). Either way we end up with a
		// deterministic non-nil timestamp on every replay.
		ts := order.GetTimestamp()
		if ts == nil {
			ts = ann.txTimestamp
		}
		state.addTemplateUsage(ledger, ref.GetName(), ts)
	}

	return nil
}

// dispatchRevertTransaction increments revert, posting, ephemeral-evicted
// and volume counters for a revert-tx order. The resolved reverse-postings,
// purged volumes and newly-created volumes live on the produced log.
func (b *Builder) dispatchRevertTransaction(
	ctx context.Context,
	handle dal.PebbleGetter,
	ledger string,
	logSeq uint64,
	state *batchState,
	entry *entryVolumeState,
) error {
	state.addCounter(ledger, usagestore.CounterRevert, 1)

	ann, err := b.readLog(ctx, handle, logSeq)
	if err != nil {
		return err
	}

	if ann.postings > 0 {
		state.addCounter(ledger, usagestore.CounterPosting, counterDelta(ann.postings))
	}

	applyVolumeAnnotations(ledger, ann, state, entry)

	return nil
}

// logVolumeAnnotations bundles the three disjoint TouchedVolume lists that
// LedgerLog carries plus the resolved posting count and the transaction
// timestamp. The lists are kept as slices (not lengths) because the counter
// dispatch needs per-tuple identity for batch-scoped deduplication — see
// applyVolumeAnnotations. txTimestamp is the effective timestamp the FSM
// stamped on the transaction (client-provided or falling back to the
// proposal date). Nil for non-transaction logs.
type logVolumeAnnotations struct {
	postings    int
	purged      []*commonpb.TouchedVolume // len — draining only
	newKept     []*commonpb.TouchedVolume // new + kept
	ephemeral   []*commonpb.TouchedVolume // new + purged (pure ephemeral)
	txTimestamp *commonpb.Timestamp       // Transaction.Timestamp on Created/Reverted logs
}

// readLog fetches the log at logSeq and returns its posting count plus the
// three disjoint volume-annotation lists. Empty when the log does not exist
// or carries no transaction / annotation.
func (b *Builder) readLog(ctx context.Context, handle dal.PebbleGetter, logSeq uint64) (logVolumeAnnotations, error) {
	log, err := query.ReadLogBySequence(ctx, handle, logSeq)
	if err != nil {
		return logVolumeAnnotations{}, fmt.Errorf("reading log at seq %d: %w", logSeq, err)
	}

	if log == nil {
		return logVolumeAnnotations{}, nil
	}

	apply, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok || apply.Apply == nil {
		return logVolumeAnnotations{}, nil
	}

	ledgerLog := apply.Apply.GetLog()
	if ledgerLog == nil {
		return logVolumeAnnotations{}, nil
	}

	// PurgedVolumes / NewKeptVolumes / EphemeralVolumes live on LedgerLog
	// directly (not on the payload variant). The three lists are DISJOINT
	// at the FSM emission site, but a single (account, asset) key can
	// still appear in multiple orders' lists within the SAME batch because
	// each order tracks the volumes IT touched. The counter side of the
	// pipeline deduplicates per audit entry — see applyVolumeDelta.
	result := logVolumeAnnotations{
		purged:    ledgerLog.GetPurgedVolumes(),
		newKept:   ledgerLog.GetNewKeptVolumes(),
		ephemeral: ledgerLog.GetEphemeralVolumes(),
	}

	if ledgerLog.GetData() == nil {
		return result, nil
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		tx := p.CreatedTransaction.GetTransaction()
		result.postings = len(tx.GetPostings())
		result.txTimestamp = tx.GetTimestamp()
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		tx := p.RevertedTransaction.GetRevertTransaction()
		result.postings = len(tx.GetPostings())
		result.txTimestamp = tx.GetTimestamp()
	}

	return result, nil
}

// commitBatch applies the accumulated counter / template deltas to the
// usagestore and advances the cursor — all in a single Pebble batch commit.
//
// Ordering inside the batch: DeleteRange cascade FIRST, then counter /
// template Puts. Pebble batches apply operations in enqueue order at commit,
// so any Put on a key inside a DeleteRange range enqueued earlier still lands
// (later ops shadow earlier ones). Combined with markLedgerDeleted clearing
// in-batch counters for the deleted ledger, this yields the correct semantic
// for a delete+recreate sequence within the same audit batch: every
// pre-batch row for the old incarnation is wiped, while any post-recreate
// Puts on the recycled name survive.
func (b *Builder) commitBatch(state *batchState, cursor uint64) error {
	batch := b.usageStore.NewBatch()

	// Ledger deletions first — see the function comment.
	for ledger := range state.deletedLedgers {
		if err := usagestore.DeleteLedger(batch, ledger); err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("dropping usage rows for deleted ledger %q: %w", ledger, err)
		}
	}

	// Counter deltas: read-modify-write against the usagestore. Not the
	// FSM's Pebble — invariant #3 does not apply here.
	for ledger, counters := range state.counters {
		// If this batch also deleted the ledger, the DeleteRange enqueued
		// above logically zeroes every counter for the recycled name. But
		// GetCounter reads the committed DB, not the pending batch, so it
		// would return the OLD incarnation's value and we'd write
		// old+delta on top of the DeleteRange — resurrecting stale counts
		// for a same-batch delete+recreate. Treat the baseline as 0 for a
		// deleted ledger so only the post-recreate deltas survive.
		_, deleted := state.deletedLedgers[ledger]

		for counterID, delta := range counters {
			var current uint64
			if !deleted {
				var err error
				current, err = b.usageStore.GetCounter(ledger, counterID)
				if err != nil {
					_ = batch.Cancel()

					return fmt.Errorf("reading counter %#x for ledger %q: %w", counterID, ledger, err)
				}
			}

			next := applyDelta(current, delta)

			if err := b.usageStore.PutCounter(batch, ledger, counterID, next); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("writing counter %#x for ledger %q: %w", counterID, ledger, err)
			}
		}
	}

	// Template deltas: same read-modify-write pattern on the TemplateUsage
	// proto. count is additive; last_used is max(previous, batch max).
	for k, delta := range state.templates {
		// Same same-batch delete+recreate hazard as counters above: for a
		// ledger this batch deleted, the DeleteRange zeroes the recycled
		// name, so ignore the persisted (old incarnation) value and start
		// from a nil baseline.
		var current *commonpb.TemplateUsage
		if _, deleted := state.deletedLedgers[k.ledger]; !deleted {
			var err error
			current, err = b.usageStore.GetTemplateUsage(k.ledger, k.template)
			if err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("reading template usage %q/%q: %w", k.ledger, k.template, err)
			}
		}

		next := mergeTemplateUsage(current, delta)

		if err := b.usageStore.PutTemplateUsage(batch, k.ledger, k.template, next); err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("writing template usage %q/%q: %w", k.ledger, k.template, err)
		}
	}

	if err := b.usageStore.WriteProgress(batch, cursor); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("writing usage progress: %w", err)
	}

	if err := batch.Commit(); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("committing usage batch: %w", err)
	}

	return nil
}

// applyDelta safely adds a signed delta to a uint64 counter, clamping at
// zero on underflow. Same helper as write_set_counters.applyDelta but kept
// local so the usagebuilder doesn't pull in state.
func applyDelta(current uint64, delta counterDelta) uint64 {
	if delta >= 0 {
		return current + uint64(delta)
	}

	sub := uint64(-delta)
	if sub > current {
		return 0
	}

	return current - sub
}

// mergeTemplateUsage folds a per-batch delta into the persisted TemplateUsage.
// A nil `current` (no persisted entry yet) is treated as {count: 0, lastUsed: nil}.
func mergeTemplateUsage(current *commonpb.TemplateUsage, delta templateDelta) *commonpb.TemplateUsage {
	next := &commonpb.TemplateUsage{}
	if current != nil {
		next.Count = current.GetCount() + delta.count
		next.LastUsed = current.GetLastUsed()
	} else {
		next.Count = delta.count
	}

	if delta.lastUsed != nil && (next.GetLastUsed() == nil || timestampGreater(delta.lastUsed, next.GetLastUsed())) {
		next.LastUsed = delta.lastUsed
	}

	return next
}
