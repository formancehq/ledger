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
// always non-negative today (all counters are monotonically increasing) but
// int64 leaves room for future decrement paths (e.g. a rollback log type).
type counterDelta = int64

// batchState holds the in-flight aggregation for one batch: per-ledger
// counter deltas + per-template usage deltas. Reset by newBatchState.
type batchState struct {
	counters  map[string]map[byte]counterDelta
	templates map[templateKey]templateDelta
}

func newBatchState() *batchState {
	return &batchState{
		counters:  make(map[string]map[byte]counterDelta),
		templates: make(map[templateKey]templateDelta),
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
// operands are non-nil. commonpb.Timestamp encodes nanoseconds-since-epoch
// as a single uint64 field (data), so ordering is direct integer compare.
func timestampGreater(a, b *commonpb.Timestamp) bool {
	return a.GetData() > b.GetData()
}

// empty reports whether the batch has no writes queued.
func (s *batchState) empty() bool {
	return len(s.counters) == 0 && len(s.templates) == 0
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

			items, err := query.ReadAuditItems(ctx, handle, entry.GetSequence())
			if err != nil {
				return cursor, fmt.Errorf("reading audit items for seq %d: %w", entry.GetSequence(), err)
			}

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

				if err := b.dispatchOrder(ctx, handle, order, item.GetLogSequence(), state); err != nil {
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
		b.entriesProcessed.Add(uint64(batchCount))

		// Sample Pebble last audit sequence from the FSM notification cache
		// when available. Notifications is nil in offline rebuild
		// (`ledgerctl store rebuild-usage`) where no FSM is running — the
		// atomic is skipped in that path.
		if b.notifications != nil {
			if cached := b.notifications.LastSequence.Load(); cached > 0 {
				b.pebbleLastAuditSeq.Store(cached)
			}
		}

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
// posting count is required (revert txs, script-backed create txs).
func (b *Builder) dispatchOrder(
	ctx context.Context,
	handle dal.PebbleGetter,
	order *raftcmdpb.Order,
	logSeq uint64,
	state *batchState,
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

	apply := scoped.GetApply()
	if apply == nil {
		// Non-apply orders (create/delete/promote/mirror-ingest) are not
		// event-counted in this MVP.
		return nil
	}

	switch data := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		return b.dispatchCreateTransaction(ctx, handle, ledger, data.CreateTransaction, logSeq, state)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		return b.dispatchRevertTransaction(ctx, handle, ledger, logSeq, state)
	}

	return nil
}

// dispatchCreateTransaction increments posting, reference, numscript-exec,
// and template usage counters for a create-tx order.
func (b *Builder) dispatchCreateTransaction(
	ctx context.Context,
	handle dal.PebbleGetter,
	ledger string,
	order *raftcmdpb.CreateTransactionOrder,
	logSeq uint64,
	state *batchState,
) error {
	// Resolved posting count lives on the log — the order carries raw
	// postings only for the non-scripted path.
	postings, err := b.postingsFromLog(ctx, handle, logSeq)
	if err != nil {
		return err
	}

	if postings > 0 {
		state.addCounter(ledger, usagestore.CounterPosting, counterDelta(postings))
	}

	if order.GetReference() != "" {
		state.addCounter(ledger, usagestore.CounterReference, 1)
	}

	isScripted := order.GetNumscriptReference() != nil ||
		(order.GetScript() != nil && order.GetScript().GetPlain() != "")

	if isScripted {
		state.addCounter(ledger, usagestore.CounterNumscriptExecution, 1)
	}

	if ref := order.GetNumscriptReference(); ref != nil {
		state.addTemplateUsage(ledger, ref.GetName(), order.GetTimestamp())
	}

	return nil
}

// dispatchRevertTransaction increments revert and posting counters for a
// revert-tx order. The resolved reverse-postings live on the log.
func (b *Builder) dispatchRevertTransaction(
	ctx context.Context,
	handle dal.PebbleGetter,
	ledger string,
	logSeq uint64,
	state *batchState,
) error {
	state.addCounter(ledger, usagestore.CounterRevert, 1)

	postings, err := b.postingsFromLog(ctx, handle, logSeq)
	if err != nil {
		return err
	}

	if postings > 0 {
		state.addCounter(ledger, usagestore.CounterPosting, counterDelta(postings))
	}

	return nil
}

// postingsFromLog fetches the log at logSeq and returns the resolved posting
// count. Handles both CreatedTransaction and RevertedTransaction payloads.
// Returns 0 if the log does not exist or carries no transaction (e.g.
// metadata-only logs).
func (b *Builder) postingsFromLog(ctx context.Context, handle dal.PebbleGetter, logSeq uint64) (int, error) {
	log, err := query.ReadLogBySequence(ctx, handle, logSeq)
	if err != nil {
		return 0, fmt.Errorf("reading log at seq %d: %w", logSeq, err)
	}

	if log == nil {
		return 0, nil
	}

	apply, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok || apply.Apply == nil {
		return 0, nil
	}

	ledgerLog := apply.Apply.GetLog()
	if ledgerLog == nil || ledgerLog.GetData() == nil {
		return 0, nil
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return len(p.CreatedTransaction.GetTransaction().GetPostings()), nil
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return len(p.RevertedTransaction.GetRevertTransaction().GetPostings()), nil
	}

	return 0, nil
}

// commitBatch applies the accumulated counter / template deltas to the
// usagestore and advances the cursor — all in a single Pebble batch commit.
func (b *Builder) commitBatch(state *batchState, cursor uint64) error {
	batch := b.usageStore.NewBatch()

	// Counter deltas: read-modify-write against the usagestore. Not the
	// FSM's Pebble — invariant #3 does not apply here.
	for ledger, counters := range state.counters {
		for counterID, delta := range counters {
			current, err := b.usageStore.GetCounter(ledger, counterID)
			if err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("reading counter %#x for ledger %q: %w", counterID, ledger, err)
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
		current, err := b.usageStore.GetTemplateUsage(k.ledger, k.template)
		if err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("reading template usage %q/%q: %w", k.ledger, k.template, err)
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
