package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// processLogs reads logs from Pebble starting after the given cursor,
// indexes them in batches of indexBatchSize. Logs are consumed on the fly
// (no intermediate slice) so the proto object can be GC'd immediately.
// When deadline is non-zero, processing stops after the deadline to yield
// time to other work (e.g. backfills).
//
// Each batch has two phases:
//   - Fold logs and buffer their index mutations into WriteBatch. Metadata
//     replacements and deletes resolve their old encoding through the reverse
//     map; known-absent transaction inserts skip that lookup.
//   - Persist the native cursor in the same Pebble batch. Only the batch that
//     reaches the fixed source snapshot's final native sequence also publishes
//     that snapshot's Raft applied-index certificate, atomically with its final
//     projection writes.
//
// Native progress for runs that produce no projection writes is coalesced until
// the end. Raft progress can still advance without native movement when the
// fixed source snapshot contains only no-op, failed, or technical Raft entries.
func (b *Builder) processLogs(ctx context.Context, cursor uint64, deadline time.Time) (uint64, error) {
	defer func() {
		b.rollbackFoldBatch()
		b.wb.Reset()
	}()

	// A checkpoint log's certificate may already be durable when filesystem
	// materialization fails, while the indexed cursor is still parked before
	// that log. Retry the side effect first, so the re-cross below finds the
	// directory marked and consumes later logs only once it exists.
	if err := b.materializePendingCheckpoint(ctx); err != nil {
		return cursor, err
	}

	handle, err := b.pebbleStore.NewReadHandle()
	if err != nil {
		return cursor, fmt.Errorf("creating read handle for log processing: %w", err)
	}

	defer func() { _ = handle.Close() }()

	if err := b.pinProjectionTarget(handle); err != nil {
		return cursor, err
	}
	targetAppliedIndex := b.projectionTargetAppliedIndex
	targetSequence := b.projectionTargetSequence

	// Per-batch schema resolver: memoizes LedgerInfo lookups for the
	// duration of this processLogs call. Hot-path encode sites read from
	// it via b.coerceForLedger.
	b.batchSchema = newSchemaResolver(handle, b.attrs)
	defer func() { b.batchSchema = nil }()

	logsCursor, err := query.ReadLogsSince(ctx, handle, cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	// Open the AppliedProposal iterator for transient-account filtering.
	proposals, err := newAppliedProposalSync(ctx, handle, b.lastAppliedProposalSeq)
	if err != nil {
		return cursor, fmt.Errorf("creating applied proposal sync: %w", err)
	}

	defer func() { _ = proposals.close() }()

	// cursor is the last durably committed native sequence and is therefore the
	// safe value returned on failure. unpersistedCursor is the latest sequence
	// consumed from this snapshot; loop bounds must use it so coalesced empty
	// batches cannot read beyond the fixed target on a continuation call.
	needsPersist := false
	unpersistedCursor := cursor
	unpersistedCount := 0
	startCursor := cursor
	lastProgressLog := time.Now()
	// A checkpoint log is always the only log of its batch: the indexed cursor
	// passes it only once the checkpoint action has run (see the commit
	// below), so a builder that dies in between re-crosses this log alone and
	// re-indexes nothing. A checkpoint log met while the batch already holds
	// other logs is carried over and heads the next batch.
	var carried *checkpointLogFields

	for unpersistedCursor < targetSequence {
		var (
			batchCount                int
			lastSeq                   uint64
			eof                       bool
			pendingCheckpointCreate   uint64
			pendingCheckpointHorizon  uint64
			pendingCheckpointRestored bool
			pendingCheckpointDelete   uint64
		)

		noteCheckpointCreate := func(id, appliedIndex uint64) error {
			pendingCheckpointCreate = id
			var err error
			pendingCheckpointHorizon, pendingCheckpointRestored, err = checkpointHorizon(handle, id, appliedIndex, targetAppliedIndex)

			return err
		}

		// Create a batch up front so write methods have a valid target.
		batch := b.readStore.NewBatch()
		b.initFoldBatch(batch)

		// Native log sequences are contiguous. Bound this batch to the fixed
		// target so later commits visible through a newly-opened continuation
		// snapshot cannot pull the target forward between deadline-driven calls.
		batchLimit := b.batchSize
		if remaining := targetSequence - unpersistedCursor; remaining < uint64(batchLimit) {
			batchLimit = int(remaining)
		}

		if carried != nil {
			lastSeq = carried.sequence
			batchCount = 1
			if carried.createID > 0 {
				if err := noteCheckpointCreate(carried.createID, carried.appliedIndex); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			} else {
				pendingCheckpointDelete = carried.deleteID
			}
			carried = nil
			// The carried checkpoint log fills this batch on its own.
			batchLimit = batchCount
		}

		// Ends the batch at the log before a checkpoint log met mid-batch; the
		// checkpoint log itself heads the next batch.
		carry := func(fields checkpointLogFields, previousSeq uint64) {
			carried = &fields
			lastSeq = previousSeq
			batchCount--
		}

		// Iterate logs from Pebble and buffer index writes into the batch.
		for batchCount < batchLimit {
			log, err := logsCursor.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof = true

					break
				}

				_ = batch.Cancel()

				return cursor, err
			}

			previousSeq := lastSeq
			lastSeq = log.GetSequence()
			batchCount++

			if log.GetPayload() == nil {
				continue
			}

			if cl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_CreateLedger); ok {
				if cl.CreateLedger == nil {
					_ = batch.Cancel()

					return cursor, historyReplayInvariantf("nil CreatedLedger payload")
				}
				if err := b.observeCreatedLedger(cl.CreateLedger.GetName()); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}

				continue
			}

			// Handle ledger deletion: remove all read indexes for the deleted ledger.
			if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
				if dl.DeleteLedger == nil {
					_ = batch.Cancel()

					return cursor, historyReplayInvariantf("nil DeletedLedger payload")
				}
				name := dl.DeleteLedger.GetName()
				if err := b.purgeDeletedLedger(batch, name); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}

				continue
			}

			// A query checkpoint creation ends the batch: the read index is
			// materialized at this exact point once the batch is committed.
			if cqc, ok := log.GetPayload().GetType().(*commonpb.LogPayload_CreatedQueryCheckpoint); ok {
				fields := checkpointLogFields{
					sequence:     log.GetSequence(),
					createID:     cqc.CreatedQueryCheckpoint.GetCheckpointId(),
					appliedIndex: cqc.CreatedQueryCheckpoint.GetAppliedIndex(),
				}
				if batchCount > 1 {
					carry(fields, previousSeq)

					break
				}

				if err := noteCheckpointCreate(fields.createID, fields.appliedIndex); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}

				break
			}

			// A query checkpoint deletion ends the batch the same way; the
			// physical checkpoint files are removed once it is committed.
			if dqc, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeletedQueryCheckpoint); ok {
				fields := checkpointLogFields{
					sequence: log.GetSequence(),
					deleteID: dqc.DeletedQueryCheckpoint.GetCheckpointId(),
				}
				if batchCount > 1 {
					carry(fields, previousSeq)

					break
				}

				pendingCheckpointDelete = fields.deleteID

				break
			}

			applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
			if !ok {
				continue
			}

			ledgerName := applyLog.Apply.GetLedgerName()
			ledgerLog := applyLog.Apply.GetLog()

			if ledgerLog == nil || ledgerLog.GetData() == nil {
				continue
			}
			historyBefore, historyExists := b.historyStateFor(ledgerName)
			category := commonpb.LedgerLogCategoryOf(ledgerLog.GetData())
			if err := b.observeLedgerPayload(ledgerName, ledgerLog.GetData()); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}

			cfg := b.ledgerConfig(ledgerName)
			var excludedVolumes map[domain.AccountAssetKey]struct{}
			if cfg.indexesPostingDerived() {
				excludedVolumes = proposals.excludedForLog(lastSeq, ledgerName, ledgerLog)
			}

			b.wb.SetEventSequence(log.GetSequence())

			// Per-ledger log index is always maintained — every log gets a
			// LedgerLogKey(ledgerName, logID) → globalSeq entry. The controller's
			// ListLogs path relies on this being unconditional.
			if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence()); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}

			// log_date covers every ledger-local log, including CONTROL. While a
			// ledger remains EMPTY we stage date rows proactively so a later
			// CreateIndex can become live immediately without a global replay;
			// this also covers the CreateIndex log itself because it is CONTROL.
			// If the first HISTORY arrives before log_date is registered, discard
			// the speculative prefix in this same fold: a genuine late index will
			// rebuild the complete stream through its normal backfill.
			logDateActive := cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
			wasEmpty := historyExists && historyBefore == ledgerHistoryEmpty
			if wasEmpty && category == commonpb.LedgerLogCategory_LEDGER_LOG_CATEGORY_HISTORY && !logDateActive {
				if err := purgeSpeculativeLogDates(batch, ledgerName); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			} else if logDateActive || wasEmpty {
				if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			}

			if err := b.indexPayload(b.kb, cfg, ledgerName, ledgerLog.GetData().GetPayload(), excludedVolumes); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}
		}

		if batchCount == 0 {
			_ = batch.Cancel()

			break
		}

		// Commit the batch if there are index writes or a checkpoint action pending.
		hasCheckpointAction := pendingCheckpointCreate > 0 || pendingCheckpointDelete > 0
		completesTarget := lastSeq >= targetSequence
		if !b.wb.Empty() || hasCheckpointAction || completesTarget {
			// The cursors move in the same commit as the rows they cover. A
			// checkpoint action holds them back until it has run: its log is
			// the batch's only log and writes no rows, so a builder that dies
			// before the action resumes at this log and runs it again.
			appliedProposalSeq := b.lastAppliedProposalSeq
			if !hasCheckpointAction {
				// Write progress into the same batch before Flush commits it.
				if err := b.readStore.WriteProgress(batch, lastSeq); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}

				// Persist only AppliedProposal entries whose log range is fully behind lastSeq.
				var err error
				appliedProposalSeq, err = b.persistAppliedProposalProgress(proposals, batch, lastSeq)
				if err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			}

			certifiedHorizon := uint64(0)
			if completesTarget {
				certifiedHorizon = targetAppliedIndex
			} else if pendingCheckpointHorizon > 0 && !pendingCheckpointRestored {
				certifiedHorizon = pendingCheckpointHorizon
			}
			if certifiedHorizon > 0 {
				if err := b.readStore.WriteRaftProgress(batch, certifiedHorizon); err != nil {
					_ = batch.Cancel()

					return cursor, fmt.Errorf("writing read projection Raft progress: %w", err)
				}
			}

			if err := b.flushWriteBatch(); err != nil {
				b.logger.WithFields(map[string]any{
					"batchSize": batchCount,
					"lastSeq":   lastSeq,
					"error":     err,
				}).Errorf("Error processing batch")

				return cursor, err
			}

			b.commitFoldBatch()
			if hasCheckpointAction {
				// The cursor commits after the action below; only the consumed
				// frontier moves here.
				unpersistedCursor = lastSeq
				unpersistedCount += batchCount
			} else {
				b.advanceCursors(lastSeq, appliedProposalSeq, batchCount+unpersistedCount)
				cursor, unpersistedCursor, unpersistedCount = lastSeq, lastSeq, 0
			}

			needsPersist = false
		} else {
			_ = batch.Cancel()
			b.wb.Reset()
			b.rollbackFoldBatch()
			needsPersist = true
			unpersistedCursor = lastSeq
			unpersistedCount += batchCount
		}

		// Materialize a query checkpoint inline, at the exact moment the builder
		// crosses the CreatedQueryCheckpoint log — so the normal read projection
		// reflects precisely MaxSequence (the checkpoint's point-in-time). The
		// independently maintained audit projection may already be ahead; audit
		// queries trim its candidates to the main checkpoint's audit sequence.
		// The materialization is atomic (temp dir + fsync + rename + .ready marker
		// last), so a reader never observes a partial checkpoint. A live audit
		// rebuild is waited through here, and a lifecycle transition during
		// materialization retries the snapshot under the new ready generation.
		// The indexed cursor is committed only after this block, so a builder
		// that dies anywhere in it resumes at this same log and materializes
		// again; a marked directory makes that a no-op. Until then reads on this
		// replica return the retryable ErrCheckpointNotReady (Unavailable). Only
		// a disabled or failed audit projection leaves the checkpoint behind for
		// good: the cursor then moves on and the client deletes and recreates it
		// (see openCheckpointStores).
		if cpID := pendingCheckpointCreate; cpID > 0 {
			b.pendingCheckpointMaterialization = pendingCheckpointMaterialization{
				id:      cpID,
				horizon: pendingCheckpointHorizon,
			}
			if err := b.materializePendingCheckpoint(ctx); err != nil {
				return cursor, err
			}
		}

		if cpID := pendingCheckpointDelete; cpID > 0 {
			b.deleteReadIndexCheckpoint(cpID)
		}

		// The checkpoint action has run: the cursors may now pass its log.
		if hasCheckpointAction {
			appliedProposalSeq, err := b.commitCheckpointProgress(proposals, lastSeq)
			if err != nil {
				return cursor, err
			}

			// The checkpoint batch folded its own count into unpersistedCount.
			b.advanceCursors(lastSeq, appliedProposalSeq, unpersistedCount)
			cursor, unpersistedCursor, unpersistedCount = lastSeq, lastSeq, 0
		}

		// Sample pebble last sequence from the cached atomic (written by the FSM
		// before signalling LogCommitted). This avoids opening a Pebble iterator
		// and deserializing a protobuf just to read a counter.
		if cached := b.notifications.LastSequence.Load(); cached > 0 {
			b.pebbleLastSeq.Store(cached)
		}

		// Periodic progress logging for long catch-up runs.
		if now := time.Now(); now.Sub(lastProgressLog) >= 10*time.Second {
			b.logger.WithFields(map[string]any{
				"cursor":  max(cursor, unpersistedCursor),
				"from":    startCursor,
				"indexed": max(cursor, unpersistedCursor) - startCursor,
			}).Infof("processLogs progress")

			lastProgressLog = now
		}

		if eof {
			break
		}
		if unpersistedCursor >= targetSequence {
			break
		}

		// Yield to backfills when a deadline is set.
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
	}

	// Persist progress once if we advanced the cursor without any index writes.
	// This reduces fsyncs from O(logs/batchSize) to O(1) when no indexes are active.
	if needsPersist {
		appliedProposalSeq, err := b.commitCoalescedProgress(proposals, unpersistedCursor, targetSequence, targetAppliedIndex)
		if err != nil {
			return cursor, err
		}

		b.lastAppliedProposalSeq = appliedProposalSeq
		cursor = unpersistedCursor
		b.lastIndexedSeq.Store(cursor)
		b.logsIndexed.Add(uint64(unpersistedCount))
		b.readStore.NotifyProgress()
	}

	// Raft can advance without producing a system log (no-op, rejected, or
	// technical-only entries). Publish that fixed snapshot horizon even when
	// there was no native cursor movement.
	if cursor >= targetSequence {
		if err := b.certifyRaftOnlyProgress(targetAppliedIndex); err != nil {
			return cursor, err
		}

		b.projectionTargetActive = false
	}

	return cursor, nil
}

// purgeDeletedLedger keeps the read-index range deletion and optimistic
// ledger-history/config mutation in the same fold batch and undo journal.
func (b *Builder) purgeDeletedLedger(batch *dal.WriteSession, name string) error {
	if err := readstore.DeleteLedgerIndexes(batch, name); err != nil {
		return err
	}
	if err := b.observeDeletedLedger(name); err != nil {
		return err
	}
	b.markLedgerDeletedInBatch(name)

	return nil
}

// purgeSpeculativeLogDates discards EMPTY-ledger CONTROL rows at the first
// HISTORY log when log_date was never registered for that incarnation.
func purgeSpeculativeLogDates(batch *dal.WriteSession, ledgerName string) error {
	return readstore.DeleteLedgerIndexPrefix(batch, readstore.PrefixLedgerLogDate, ledgerName)
}

// advanceCursors publishes a committed progress batch: both durable cursors now
// stand at lastSeq and indexed logs were folded under it.
func (b *Builder) advanceCursors(lastSeq, appliedProposalSeq uint64, indexed int) {
	b.lastAppliedProposalSeq = appliedProposalSeq
	b.lastIndexedSeq.Store(lastSeq)
	b.logsIndexed.Add(uint64(indexed))
	b.readStore.NotifyProgress()
}

// pinProjectionTarget keeps a single upper bound across deadline-driven calls.
// Both values come from the same main-store snapshot; a continuation never
// admits logs committed after that snapshot until it reaches this target.
func (b *Builder) pinProjectionTarget(handle *dal.ReadHandle) error {
	if b.projectionTargetActive {
		return nil
	}
	targetAppliedIndex, err := query.ReadLastAppliedIndex(handle)
	if err != nil {
		return fmt.Errorf("reading target applied index: %w", err)
	}
	targetSequence, err := query.ReadLastSequence(handle)
	if err != nil {
		return fmt.Errorf("reading target log sequence: %w", err)
	}
	b.projectionTargetAppliedIndex = targetAppliedIndex
	b.projectionTargetSequence = targetSequence
	b.projectionTargetActive = true

	return nil
}

// persistAppliedProposalProgress includes only proposals wholly behind the
// native log frontier. A cursor error aborts the batch before it can certify
// an incomplete transient-volume exclusion set.
func (b *Builder) persistAppliedProposalProgress(proposals *appliedProposalSync, batch *dal.WriteSession, lastProcessedSeq uint64) (uint64, error) {
	proposals.advanceBefore(lastProcessedSeq + 1)
	if err := proposals.err(); err != nil {
		return b.lastAppliedProposalSeq, fmt.Errorf("applied proposal cursor failed: %w", err)
	}
	if seq := proposals.resumeSequence(); seq > b.lastAppliedProposalSeq {
		if err := b.readStore.WriteAppliedProposalProgress(batch, seq); err != nil {
			return b.lastAppliedProposalSeq, err
		}

		return seq, nil
	}

	return b.lastAppliedProposalSeq, nil
}

// commitCheckpointProgress runs only after the checkpoint filesystem action.
// Its separate commit keeps the durable cursor before that log until a retry
// can safely re-cross it; the checkpoint's projection batch was committed first.
func (b *Builder) commitCheckpointProgress(proposals *appliedProposalSync, lastSeq uint64) (uint64, error) {
	batch := b.readStore.NewBatch()
	if err := b.readStore.WriteProgress(batch, lastSeq); err != nil {
		_ = batch.Cancel()

		return b.lastAppliedProposalSeq, fmt.Errorf("writing progress past checkpoint log %d: %w", lastSeq, err)
	}
	seq, err := b.persistAppliedProposalProgress(proposals, batch, lastSeq)
	if err != nil {
		_ = batch.Cancel()

		return b.lastAppliedProposalSeq, fmt.Errorf("writing applied proposal progress past checkpoint log %d: %w", lastSeq, err)
	}
	if err := batch.Commit(); err != nil {
		_ = batch.Cancel()

		return b.lastAppliedProposalSeq, fmt.Errorf("committing progress past checkpoint log %d: %w", lastSeq, err)
	}

	return seq, nil
}

// commitCoalescedProgress pays one fsync for a run of native logs that wrote
// no index rows. It certifies the Raft target only after the native frontier
// has reached the fixed snapshot's last sequence.
func (b *Builder) commitCoalescedProgress(proposals *appliedProposalSync, lastSeq, targetSequence, targetAppliedIndex uint64) (uint64, error) {
	batch := b.readStore.NewBatch()
	if err := b.readStore.WriteProgress(batch, lastSeq); err != nil {
		_ = batch.Cancel()

		return b.lastAppliedProposalSeq, fmt.Errorf("writing progress: %w", err)
	}
	seq, err := b.persistAppliedProposalProgress(proposals, batch, lastSeq)
	if err != nil {
		_ = batch.Cancel()

		return b.lastAppliedProposalSeq, fmt.Errorf("writing applied proposal progress: %w", err)
	}
	if lastSeq >= targetSequence {
		if err := b.readStore.WriteRaftProgress(batch, targetAppliedIndex); err != nil {
			_ = batch.Cancel()

			return b.lastAppliedProposalSeq, fmt.Errorf("writing read projection Raft progress: %w", err)
		}
	}
	if err := batch.Commit(); err != nil {
		_ = batch.Cancel()

		return b.lastAppliedProposalSeq, fmt.Errorf("committing progress: %w", err)
	}

	return seq, nil
}

// certifyRaftOnlyProgress handles applied entries that produced no native log.
func (b *Builder) certifyRaftOnlyProgress(targetAppliedIndex uint64) error {
	progress, err := b.readStore.ReadRaftProgress()
	if err != nil {
		return fmt.Errorf("reading read projection Raft progress: %w", err)
	}
	if progress >= targetAppliedIndex {
		return nil
	}
	batch := b.readStore.NewBatch()
	defer func() { _ = batch.Cancel() }()
	if err := b.readStore.WriteRaftProgress(batch, targetAppliedIndex); err != nil {
		return fmt.Errorf("writing read projection Raft progress: %w", err)
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing read projection Raft progress: %w", err)
	}
	b.readStore.NotifyProgress()

	return nil
}
