package state

import (
	"errors"
	"fmt"
	"sort"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/holiman/uint256"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// mergeAndTrackBloom merges a DerivedKeyStore into its parent, writes the updates
// to the Pebble batch via the Attribute AND to the 0xFF cache zone (lean format),
// tracks canonical keys for bloom filter updates, and processes any attribute deletions.
func mergeAndTrackBloom[K attributes.Key, V proto.Message](
	derived *attributes.DerivedKeyStore[K, V],
	attr *attributes.Attribute[V],
	batch *dal.WriteSession,
	genByte byte,
	cacheType byte,
	bloomSlice *[]attributes.U128,
	label string,
) ([]attributes.Update[K, V], []attributes.Deletion[K], error) {
	updates, deletions, err := derived.Merge()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to merge %s: %w", label, err)
	}

	// Write to 0xF1 (attributes) + 0xFF (cache) in a single loop, sharing marshaled bytes.
	if err := mergeSimpleWithCache(attr, batch, genByte, cacheType, updates); err != nil {
		return nil, nil, fmt.Errorf("failed merging %s attributes: %w", label, err)
	}

	for _, update := range updates {
		*bloomSlice = append(*bloomSlice, update.ID)
	}

	for _, deletion := range deletions {
		if err := attr.Delete(batch, deletion.CanonicalKey); err != nil {
			return nil, nil, fmt.Errorf("failed deleting %s attribute: %w", label, err)
		}

		if err := writeCacheTombstone(batch, cacheType, deletion.ID, deletion.Tag); err != nil {
			return nil, nil, fmt.Errorf("failed writing %s cache tombstone: %w", label, err)
		}
	}

	return updates, deletions, nil
}

// signingKeyUpdate represents a pending signing key change to be applied during Merge.
type signingKeyUpdate struct {
	keyID       string
	publicKey   []byte // nil for removals
	parentKeyID string
	remove      bool
}

// signingConfigUpdate represents a pending require-signatures change.
type signingConfigUpdate struct {
	requireSignatures bool
}

// maintenanceModeUpdate represents a pending maintenance mode change.
type maintenanceModeUpdate struct {
	enabled bool
}

type WriteSet struct {
	fsm                   *Machine
	attrs                 *attributes.Attributes
	Date                  *commonpb.Timestamp
	NextSequenceID        uint64
	NextAuditSequenceID   uint64
	NextLedgerID          uint32
	NextQueryCheckpointID uint64

	Derived                              *DerivedRegistry
	pendingSigningKeyUpdates             []signingKeyUpdate
	pendingSigningConfigUpdate           *signingConfigUpdate
	pendingMaintenanceModeUpdate         *maintenanceModeUpdate
	pendingChapterScheduleUpdate         *string
	pendingQueryCheckpointScheduleUpdate *string
	sinkConfigChanged                    bool
	// chapters is a lazy clone of fsm.Chapters, created on first chapter access.
	// Nil means no chapter method was called — Merge() skips chapter propagation.
	// Chapter orders (CloseChapter, SealChapter, etc.) read chapter protos and mutate
	// them in-place, so the clone must happen before any read to avoid corrupting
	// the FSM's state. CreateTransaction never touches chapters, so the clone is
	// avoided on the hot path.
	chapters                       *ChapterTracker
	changedChapters                []*commonpb.Chapter
	purgeRanges                    []purgeRange
	pendingArchives                []ArchiveRequest
	pendingMetadataConvertRequests []MetadataConvertRequest

	// pendingMirrorSyncs queues mirror cursor / source-head / status
	// writes produced by applyMirrorSyncUpdate. They are drained into
	// the Pebble batch by Merge, which only runs when ProcessOrders +
	// ValidateTransientVolumes succeed — so a business-rejected order
	// in the same proposal as a mirror-sync TU never advances the
	// cursor (mirror worker bundles ingest orders + cursor TU in a
	// single proposal; without this gate the cursor would commit via
	// the failure audit batch path and the worker would skip source
	// logs on the next batch).
	pendingMirrorSyncs []MirrorSyncWrite

	// pendingLedgerDeletions holds ledger names scheduled for data cleanup during Merge.
	pendingLedgerDeletions []string

	// allVolumeUpdates includes kept + purged updates (for delta/posting cross-check).
	allVolumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]

	// keptVolumeUpdates excludes ephemeral purged entries (for post-commit Pebble verification).
	keptVolumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]

	// purgedVolumeKeys holds keys of volumes removed by ephemeral purge.
	// Used to exclude these from cross-entry post-commit verification.
	purgedVolumeKeys []domain.VolumeKey

	// transientVolumes holds unique transient (account, asset) volumes per
	// ledger, populated during Merge for inclusion in the AppliedProposal
	// entry. The asset dimension is preserved so a multi-asset account is
	// correctly described when only some of its assets are transient.
	transientVolumes map[string][]*commonpb.TouchedVolume

	// perOrderVolumeKeys[i] records the VolumeKeys touched by the order at
	// zero-based index i within the current proposal. Populated by PutVolume
	// (which reads currentOrderIndex set via BeginOrder) and drained at Merge
	// time to compute purgedByLog. Reset to length 0 between proposals so the
	// outer slice's backing array is reused; PutVolume overwrites each inner
	// slot with a fresh nil before re-growing it, so inner backing arrays are
	// not preserved.
	perOrderVolumeKeys [][]domain.VolumeKey

	// currentOrderIndex is the index passed to the most recent BeginOrder
	// call. PutVolume uses it to attribute each volume touch to the order
	// that produced it. Defaults to -1 before the first BeginOrder so that
	// out-of-band PutVolume calls (recovery, technical updates) are not
	// silently attributed to order 0.
	currentOrderIndex int

	// purgedByLog[i] is the deduplicated list of (account, asset) volumes
	// that the log produced by order i touched and that the proposal-level
	// partitionVolumes classified as purged. Computed during Merge from
	// perOrderVolumeKeys ∩ partResult.purged. Injected into each
	// LedgerLog.purged_volumes before AppendLogs.
	purgedByLog [][]*commonpb.TouchedVolume

	// bloomUpdates collects canonical keys per attribute type during Merge
	// for bloom filter updates before batch.Commit().
	bloomUpdates bloom.BloomUpdates

	// Pending query checkpoint changes for Merge.
	pendingQueryCheckpointSaves   []*raftcmdpb.QueryCheckpointState
	pendingQueryCheckpointDeletes []uint64
}

// purgeRange identifies a chapter's sequence ranges to delete from Pebble during Merge().
// Log and audit entries have independent sequence counters, so separate ranges are needed.
type purgeRange struct {
	chapterID          uint64
	startSequence      uint64 // log sequence range start
	closeSequence      uint64 // log sequence range end
	startAuditSequence uint64 // audit sequence range start
	closeAuditSequence uint64 // audit sequence range end
}

// MirrorSyncWrite captures one queued mirror-sync update. applyMirrorSyncUpdate
// builds it from a TechnicalUpdate_MirrorSync; Merge drains the queue into
// the Pebble batch via SetMirrorCursor / SetMirrorSourceHead / SetMirrorStatus /
// clearMirrorStatus. Zero-valued Cursor and SourceLogCount mean "no write"
// for that field (matches the proto convention used by MirrorSyncUpdate).
type MirrorSyncWrite struct {
	LedgerName     string
	Cursor         uint64
	SourceLogCount uint64
	ClearError     bool
	Error          *commonpb.MirrorSyncError
}

// QueueMirrorSync enqueues a mirror-sync write so it lands in Pebble only if
// the proposal's orders + transient-volume validation succeed (Merge is the
// commit gate). See pendingMirrorSyncs for context.
func (b *WriteSet) QueueMirrorSync(w MirrorSyncWrite) {
	b.pendingMirrorSyncs = append(b.pendingMirrorSyncs, w)
}

// Merge drains the WriteSet's accumulated overlay into the Pebble batch and
// applies the side effects of the proposal. `logsOrRefs` is the per-order
// output of processor.ProcessOrders (one entry per order: either a freshly
// created log or a reference back to an idempotency-matched prior log).
// Merge filters out the ReferenceSequence entries, injects the per-log
// purged_volumes subset (see purgedByLog), and writes the resulting logs to
// Pebble via AppendLogs. Pass nil when the proposal produced no orders
// (technical-only path).
func (b *WriteSet) Merge(batch *dal.WriteSession, logsOrRefs []*raftcmdpb.CreatedLogOrReference) error {
	// gen0 byte for incremental 0xFF cache writes.
	genByte := byte(b.fsm.Registry.Cache.CurrentGeneration() % 2)

	// Process Ledger updates
	ledgerUpdates, _, err := mergeAndTrackBloom(b.Derived.Ledgers, b.attrs.Ledger, batch, genByte, dal.SubAttrLedger, &b.bloomUpdates.Ledgers, "ledgers")
	if err != nil {
		return err
	}

	for _, update := range ledgerUpdates {
		if err := SaveLedger(batch, update.New); err != nil {
			return fmt.Errorf("failed to save ledger: %w", err)
		}
	}

	// Process Volume updates — volumes are handled inline (not via mergeAndTrackBloom)
	// because of the unique ephemeral purge, double-entry invariant, and sentinel checks.
	volumeUpdates, _, err := b.Derived.Volumes.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge volumes: %w", err)
	}

	// Partition volumes by persistence mode: normal (kept), ephemeral (purged), transient (skipped).
	partResult := b.partitionVolumes(volumeUpdates)

	// Write kept volumes to 0xF1 + 0xFF in one pass (shared marshaled bytes).
	if err := mergeSimpleWithCache(b.attrs.Volume, batch, genByte, dal.SubAttrVolume, partResult.kept); err != nil {
		return fmt.Errorf("failed merging volume attributes: %w", err)
	}

	for _, update := range partResult.kept {
		b.bloomUpdates.Volumes = append(b.bloomUpdates.Volumes, update.ID)
	}

	if err := b.applyEphemeralPurge(batch, genByte, partResult.purged); err != nil {
		return fmt.Errorf("failed purging ephemeral volumes: %w", err)
	}

	// Transient volumes are NOT written to 0xF1 (attributes). The in-memory
	// KeyStore and 0xFF cache are overwritten with {0, 0} — matching the
	// documented "never persisted, must be zero at end of batch" semantic.
	// Writing the cumulative update.New here would silently accumulate across
	// batches: the next GetVolume would return the prior cumulative value,
	// causing PCVs on re-touched transient cells to drift. A populated cache
	// entry (rather than a delete) is still required for any co-batched
	// proposal admitted with CacheGuaranteed.
	if err := b.zeroVolumeCache(batch, genByte, partResult.transient); err != nil {
		return fmt.Errorf("failed zeroing transient volumes in cache: %w", err)
	}

	// Trace volume partitions for sentinel diagnostics.
	if b.fsm.sentinelMode {
		b.fsm.sentinelTracer.TraceVolumeUpdates(partResult.kept, partResult.transient, partResult.purged)
	}

	// Collect unique transient (account, asset) volumes per ledger for the
	// AppliedProposal entry. Purged volumes are not aggregated here — the
	// per-log subset is computed below via buildPurgedByLog and injected
	// into each LedgerLog.purged_volumes.
	if len(partResult.transient) > 0 {
		b.transientVolumes = collectUniqueVolumes(partResult.transient)
	}

	// Defensive check: double-entry invariant (on all updates, including purged).
	if err := checkDoubleEntryInvariant(volumeUpdates); err != nil {
		return err
	}

	// Defensive check: persisted volume deltas must be balanced.
	// This includes kept volumes (written to Pebble) and ephemeral purged volumes
	// (deleted from Pebble, but their deltas must still balance).
	// Transient volumes are excluded: they are never written to Pebble and their
	// individual zero-balance is verified separately by ValidateTransientVolumes.
	if b.fsm.sentinelMode {
		persistedUpdates := make([]attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair], 0, len(partResult.kept)+len(partResult.purged))
		persistedUpdates = append(persistedUpdates, partResult.kept...)
		persistedUpdates = append(persistedUpdates, partResult.purged...)
		if err := checkDoubleEntryInvariant(persistedUpdates); err != nil {
			for _, u := range persistedUpdates {
				var oldIn, oldOut string
				if u.Old.IsDefined() && u.Old.Value() != nil {
					oldIn = u.Old.Value().GetInput().ToBigInt().String()
					oldOut = u.Old.Value().GetOutput().ToBigInt().String()
				}

				b.fsm.logger.WithFields(map[string]any{
					"ledger":    u.Key.LedgerName,
					"account":   u.Key.Account,
					"asset":     u.Key.Asset,
					"oldInput":  oldIn,
					"oldOutput": oldOut,
					"newInput":  u.New.GetInput().ToBigInt().String(),
					"newOutput": u.New.GetOutput().ToBigInt().String(),
				}).Errorf("PERSISTED VOLUME UPDATE at invariant violation")
			}

			for _, u := range partResult.transient {
				b.fsm.logger.WithFields(map[string]any{
					"ledger":  u.Key.LedgerName,
					"account": u.Key.Account,
					"asset":   u.Key.Asset,
					"input":   u.New.GetInput().ToBigInt().String(),
					"output":  u.New.GetOutput().ToBigInt().String(),
				}).Errorf("TRANSIENT VOLUME at invariant violation")
			}

			return fmt.Errorf("persisted double-entry invariant violated (transient excluded): %w", err)
		}
	}

	// Defensive check: volumes must never decrease (stale base detection).
	if b.fsm.sentinelMode {
		if err := verifyVolumeUpdateMonotonicity(volumeUpdates); err != nil {
			return err
		}
	}

	// Store both sets: all updates for delta/posting cross-check (needs purged
	// entries too), and kept-only for post-commit Pebble verification (purged
	// entries are intentionally deleted from Pebble by applyEphemeralPurge).
	b.allVolumeUpdates = volumeUpdates
	b.keptVolumeUpdates = partResult.kept

	// Fresh slice each Merge: exposed via ApplyResult and read later by
	// deduplicateVolumeUpdates, so it must not alias a reused buffer.
	b.purgedVolumeKeys = make([]domain.VolumeKey, len(partResult.purged))
	for i, purged := range partResult.purged {
		b.purgedVolumeKeys[i] = purged.Key
	}

	// Process AccountMetadata updates
	metadataUpdates, metadataDeletions, err := mergeAndTrackBloom(b.Derived.AccountMetadata, b.attrs.Metadata, batch, genByte, dal.SubAttrMetadata, &b.bloomUpdates.Metadata, "account metadata")
	if err != nil {
		return err
	}

	// Flush pending reversions to the authoritative in-memory bitset and persist only the modified words.
	type dirtyWord struct {
		ledgerName string
		wordIndex  uint64
	}

	var dirtyWords []dirtyWord

	for _, txKey := range b.Derived.PendingReversions {
		wi := b.fsm.Registry.SetReverted(txKey)
		dirtyWords = append(dirtyWords, dirtyWord{ledgerName: txKey.LedgerName, wordIndex: wi})
	}

	for _, dw := range dirtyWords {
		word := b.fsm.Registry.Reversions[dw.ledgerName].Word(dw.wordIndex)
		if err := saveReversionWord(batch, dw.ledgerName, dw.wordIndex, word); err != nil {
			return fmt.Errorf("saving reversion word for %q: %w", dw.ledgerName, err)
		}
	}

	// Process idempotency key updates (dedicated prefix, not attribute system)
	if err := b.Derived.Idempotency.Merge(batch); err != nil {
		return fmt.Errorf("failed to merge idempotency keys: %w", err)
	}

	// Process References updates
	referenceUpdates, _, err := mergeAndTrackBloom(b.Derived.References, b.attrs.References, batch, genByte, dal.SubAttrReference, &b.bloomUpdates.References, "references")
	if err != nil {
		return err
	}

	// Update per-ledger attribute counters in boundaries before merging them.
	b.updateBoundaryCounters(volumeUpdates, partResult.purged, partResult.transient, metadataUpdates, metadataDeletions, referenceUpdates)

	// Process Boundary updates (after counted attributes so counters are included).
	if _, _, err := mergeAndTrackBloom(b.Derived.Boundaries, b.attrs.Boundary, batch, genByte, dal.SubAttrBoundary, &b.bloomUpdates.Boundaries, "boundaries"); err != nil {
		return err
	}

	// Process Transaction state updates
	if _, _, err := mergeAndTrackBloom(b.Derived.Transactions, b.attrs.Transaction, batch, genByte, dal.SubAttrTransaction, &b.bloomUpdates.Transactions, "transactions"); err != nil {
		return err
	}

	// Process LedgerMetadata updates
	if _, _, err := mergeAndTrackBloom(b.Derived.LedgerMetadata, b.attrs.LedgerMetadata, batch, genByte, dal.SubAttrLedgerMetadata, &b.bloomUpdates.LedgerMetadata, "ledger metadata"); err != nil {
		return err
	}

	// Build createdLogs (skipping idempotency replays) and inject the
	// per-log purged_volumes subset before persisting. purgedByLog is
	// indexed by ORDER index — same as logsOrRefs — so the mapping uses
	// the loop's i, not the createdLogs append index.
	purgedSet := makePurgedKeySet(partResult.purged)
	b.purgedByLog = buildPurgedByLog(b.perOrderVolumeKeys, purgedSet)

	createdLogs := make([]*commonpb.Log, 0, len(logsOrRefs))
	for i, lr := range logsOrRefs {
		log := lr.GetCreatedLog()
		if log == nil {
			// Idempotency replay (ReferenceSequence) — no fresh log to
			// persist, and the prior log already carries any purged
			// accounts from its original batch.
			continue
		}

		if i < len(b.purgedByLog) && len(b.purgedByLog[i]) > 0 {
			apply := log.GetPayload().GetApply()
			if apply == nil {
				return fmt.Errorf("invariant: order %d produced purged volumes %v but its log payload is not an ApplyLedgerLog (payload=%T)",
					i, b.purgedByLog[i], log.GetPayload())
			}
			ledgerLog := apply.GetLog()
			if ledgerLog == nil {
				return fmt.Errorf("invariant: order %d produced purged volumes %v but its ApplyLedgerLog carries no LedgerLog", i, b.purgedByLog[i])
			}
			ledgerLog.PurgedVolumes = b.purgedByLog[i]
		}

		createdLogs = append(createdLogs, log)
	}

	err = AppendLogs(batch, createdLogs)
	if err != nil {
		return fmt.Errorf("failed appending pending logs: %w", err)
	}

	// Apply signing key updates to Pebble batch and in-memory KeyStore
	for _, update := range b.pendingSigningKeyUpdates {
		if update.remove {
			err := DeleteSigningKey(batch, update.keyID)
			if err != nil {
				return fmt.Errorf("deleting signing key: %w", err)
			}

			if b.fsm.keyStore != nil {
				b.fsm.keyStore.RemovePublicKey(update.keyID)
			}
		} else {
			err := SaveSigningKey(batch, update.keyID, update.publicKey, update.parentKeyID)
			if err != nil {
				return fmt.Errorf("saving signing key: %w", err)
			}

			if b.fsm.keyStore != nil {
				b.fsm.keyStore.AddPublicKey(update.keyID, update.publicKey, update.parentKeyID)
			}
		}
	}

	if b.pendingSigningConfigUpdate != nil {
		err := SaveSigningConfig(batch, b.pendingSigningConfigUpdate.requireSignatures)
		if err != nil {
			return fmt.Errorf("saving signing config: %w", err)
		}

		b.fsm.sharedState.SetRequireSignatures(b.pendingSigningConfigUpdate.requireSignatures)
	}

	if b.pendingMaintenanceModeUpdate != nil {
		err := SaveMaintenanceMode(batch, b.pendingMaintenanceModeUpdate.enabled)
		if err != nil {
			return fmt.Errorf("saving maintenance mode: %w", err)
		}

		b.fsm.sharedState.SetMaintenanceMode(b.pendingMaintenanceModeUpdate.enabled)
	}

	if b.pendingChapterScheduleUpdate != nil {
		if *b.pendingChapterScheduleUpdate == "" {
			err := batchDeleteChapterSchedule(batch)
			if err != nil {
				return fmt.Errorf("deleting chapter schedule: %w", err)
			}
		} else {
			err := SaveChapterSchedule(batch, *b.pendingChapterScheduleUpdate)
			if err != nil {
				return fmt.Errorf("saving chapter schedule: %w", err)
			}
		}

		b.fsm.Chapters.SetSchedule(*b.pendingChapterScheduleUpdate)
	}

	if b.pendingQueryCheckpointScheduleUpdate != nil {
		if *b.pendingQueryCheckpointScheduleUpdate == "" {
			err := batchDeleteQueryCheckpointSchedule(batch)
			if err != nil {
				return fmt.Errorf("deleting query checkpoint schedule: %w", err)
			}
		} else {
			err := SaveQueryCheckpointSchedule(batch, *b.pendingQueryCheckpointScheduleUpdate)
			if err != nil {
				return fmt.Errorf("saving query checkpoint schedule: %w", err)
			}
		}

		b.fsm.setQueryCheckpointSchedule(*b.pendingQueryCheckpointScheduleUpdate)
	}

	// Process SinkConfig updates
	if _, _, err := mergeAndTrackBloom(b.Derived.SinkConfigs, b.attrs.SinkConfig, batch, genByte, dal.SubAttrSinkConfig, &b.bloomUpdates.SinkConfigs, "sink configs"); err != nil {
		return err
	}

	// Process NumscriptVersion updates
	if _, _, err := mergeAndTrackBloom(b.Derived.NumscriptVersions, b.attrs.NumscriptVersion, batch, genByte, dal.SubAttrNumscriptVersion, &b.bloomUpdates.NumscriptVersions, "numscript versions"); err != nil {
		return err
	}

	// Process NumscriptContent updates
	if _, _, err := mergeAndTrackBloom(b.Derived.NumscriptContents, b.attrs.NumscriptContent, batch, genByte, dal.SubAttrNumscriptContent, &b.bloomUpdates.NumscriptContents, "numscript contents"); err != nil {
		return err
	}

	// Process PreparedQuery updates
	if _, _, err := mergeAndTrackBloom(b.Derived.PreparedQueries, b.attrs.PreparedQuery, batch, genByte, dal.SubAttrPreparedQuery, &b.bloomUpdates.PreparedQueries, "prepared queries"); err != nil {
		return err
	}

	for _, p := range b.changedChapters {
		err := StoreChapter(batch, p)
		if err != nil {
			return fmt.Errorf("storing chapter %d: %w", p.GetId(), err)
		}
	}

	// Persist next chapter ID only if chapters were touched.
	if b.chapters != nil {
		if err := StoreNextChapterID(batch, b.chapters.NextChapterID()); err != nil {
			return fmt.Errorf("storing next chapter ID: %w", err)
		}
	}

	// Purge archived chapter data (logs + audit entries) if requested
	for i := range b.purgeRanges {
		err := b.executePurge(batch, &b.purgeRanges[i])
		if err != nil {
			return fmt.Errorf("purging archived chapter %d data: %w", b.purgeRanges[i].chapterID, err)
		}
	}

	// Process query checkpoint writes/deletes
	for _, cp := range b.pendingQueryCheckpointSaves {
		if err := saveQueryCheckpoint(batch, cp); err != nil {
			return fmt.Errorf("saving query checkpoint %d: %w", cp.GetCheckpointId(), err)
		}
	}

	for _, cpID := range b.pendingQueryCheckpointDeletes {
		if err := deleteQueryCheckpointFromBatch(batch, cpID); err != nil {
			return fmt.Errorf("deleting query checkpoint %d: %w", cpID, err)
		}
	}

	for _, w := range b.pendingMirrorSyncs {
		if w.Cursor > 0 {
			if err := SetMirrorCursor(batch, w.LedgerName, w.Cursor); err != nil {
				return fmt.Errorf("setting mirror cursor for %q: %w", w.LedgerName, err)
			}
		}

		if w.SourceLogCount > 0 {
			if err := SetMirrorSourceHead(batch, w.LedgerName, w.SourceLogCount); err != nil {
				return fmt.Errorf("setting mirror source head for %q: %w", w.LedgerName, err)
			}
		}

		if w.ClearError {
			if err := clearMirrorStatus(batch, w.LedgerName); err != nil {
				return fmt.Errorf("clearing mirror status for %q: %w", w.LedgerName, err)
			}
		} else if w.Error != nil {
			if err := SetMirrorStatus(batch, w.LedgerName, w.Error); err != nil {
				return fmt.Errorf("setting mirror status for %q: %w", w.LedgerName, err)
			}
		}
	}

	// Register pending ledger data cleanups (deferred to purge time).
	// Find the delete sequence for each pending deletion from the logs.
	if len(b.pendingLedgerDeletions) > 0 {
		deleteSequences := make(map[string]uint64, len(b.pendingLedgerDeletions))

		for _, log := range createdLogs {
			if dl := log.GetPayload().GetDeleteLedger(); dl != nil {
				deleteSequences[dl.GetName()] = log.GetSequence()
			}
		}

		for _, ledgerName := range b.pendingLedgerDeletions {
			seq := deleteSequences[ledgerName]

			if _, err := b.GetLedger(ledgerName); err != nil {
				// The ledger name comes from a DeleteLedger order the
				// processor already validated against b.GetLedger — a
				// miss here means the WriteSet's view of ledgers became
				// inconsistent between order processing and Merge. Fail
				// loudly instead of skipping the cleanup write.
				return fmt.Errorf("invariant: pending ledger deletion for %q but ledger not in WriteSet view", ledgerName)
			}

			if err := savePendingLedgerCleanup(batch, ledgerName, seq); err != nil {
				return fmt.Errorf("saving pending ledger cleanup for %q: %w", ledgerName, err)
			}

			b.fsm.State.PendingLedgerCleanups[ledgerName] = seq

			// Boundary deletion is handled above via boundaryDeletions
			// (MarkLedgerForCleanup adds a Delete to the Derived.Boundaries overlay).

			// Clean in-memory reversion bitset and Pebble words — not needed after deletion.
			delete(b.fsm.Registry.Reversions, ledgerName)

			if err := deleteReversionsByLedger(batch, ledgerName); err != nil {
				return fmt.Errorf("deleting reversions for %q: %w", ledgerName, err)
			}
		}
	}

	// Persist next query checkpoint ID if it changed.
	if b.NextQueryCheckpointID != b.fsm.State.NextQueryCheckpointID {
		if err := storeNextQueryCheckpointID(batch, b.NextQueryCheckpointID); err != nil {
			return fmt.Errorf("storing next query checkpoint ID: %w", err)
		}
	}

	// Persist next ledger ID if it changed.
	if b.NextLedgerID != b.fsm.State.NextLedgerID {
		if err := saveNextLedgerID(batch, b.NextLedgerID); err != nil {
			return fmt.Errorf("storing next ledger ID: %w", err)
		}
	}

	b.fsm.State.NextSequenceID = b.NextSequenceID
	b.fsm.State.NextLedgerID = b.NextLedgerID
	b.fsm.State.NextQueryCheckpointID = b.NextQueryCheckpointID

	// Apply changed chapters to Machine's Chapters tracker
	for _, p := range b.changedChapters {
		b.fsm.Chapters.PutChapter(p)
	}

	// Remove purged chapters from memory
	for _, pr := range b.purgeRanges {
		b.fsm.Chapters.DeleteChapter(pr.chapterID)
	}

	// Propagate chapter tracker state only if chapters were touched (lazy clone occurred).
	// On the hot transaction path (CreateTransaction, etc.), b.chapters stays nil
	// and the FSM's tracker is already correct.
	if b.chapters != nil {
		b.fsm.Chapters.SetCurrentOpenChapter(b.chapters.CurrentOpenChapter())
		b.fsm.Chapters.SetClosingChapters(b.chapters.ClosingChapters())
		b.fsm.Chapters.SetNextChapterID(b.chapters.NextChapterID())
	}

	return nil
}

func NewWriteSet(fsm *Machine) *WriteSet {
	return &WriteSet{
		fsm:               fsm,
		attrs:             fsm.Registry.Attrs,
		Derived:           NewDerivedRegistry(fsm.Registry),
		currentOrderIndex: -1,
	}
}

// Reset prepares the WriteSet for a new proposal, clearing all per-proposal
// state while preserving allocated maps and slice backing arrays. The
// coverage gate lives one layer up on gatedScope; WriteSet itself is the
// raw engine (Derived → KeyStore → cache).
func (b *WriteSet) Reset(at *commonpb.Timestamp) {
	b.Date = at
	b.NextSequenceID = b.fsm.State.NextSequenceID
	b.NextAuditSequenceID = b.fsm.State.NextAuditSequenceID
	b.NextLedgerID = b.fsm.State.NextLedgerID
	b.NextQueryCheckpointID = b.fsm.State.NextQueryCheckpointID
	b.Derived.Reset()

	b.pendingSigningKeyUpdates = b.pendingSigningKeyUpdates[:0]
	b.pendingSigningConfigUpdate = nil
	b.pendingMaintenanceModeUpdate = nil
	b.pendingChapterScheduleUpdate = nil
	b.pendingQueryCheckpointScheduleUpdate = nil
	b.sinkConfigChanged = false
	b.chapters = nil
	b.changedChapters = b.changedChapters[:0]
	b.purgeRanges = b.purgeRanges[:0]
	b.pendingArchives = b.pendingArchives[:0]
	b.pendingMetadataConvertRequests = b.pendingMetadataConvertRequests[:0]
	b.pendingMirrorSyncs = b.pendingMirrorSyncs[:0]
	b.pendingLedgerDeletions = b.pendingLedgerDeletions[:0]
	b.allVolumeUpdates = b.allVolumeUpdates[:0]
	b.keptVolumeUpdates = b.keptVolumeUpdates[:0]
	b.transientVolumes = nil
	// Outer-only truncate: PutVolume below overwrites each inner slot with a
	// fresh nil before re-growing, so an inner [:0] loop would be wasted work
	// — the preserved [:0] slices are clobbered on the next PutVolume.
	b.perOrderVolumeKeys = b.perOrderVolumeKeys[:0]
	b.currentOrderIndex = -1
	for i := range b.purgedByLog {
		b.purgedByLog[i] = nil
	}
	b.purgedByLog = b.purgedByLog[:0]
	b.bloomUpdates.Reset()
	b.pendingQueryCheckpointSaves = b.pendingQueryCheckpointSaves[:0]
	b.pendingQueryCheckpointDeletes = b.pendingQueryCheckpointDeletes[:0]
}

// Engine surface: the read/write/counter/chapter methods that gatedScope
// forwards via embedding. The coverage gate method (CheckCoverage) is
// deliberately absent here — it lives on gatedScope, which embeds
// *WriteSet and overrides the cache-attribute Get* to insert the gate.

func (b *WriteSet) GetLedger(name string) (*commonpb.LedgerInfo, error) {
	info, err := b.Derived.Ledgers.Get(domain.LedgerKey{Name: name})
	if err != nil {
		return nil, err
	}

	if info == nil {
		return nil, domain.ErrNotFound
	}

	return info, nil
}

func (b *WriteSet) PutLedger(name string, info *commonpb.LedgerInfo) {
	b.Derived.Ledgers.Put(domain.LedgerKey{Name: name}, info)
}

func (b *WriteSet) MarkLedgerForCleanup(ledger string) {
	b.pendingLedgerDeletions = append(b.pendingLedgerDeletions, ledger)
	// Remove boundary from the in-memory overlay so that subsequent
	// GetBoundaries calls return domain.ErrNotFound — both within this
	// proposal and in future proposals after Merge propagates the deletion.
	b.Derived.Boundaries.Delete(domain.LedgerKey{Name: ledger})
}

func (b *WriteSet) GetBoundaries(ledger string) (raftcmdpb.LedgerBoundariesReader, error) {
	boundaries, err := b.Derived.Boundaries.Get(domain.LedgerKey{Name: ledger})
	if err != nil {
		return nil, err
	}

	if boundaries == nil {
		return nil, domain.ErrNotFound
	}

	return boundaries.AsReader(), nil
}

func (b *WriteSet) ResolveNumscriptContent(ledgerName string, name, version string) (*commonpb.NumscriptInfo, error) {
	return b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: version})
}

func (b *WriteSet) PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
	b.Derived.Boundaries.Put(domain.LedgerKey{Name: ledger}, boundaries)
}

func (b *WriteSet) GetVolume(key domain.VolumeKey) (raftcmdpb.VolumePairReader, error) {
	v, err := b.Derived.Volumes.Get(key)
	if err != nil || v == nil {
		return nil, err
	}

	return v.AsReader(), nil
}

func (b *WriteSet) PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
	b.Derived.Volumes.Put(key, value)

	// Record the touch under the current order so Merge can compute the
	// per-log subset of purged ephemeral accounts (see purgedByLog).
	// currentOrderIndex < 0 means PutVolume is being called outside of an
	// order — e.g. recovery, technical updates, ValidateTransientVolumes —
	// where per-log attribution is meaningless; skip silently.
	if b.currentOrderIndex < 0 {
		return
	}

	for len(b.perOrderVolumeKeys) <= b.currentOrderIndex {
		b.perOrderVolumeKeys = append(b.perOrderVolumeKeys, nil)
	}
	b.perOrderVolumeKeys[b.currentOrderIndex] = append(b.perOrderVolumeKeys[b.currentOrderIndex], key)
}

// BeginOrder tags subsequent PutVolume calls with the given zero-based order
// index. Called by ProcessOrders before each handler invocation so the
// WriteSet can attribute volume touches to the order that produced them. See
// purgedByLog for how the recorded touches are consumed at Merge time.
func (b *WriteSet) BeginOrder(orderIndex int) {
	b.currentOrderIndex = orderIndex
}

// ValidateTransientVolumes checks that all transient account volumes have zero balance.
// Must be called after ProcessOrders and before Commit, so that failures are
// treated as business errors (rejected proposals) rather than fatal FSM errors.
//
// The end-of-batch zero-balance check only applies when the base volume (before
// this batch) is itself zero-balance or absent — the steady-state transient case.
// Pre-existing non-zero volumes (from before the transient pattern started matching
// the account) are exempt: partitionVolumes routes those batches to the ephemeral-
// mirror lifecycle (kept while unbalanced, purged once the running cumulative
// returns to zero), so a balance check here would double-up with that flow.
//
// The scope parameter is the gated processing.Scope the rest of the proposal
// used — coverage checks on ledger reads here go through the same gate as
// every handler-level read so a missing ledger declaration surfaces as
// *ErrCoverageMiss instead of an opaque "ledger not found" skip.
func (b *WriteSet) ValidateTransientVolumes(scope processing.Scope) domain.Describable {
	ledgerTypes := make(map[string][]accounttype.CompiledType)

	for key, vol := range b.Derived.Volumes.DirtyValues() {
		compiled, ok := ledgerTypes[key.LedgerName]
		if !ok {
			info, err := scope.GetLedger(key.LedgerName)
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}

			if err != nil {
				return &domain.ErrStorageOperation{Operation: "loading ledger for transient volume validation", Cause: err}
			}

			compiled = accounttype.CompileTypes(info.GetAccountTypes())
			ledgerTypes[key.LedgerName] = compiled
		}

		matched := accounttype.FindMatchingType(key.Account, compiled)
		if matched == nil || matched.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			continue
		}

		// Check if the parent KeyStore has a pre-existing non-zero volume.
		// If so, the account had volumes before being marked transient —
		// skip the zero-balance assertion.
		//
		// We need the BASE volume (pre-batch), not the in-batch overlay,
		// so we read via Derived.Volumes.Parent() rather than scope.
		// The gate is enforced explicitly via scope.CheckCoverage to
		// preserve the coverage invariant on this otherwise-engine-
		// internal read.
		if err := scope.CheckCoverage(dal.SubAttrVolume, key.Bytes()); err != nil {
			return &domain.ErrStorageOperation{Operation: "coverage check on transient base volume", Cause: err}
		}

		baseVol, _, baseErr := b.Derived.Volumes.Parent().GetKey(key)
		if baseErr == nil && !isVolumeZeroBalance(baseVol) {
			continue
		}

		if !isVolumeZeroBalance(vol) {
			return &domain.ErrTransientAccountNonZero{
				Account: key.Account,
				Asset:   key.Asset,
			}
		}
	}

	return nil
}

func (b *WriteSet) GetAccountMetadata(key domain.MetadataKey) (*commonpb.MetadataValue, error) {
	return b.Derived.AccountMetadata.Get(key)
}

// GetAccountMetadataEntry returns the raw cache Entry (tombstone flag
// exposed) for an account metadata canonical key. The metadata-conversion
// path inspects entry.Deleted to skip stale conversions for keys deleted
// after the converter scan. Coverage is enforced by the gatedScope
// wrapper above the engine, not here.
func (b *WriteSet) GetAccountMetadataEntry(canonical []byte) (attributes.Entry[*commonpb.MetadataValue], error) {
	entry, ok := b.fsm.Registry.AccountMetadata.KeyStore().GetEntry(canonical)
	if !ok {
		return attributes.Entry[*commonpb.MetadataValue]{}, domain.ErrNotFound
	}

	return entry, nil
}

func (b *WriteSet) PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue) {
	b.Derived.AccountMetadata.Put(key, value)
}

func (b *WriteSet) DeleteAccountMetadata(key domain.MetadataKey) {
	b.Derived.AccountMetadata.Delete(key)
}

func (b *WriteSet) GetLedgerMetadata(key domain.LedgerMetadataKey) (*commonpb.MetadataValue, error) {
	return b.Derived.LedgerMetadata.Get(key)
}

// GetLedgerMetadataEntry returns the raw cache Entry for a ledger
// metadata canonical key. Mirrors GetAccountMetadataEntry for the
// per-ledger metadata zone. Coverage is enforced by the gatedScope
// wrapper above the engine, not here.
func (b *WriteSet) GetLedgerMetadataEntry(canonical []byte) (attributes.Entry[*commonpb.MetadataValue], error) {
	entry, ok := b.fsm.Registry.LedgerMetadata.KeyStore().GetEntry(canonical)
	if !ok {
		return attributes.Entry[*commonpb.MetadataValue]{}, domain.ErrNotFound
	}

	return entry, nil
}

func (b *WriteSet) PutLedgerMetadata(key domain.LedgerMetadataKey, value *commonpb.MetadataValue) {
	b.Derived.LedgerMetadata.Put(key, value)
}

func (b *WriteSet) DeleteLedgerMetadata(key domain.LedgerMetadataKey) {
	b.Derived.LedgerMetadata.Delete(key)
}

func (b *WriteSet) GetReverted(key domain.TransactionKey) (bool, error) {
	return b.Derived.GetReverted(key), nil
}

func (b *WriteSet) PutReverted(key domain.TransactionKey, reverted bool) {
	if reverted {
		b.Derived.PutReverted(key)
	}
}

func (b *WriteSet) GetIdempotencyKey(key domain.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error) {
	value, err := b.Derived.Idempotency.Get(key.Key)
	if err != nil || value == nil {
		return nil, err
	}

	// Check TTL expiration: treat expired keys as not found.
	if b.fsm.Registry.Idempotency.IsExpired(value, b.Date.GetData()) {
		return nil, nil
	}

	return value, nil
}

func (b *WriteSet) PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	value.CreatedAt = b.Date.GetData() // HLC timestamp
	b.Derived.Idempotency.Put(key.Key, value)
}

func (b *WriteSet) GetTransactionReference(key domain.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error) {
	return b.Derived.References.Get(key)
}

func (b *WriteSet) PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	b.Derived.References.Put(key, value)
}

func (b *WriteSet) GetTransactionState(key domain.TransactionKey) (*commonpb.TransactionState, error) {
	return b.Derived.Transactions.Get(key)
}

func (b *WriteSet) PutTransactionState(key domain.TransactionKey, state *commonpb.TransactionState) {
	b.Derived.Transactions.Put(key, state)
}

func (b *WriteSet) AddSigningKey(keyID string, publicKey []byte, parentKeyID string) {
	b.pendingSigningKeyUpdates = append(b.pendingSigningKeyUpdates, signingKeyUpdate{
		keyID:       keyID,
		publicKey:   publicKey,
		parentKeyID: parentKeyID,
	})
}

func (b *WriteSet) RemoveSigningKey(keyID string) {
	b.pendingSigningKeyUpdates = append(b.pendingSigningKeyUpdates, signingKeyUpdate{
		keyID:  keyID,
		remove: true,
	})
}

// GetSigningKeyChildren returns all key IDs that have keyID as their parent.
// It checks the committed KeyStore and accounts for pending additions/removals.
func (b *WriteSet) GetSigningKeyChildren(keyID string) []string {
	// Start from committed state
	children := b.fsm.keyStore.GetChildren(keyID)

	// Build a set of pending removals for fast lookup
	pendingRemovals := make(map[string]struct{})

	for _, update := range b.pendingSigningKeyUpdates {
		if update.remove {
			pendingRemovals[update.keyID] = struct{}{}
		}
	}

	// Filter out pending removals from committed children
	filtered := children[:0]
	for _, child := range children {
		if _, removed := pendingRemovals[child]; !removed {
			filtered = append(filtered, child)
		}
	}

	// Add pending additions whose parent matches
	for _, update := range b.pendingSigningKeyUpdates {
		if !update.remove && update.parentKeyID == keyID {
			if _, removed := pendingRemovals[update.keyID]; !removed {
				filtered = append(filtered, update.keyID)
			}
		}
	}

	return filtered
}

func (b *WriteSet) SetRequireSignatures(require bool) {
	b.pendingSigningConfigUpdate = &signingConfigUpdate{
		requireSignatures: require,
	}
}

func (b *WriteSet) SetMaintenanceMode(enabled bool) {
	b.pendingMaintenanceModeUpdate = &maintenanceModeUpdate{
		enabled: enabled,
	}
}

func (b *WriteSet) SetChapterSchedule(cronExpr string) {
	b.pendingChapterScheduleUpdate = &cronExpr
}

func (b *WriteSet) DeleteChapterSchedule() {
	empty := ""
	b.pendingChapterScheduleUpdate = &empty
}

func (b *WriteSet) SetQueryCheckpointSchedule(cronExpr string) {
	b.pendingQueryCheckpointScheduleUpdate = &cronExpr
}

func (b *WriteSet) DeleteQueryCheckpointSchedule() {
	empty := ""
	b.pendingQueryCheckpointScheduleUpdate = &empty
}

func (b *WriteSet) GetSinkConfig(name string) (*commonpb.SinkConfig, error) {
	cfg, err := b.Derived.SinkConfigs.Get(domain.SinkConfigKey{Name: name})
	if err != nil {
		return nil, nil
	}

	return cfg, nil
}

func (b *WriteSet) AddSinkConfig(config *commonpb.SinkConfig) {
	b.Derived.SinkConfigs.Put(domain.SinkConfigKey{Name: config.GetName()}, config)
	b.sinkConfigChanged = true
}

func (b *WriteSet) RemoveSinkConfig(name string) {
	b.Derived.SinkConfigs.Delete(domain.SinkConfigKey{Name: name})
	b.sinkConfigChanged = true
}

func (b *WriteSet) HasPendingSinkChanges() bool {
	return b.sinkConfigChanged
}

// AllVolumeUpdates returns all volume updates (kept + purged) captured during Merge.
// Used for delta/posting cross-check which needs purged ephemeral entries too.
func (b *WriteSet) AllVolumeUpdates() []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	return b.allVolumeUpdates
}

// KeptVolumeUpdates returns only kept volume updates (excluding ephemeral purges).
// Used for post-commit Pebble verification where purged entries are intentionally absent.
func (b *WriteSet) KeptVolumeUpdates() []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	return b.keptVolumeUpdates
}

func (b *WriteSet) GetNextSequenceID() uint64 {
	return b.NextSequenceID
}

func (b *WriteSet) GetNextAuditSequenceID() uint64 {
	return b.NextAuditSequenceID
}

func (b *WriteSet) IncrementNextSequenceID() uint64 {
	id := b.NextSequenceID
	b.NextSequenceID++

	return id
}

func (b *WriteSet) GetNextLedgerID() uint32 {
	return b.NextLedgerID
}

func (b *WriteSet) IncrementNextLedgerID() uint32 {
	id := b.NextLedgerID
	b.NextLedgerID++

	return id
}

func (b *WriteSet) GetDate() *commonpb.Timestamp {
	return b.Date
}

// SetDate updates the proposal date late in the apply cycle. The technical-
// update phase runs with `proposal.GetDate()` (raw, no HLC advance); when
// orders follow, applyProposal computes the HLC-advanced effective date and
// pushes it here so order handlers see the monotonic timestamp. The overlay
// (Derived) is preserved — only the timestamp field is rewired.
func (b *WriteSet) SetDate(date *commonpb.Timestamp) {
	b.Date = date
}

// addVolumeSideDelta extracts the net delta for one side (input or output) of a VolumePair update.
// Known values are always non-nil (preloaders send explicit 0).
// Uses the provided tmp and scratch uint256.Ints for intermediate computations to avoid heap allocations.
func addVolumeSideDelta(acc *uint256.Int, tmp *uint256.Int, scratch *uint256.Int, newKnown, oldKnown *commonpb.Uint256) {
	newKnown.IntoUint256(tmp)

	if oldKnown != nil {
		oldKnown.IntoUint256(scratch)
		tmp.Sub(tmp, scratch)
	}

	acc.Add(acc, tmp)
}

// checkDoubleEntryInvariant verifies that the sum of input deltas equals the sum of output deltas.
// This is a fundamental accounting invariant: every posting moves the same amount from a source
// account (output) to a destination account (input), so the totals must always balance.
func checkDoubleEntryInvariant(
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) error {
	var (
		inputSum  uint256.Int
		outputSum uint256.Int
		tmp       uint256.Int
		scratch   uint256.Int
	)

	for _, update := range volumeUpdates {
		var oldInput, oldOutput *commonpb.Uint256

		if update.Old.IsDefined() {
			if old := update.Old.Value(); old != nil {
				oldInput = old.GetInput()
				oldOutput = old.GetOutput()
			}
		}

		addVolumeSideDelta(&inputSum, &tmp, &scratch, update.New.GetInput(), oldInput)
		addVolumeSideDelta(&outputSum, &tmp, &scratch, update.New.GetOutput(), oldOutput)
	}

	if !inputSum.Eq(&outputSum) {
		return &ErrDoubleEntryInvariantViolated{
			InputSum:  inputSum.Dec(),
			OutputSum: outputSum.Dec(),
		}
	}

	return nil
}

// Chapter operations

// ensureChapters clones the FSM's ChapterTracker on first access.
// Chapter orders (CloseChapter, SealChapter, etc.) read chapter protos and mutate
// them in-place, so the clone must happen before any read to protect the FSM.
// CreateTransaction never calls chapter methods, so this is never triggered on
// the hot transaction path.
func (b *WriteSet) ensureChapters() {
	if b.chapters == nil {
		b.chapters = b.fsm.Chapters.Clone()
	}
}

func (b *WriteSet) GetCurrentOpenChapter() (*commonpb.Chapter, bool) {
	b.ensureChapters()

	p := b.chapters.CurrentOpenChapter()
	if p != nil {
		return p, true
	}

	return nil, false
}

func (b *WriteSet) GetClosingChapters() []*commonpb.Chapter {
	b.ensureChapters()

	return b.chapters.ClosingChapters()
}

func (b *WriteSet) GetClosingChapterByID(chapterID uint64) (*commonpb.Chapter, bool) {
	b.ensureChapters()

	return b.chapters.ClosingChapterByID(chapterID)
}

func (b *WriteSet) SetCurrentOpenChapter(chapter *commonpb.Chapter) {
	b.ensureChapters()
	b.chapters.SetCurrentOpenChapter(chapter)
	b.changedChapters = append(b.changedChapters, chapter)
}

func (b *WriteSet) AddClosingChapter(chapter *commonpb.Chapter) {
	b.ensureChapters()
	b.chapters.AddClosingChapter(chapter)
	b.changedChapters = append(b.changedChapters, chapter)
}

// RemoveClosingChapter persists the closing chapter's final state and removes it from in-memory tracking.
func (b *WriteSet) RemoveClosingChapter(chapterID uint64) {
	b.ensureChapters()

	if closing, ok := b.chapters.ClosingChapterByID(chapterID); ok {
		b.changedChapters = append(b.changedChapters, closing)
	}

	b.chapters.RemoveClosingChapter(chapterID)
}

func (b *WriteSet) GetNextChapterID() uint64 {
	b.ensureChapters()

	return b.chapters.NextChapterID()
}

func (b *WriteSet) IncrementNextChapterID() uint64 {
	b.ensureChapters()

	id := b.chapters.NextChapterID()
	b.chapters.SetNextChapterID(id + 1)

	return id
}

// GetChapterByID looks up a chapter by ID from in-memory state only.
// It checks changedChapters first (most recent modifications), then the chapters tracker.
func (b *WriteSet) GetChapterByID(chapterID uint64) (*commonpb.Chapter, bool) {
	// Check changedChapters (most recently changed first)
	for i := len(b.changedChapters) - 1; i >= 0; i-- {
		if b.changedChapters[i].GetId() == chapterID {
			return b.changedChapters[i], true
		}
	}

	b.ensureChapters()

	return b.chapters.GetChapterByID(chapterID)
}

// UpdateChapter records a chapter modification to be persisted in Merge().
func (b *WriteSet) UpdateChapter(chapter *commonpb.Chapter) {
	b.changedChapters = append(b.changedChapters, chapter)
}

// SetPurgeRange records sequence ranges to be purged during Merge().
// Log and audit entries have independent sequence counters (audit advances
// slower due to batching), so both ranges are needed for correct purging.
func (b *WriteSet) SetPurgeRange(chapterID, startSequence, closeSequence, startAuditSequence, closeAuditSequence uint64) {
	b.purgeRanges = append(b.purgeRanges, purgeRange{
		chapterID:          chapterID,
		startSequence:      startSequence,
		closeSequence:      closeSequence,
		startAuditSequence: startAuditSequence,
		closeAuditSequence: closeAuditSequence,
	})
}

// SetPendingArchive records a chapter that needs archiving after the batch is committed.
// The Machine reads this after Merge() to construct and send the ArchiveRequest.
// Can be called multiple times to archive multiple chapters in the same batch.
func (b *WriteSet) SetPendingArchive(chapterID, startSequence, closeSequence, startAuditSequence, closeAuditSequence uint64) {
	b.pendingArchives = append(b.pendingArchives, ArchiveRequest{
		ChapterID:          chapterID,
		StartSequence:      startSequence,
		CloseSequence:      closeSequence,
		StartAuditSequence: startAuditSequence,
		CloseAuditSequence: closeAuditSequence,
	})
}

// executePurge deletes cold-storable data for a single purge range.
// It also cleans up per-ledger data for any deleted ledgers whose
// DeleteLedger log falls within the purge range.
func (b *WriteSet) executePurge(batch *dal.WriteSession, pr *purgeRange) error {
	// Logs: purge using log sequence range.
	logStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(pr.startSequence).Build()
	logEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(pr.closeSequence + 1).Build()

	if err := batch.DeleteRange(logStart, logEnd, nil); err != nil {
		return fmt.Errorf("purging logs [%d, %d]: %w", pr.startSequence, pr.closeSequence, err)
	}

	// Audit: purge using audit sequence range (independent counter, advances slower).
	if pr.closeAuditSequence >= pr.startAuditSequence {
		auditStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(pr.startAuditSequence).Build()
		auditEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(pr.closeAuditSequence + 1).Build()

		if err := batch.DeleteRange(auditStart, auditEnd, nil); err != nil {
			return fmt.Errorf("purging audit [%d, %d]: %w", pr.startAuditSequence, pr.closeAuditSequence, err)
		}

		// AppliedProposal entries share the audit sequence counter (1:1 with
		// AuditEntry on the success path). Failed proposals leave gaps but
		// DeleteRange tolerates them.
		proposalStart := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(pr.startAuditSequence).Build()
		proposalEnd := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).PutUint64(pr.closeAuditSequence + 1).Build()

		if err := batch.DeleteRange(proposalStart, proposalEnd, nil); err != nil {
			return fmt.Errorf("purging applied proposals [%d, %d]: %w", pr.startAuditSequence, pr.closeAuditSequence, err)
		}
	}

	// Clean up per-ledger data for deleted ledgers whose delete log
	// falls within this purge range.
	for ledgerName, deleteSeq := range b.fsm.State.PendingLedgerCleanups {
		if deleteSeq >= pr.startSequence && deleteSeq <= pr.closeSequence {
			if err := deleteLedgerData(batch, ledgerName); err != nil {
				return fmt.Errorf("purging ledger data for ledger %q: %w", ledgerName, err)
			}

			if err := deletePendingLedgerCleanup(batch, ledgerName); err != nil {
				return fmt.Errorf("removing pending cleanup entry for ledger %q: %w", ledgerName, err)
			}

			delete(b.fsm.State.PendingLedgerCleanups, ledgerName)

			// Liveness anchor for deleted-ledger-data-isolation-and-eventual-purge:
			// the deferred cleanup recorded at DeleteLedger apply time is only
			// consumed here, when a covering purge range (chapter archival
			// confirmation) reaches the delete sequence. The chapter-close
			// singleton driver closes/archives/confirms chapters continuously
			// and ledger-delete drivers run in parallel, so this branch is
			// expected to be exercised in every full run.
			assert.Reachable("deleted ledger deferred cleanup executed by covering purge", map[string]any{
				"ledger":    ledgerName,
				"deleteSeq": deleteSeq,
				"chapterId": pr.chapterID,
			})
		}
	}

	return nil
}

func (b *WriteSet) AddMetadataConvertRequest(ledgerName string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType) {
	b.pendingMetadataConvertRequests = append(b.pendingMetadataConvertRequests, MetadataConvertRequest{
		LedgerName: ledgerName,
		TargetType: targetType,
		Key:        key,
		Type:       metadataType,
	})
}

// MetadataConvertRequests returns the accumulated metadata conversion requests.
func (b *WriteSet) MetadataConvertRequests() []MetadataConvertRequest {
	return b.pendingMetadataConvertRequests
}

// HasPurges returns true if the buffer contains any pending purge ranges.
func (b *WriteSet) HasPurges() bool {
	return len(b.purgeRanges) > 0
}

func (b *WriteSet) GetPreparedQuery(ledgerName string, name string) (*commonpb.PreparedQuery, error) {
	pq, err := b.Derived.PreparedQueries.Get(domain.PreparedQueryKey{LedgerName: ledgerName, Name: name})
	// Treat a cache miss as "doesn't exist". A delete in an earlier entry of
	// the same batch will have cleared the cache
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}

	return pq, err
}

func (b *WriteSet) PutPreparedQuery(ledgerName string, pq *commonpb.PreparedQuery) {
	b.Derived.PreparedQueries.Put(domain.PreparedQueryKey{LedgerName: ledgerName, Name: pq.GetName()}, pq)
}

func (b *WriteSet) DeletePreparedQuery(ledgerName string, name string) {
	b.Derived.PreparedQueries.Delete(domain.PreparedQueryKey{LedgerName: ledgerName, Name: name})
}

// Numscript library operations

func (b *WriteSet) GetNumscriptLatestVersion(ledgerName string, name string) (string, error) {
	val, err := b.Derived.NumscriptVersions.Get(domain.NumscriptVersionKey{LedgerName: ledgerName, Name: name})
	if err != nil || val == nil {
		return "", err
	}

	return val.GetVersion(), nil
}

func (b *WriteSet) PutNumscript(ledgerName string, info *commonpb.NumscriptInfo) {
	b.Derived.NumscriptVersions.Put(domain.NumscriptVersionKey{LedgerName: ledgerName, Name: info.GetName()}, &commonpb.NumscriptVersionValue{Version: info.GetVersion()})
	b.Derived.NumscriptContents.Put(domain.NumscriptEntryKey{LedgerName: ledgerName, Name: info.GetName(), Version: info.GetVersion()}, info)
}

func (b *WriteSet) DeleteNumscriptLatest(ledgerName string, name string) {
	b.Derived.NumscriptVersions.Put(domain.NumscriptVersionKey{LedgerName: ledgerName, Name: name}, &commonpb.NumscriptVersionValue{})
}

func (b *WriteSet) NumscriptVersionExists(ledgerName string, name, version string) (bool, error) {
	info, err := b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: version})
	if err != nil {
		// Not in cache — treat as not existing (admission ensures preloading)
		return false, nil
	}

	return info != nil, nil
}

func (b *WriteSet) GetNextQueryCheckpointID() uint64 {
	return b.NextQueryCheckpointID
}

func (b *WriteSet) IncrementNextQueryCheckpointID() uint64 {
	id := b.NextQueryCheckpointID
	b.NextQueryCheckpointID++

	return id
}

// SaveQueryCheckpoint stores a query checkpoint for Merge.
func (b *WriteSet) SaveQueryCheckpoint(cp *raftcmdpb.QueryCheckpointState) {
	b.pendingQueryCheckpointSaves = append(b.pendingQueryCheckpointSaves, cp)
}

// DeleteQueryCheckpoint marks a query checkpoint for deletion during Merge.
func (b *WriteSet) DeleteQueryCheckpoint(checkpointID uint64) {
	b.pendingQueryCheckpointDeletes = append(b.pendingQueryCheckpointDeletes, checkpointID)
}

// BloomUpdates returns the canonical keys collected during Merge for bloom filter updates.
func (b *WriteSet) BloomUpdates() *bloom.BloomUpdates {
	return &b.bloomUpdates
}

// PurgedVolumeKeys returns the keys of volumes that were purged by ephemeral purge.
// Used to exclude these keys from post-commit Pebble verification when a later entry
// in the same ApplyEntries batch purges a volume that was written by an earlier entry.
func (b *WriteSet) PurgedVolumeKeys() []domain.VolumeKey {
	return b.purgedVolumeKeys
}

// TransientVolumes returns the unique transient (account, asset) volumes
// per ledger, collected during Merge from the transient volume partition.
func (b *WriteSet) TransientVolumes() map[string][]*commonpb.TouchedVolume {
	return b.transientVolumes
}

// collectUniqueVolumes extracts unique (account, asset) tuples per ledger
// from volume updates and emits them as deterministically-ordered
// commonpb.TouchedVolume slices.
func collectUniqueVolumes(updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[string][]*commonpb.TouchedVolume {
	type accAsset struct{ Account, Asset string }
	seen := make(map[string]map[accAsset]struct{})

	for _, update := range updates {
		ledgerName := update.Key.LedgerName
		k := accAsset{Account: update.Key.Account, Asset: update.Key.Asset}

		if seen[ledgerName] == nil {
			seen[ledgerName] = make(map[accAsset]struct{})
		}

		seen[ledgerName][k] = struct{}{}
	}

	result := make(map[string][]*commonpb.TouchedVolume, len(seen))
	for ledgerName, vols := range seen {
		list := make([]accAsset, 0, len(vols))
		for k := range vols {
			list = append(list, k)
		}

		sort.Slice(list, func(a, b int) bool {
			if list[a].Account != list[b].Account {
				return list[a].Account < list[b].Account
			}

			return list[a].Asset < list[b].Asset
		})

		out := make([]*commonpb.TouchedVolume, len(list))
		for i, k := range list {
			out[i] = &commonpb.TouchedVolume{Account: k.Account, Asset: k.Asset}
		}

		result[ledgerName] = out
	}

	return result
}

// WriteSet is the raw engine — it does NOT implement processing.Scope by
// itself (no CheckCoverage). Handlers always see a gatedScope wrapping
// a *WriteSet (built via NewScopeFactory). Keeping the coverage concept
// off the engine means a future engine implementation (or a test fake)
// cannot accidentally route around the gate.
