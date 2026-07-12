package state

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sort"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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

	Derived                       *DerivedRegistry
	pendingSigningKeyUpdates      []signingKeyUpdate
	pendingSigningConfigUpdate    *signingConfigUpdate
	pendingMaintenanceModeUpdate  *maintenanceModeUpdate
	chapterScheduleUpdate         *string
	queryCheckpointScheduleUpdate *string
	sinkConfigChanged             bool
	// chapters is a lazy clone of fsm.Chapters, created on first chapter access.
	// Nil means no chapter method was called — Merge() skips chapter propagation.
	// Chapter orders (CloseChapter, SealChapter, etc.) read chapter protos and mutate
	// them in-place, so the clone must happen before any read to avoid corrupting
	// the FSM's state. CreateTransaction never touches chapters, so the clone is
	// avoided on the hot path.
	chapters        *ChapterTracker
	changedChapters []*commonpb.Chapter
	purgeRanges     []purgeRange
	archiveRequests []ArchiveRequest

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

	// deletedLedgers holds ledger names scheduled for data cleanup during Merge.
	deletedLedgers []string

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

	// purgedByLog[i] is the deduplicated list of (account, asset) volumes
	// that the log produced by order i touched and that the proposal-level
	// partitionVolumes classified as purged. Computed during Merge from
	// volumes.Slots() ∩ partResult.purged. Injected into each
	// LedgerLog.purged_volumes before AppendLogs.
	purgedByLog [][]*commonpb.TouchedVolume

	// bloomUpdates collects canonical keys per attribute type during Merge
	// for bloom filter updates before batch.Commit().
	bloomUpdates bloom.BloomUpdates

	// Pending query checkpoint changes for Merge.
	pendingQueryCheckpointSaves   []*raftcmdpb.QueryCheckpointState
	pendingQueryCheckpointDeletes []uint64

	// chapterClosing is true when a processor emitted at least one
	// CloseChapter intent during this proposal. applyProposal reads it
	// (via ChapterClosing) AFTER the audit entry is written so the
	// LastAuditHash carry to ChapterTracker.LatestClosingChapter happens
	// in O(1) instead of by walking the log slice.
	chapterClosing bool

	// mirrorConfigChanged is true when a processor emitted a mirror-config
	// change (CreateLedger Mirror or PromoteLedger). Read by applyProposal
	// to populate ApplyResult.MirrorConfigChanged in O(1) instead of by
	// walking proposal.GetOrders().
	mirrorConfigChanged bool

	// queryCheckpointCreated / queryCheckpointDeleted hold the id of the
	// query checkpoint the proposal created / deleted (zero when none).
	// Absorb sets them from the CreatedQueryCheckpoint / DeletedQueryCheckpoint
	// log payloads, applyProposal reads them in O(1) to populate
	// ApplyResult.QueryCheckpointCreated / Deleted instead of walking
	// pendingQueryCheckpointSaves and proposal.GetOrders() respectively.
	queryCheckpointCreated uint64
	queryCheckpointDeleted uint64

	// Per-kind accessors — instantiated once at NewWriteSet and reused
	// across proposals. The Derived stores they wrap are reset between
	// proposals but the accessor pointers stay valid. Volumes wraps the
	// raw store in a recorderAccessor: every Put records the touched key
	// under the current slot (set via BeginOrder) so Merge can compute
	// the per-log subset of purged ephemeral accounts (purgedByLog).
	ledgers               *rawAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader]
	boundaries            *rawAccessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader]
	volumes               *recorderAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader]
	accountMetadata       *rawAccessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	ledgerMetadata        *rawAccessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]
	transactionReferences *rawAccessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader]
	transactionStates     *rawAccessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader]
	preparedQueries       *rawAccessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]
	indexes               *rawAccessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader]
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
//
// Merge is structured in four phases so Pebble keys are written in
// monotonically increasing order, keeping the memtable skiplist on its fast
// path and improving SST layout on flush. The phases are:
//
//  1. Overlay drain (no Pebble writes) — call derived.Merge() on each
//     DerivedKeyStore in dependency order. updateBoundaryCounters reads volume
//     / metadata / reference deltas, so those overlays drain before
//     Boundaries.
//  2. Cross-zone in-memory side effects — invariant checks, transient volume
//     collection, in-memory bitset mutation (SetReverted), purged-by-log
//     computation and per-log PurgedVolumes injection, deleteSequences map.
//  3. Pebble flush in zone+sub-prefix monotone order:
//     ZoneAttributes (0x01) + ZoneCache (0x02), sub-prefix monotone:
//     SubAttrVolume (01) → Metadata (02) → Transaction (03) → Ledger (04)
//     → Boundary (05) → Reference (06) → LedgerMetadata (07) → SinkConfig
//     (08) → NumscriptVersion (09) → NumscriptContent (0A) → PreparedQuery
//     (0B). Cache writes are emitted paired with the attribute writes via
//     mergeSimpleWithCache so the marshaled value bytes are shared.
//     ZonePerLedger (0x03): SubPLReversions (01) → MirrorSourceHead (04) →
//     MirrorCursor (05) → MirrorStatus (06).
//     ZoneCold (0x04): SubColdLog (01) via AppendLogs. SubColdAudit (02),
//     AuditItem (03) and AppliedProposal (04) are written by applyProposal
//     after Merge returns, preserving the global Cold ordering.
//     ZoneIdempotency (0x05).
//     ZoneGlobal (0x06): LedgerInfo (03) → SigningKey (04) → SigningConfig
//     (05) → Chapters (06) → NextChapterID (07) → MaintenanceMode (0B) →
//     ChapterSchedule (0D) → QueryCheckpoint (0E) → NextQueryCheckpointID
//     (0F) → QueryCheckpointSchedule (10) → NextLedgerID (13).
//  4. Range purges and in-memory FSM state finalisation — executePurge and
//     deletedLedgers use DeleteRange (range tombstones live in a
//     separate skiplist and do not affect point-write monotonicity).
//
// Any new write site must respect this ordering. If a new write would land
// between zones already drained, route it through the appropriate sub-step
// — never append at the end as a fresh block.
func (b *WriteSet) Merge(batch *dal.WriteSession, logsOrRefs []*raftcmdpb.CreatedLogOrReference) error {
	// gen0 byte for incremental 0xFF cache writes.
	genByte := byte(b.fsm.Registry.Cache.CurrentGeneration() % 2)

	// === Phase 1: overlay drain (no Pebble writes) ============================
	//
	// derived.Merge() pulls each DerivedKeyStore's dirty values into a
	// (updates, deletions) pair and resets the overlay. Order is dictated by
	// downstream consumers: counter aggregation reads Volume / Metadata /
	// Reference deltas, so those overlays must drain before Boundaries.Merge.

	ledgerUpdates, ledgerDeletions, err := b.Derived.Ledgers.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge ledgers: %w", err)
	}

	volumeUpdates, _, err := b.Derived.Volumes.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge volumes: %w", err)
	}

	// Partition volumes by persistence mode: normal (kept), ephemeral (purged), transient (skipped).
	partResult := b.partitionVolumes(volumeUpdates)

	metadataUpdates, metadataDeletions, err := b.Derived.AccountMetadata.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge account metadata: %w", err)
	}

	referenceUpdates, referenceDeletions, err := b.Derived.References.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge references: %w", err)
	}

	// Update per-ledger attribute counters in boundaries before merging them.
	b.updateBoundaryCounters(volumeUpdates, partResult.purged, partResult.transient, metadataUpdates, metadataDeletions, referenceUpdates)

	boundaryUpdates, boundaryDeletions, err := b.Derived.Boundaries.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge boundaries: %w", err)
	}

	transactionUpdates, transactionDeletions, err := b.Derived.Transactions.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge transactions: %w", err)
	}

	ledgerMetadataUpdates, ledgerMetadataDeletions, err := b.Derived.LedgerMetadata.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge ledger metadata: %w", err)
	}

	sinkConfigUpdates, sinkConfigDeletions, err := b.Derived.SinkConfigs.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge sink configs: %w", err)
	}

	numscriptVersionUpdates, numscriptVersionDeletions, err := b.Derived.NumscriptVersions.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge numscript versions: %w", err)
	}

	numscriptContentUpdates, numscriptContentDeletions, err := b.Derived.NumscriptContents.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge numscript contents: %w", err)
	}

	preparedQueryUpdates, preparedQueryDeletions, err := b.Derived.PreparedQueries.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge prepared queries: %w", err)
	}

	indexUpdates, indexDeletions, err := b.Derived.Indexes.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge indexes: %w", err)
	}

	// === Phase 2: cross-zone in-memory side effects (no Pebble writes) ========

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

	// Flush pending reversions to the authoritative in-memory bitset and
	// collect the per-word dirty list. The Pebble writes for these dirty
	// words happen in phase 3 (ZonePerLedger drain).
	type dirtyWord struct {
		ledgerName string
		wordIndex  uint64
	}

	var dirtyWords []dirtyWord

	for _, txKey := range b.Derived.PendingReversions {
		wi := b.fsm.Registry.SetReverted(txKey)
		dirtyWords = append(dirtyWords, dirtyWord{ledgerName: txKey.LedgerName, wordIndex: wi})
	}

	// Build createdLogs (skipping idempotency replays) and inject the
	// per-log purged_volumes subset before persisting. purgedByLog is
	// indexed by ORDER index — same as logsOrRefs — so the mapping uses
	// the loop's i, not the createdLogs append index.
	purgedSet := makePurgedKeySet(partResult.purged)
	b.purgedByLog = buildPurgedByLog(b.volumes.Slots(), purgedSet)

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

	// Resolve the delete sequence for each ledger marked for cleanup. The
	// actual PerLedger writes happen in phase 4 (after the Global drain)
	// because they require createdLogs to be finalised first.
	var deleteSequences map[string]uint64
	if len(b.deletedLedgers) > 0 {
		deleteSequences = make(map[string]uint64, len(b.deletedLedgers))

		for _, log := range createdLogs {
			if dl := log.GetPayload().GetDeleteLedger(); dl != nil {
				deleteSequences[dl.GetName()] = log.GetSequence()
			}
		}
	}

	// === Phase 3: Pebble flush in monotone zone+sub order =====================
	//
	// Within each (zone, sub) bucket, paired attribute (0x01) + cache (0x02)
	// writes still micro-zigzag at byte 0 — this is intentional:
	// mergeSimpleWithCache shares the marshaled value bytes between the two
	// writes and the issue explicitly calls this out as a paired logical step.

	// ZoneAttributes (0x01) + ZoneCache (0x02), sub-prefix monotone.

	// SubAttrVolume (0x01): kept go through mergeSimpleWithCache + bloom;
	// purged go through applyEphemeralPurge (attribute Delete + cache zero);
	// transient go through zeroVolumeCache (cache zero, no Pebble attribute
	// write).
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
	// proposal admitted with CacheHit.
	if err := b.zeroVolumeCache(batch, genByte, partResult.transient); err != nil {
		return fmt.Errorf("failed zeroing transient volumes in cache: %w", err)
	}

	// SubAttrMetadata (0x02)
	if err := flushAttributeAndCache(b.attrs.Metadata, batch, genByte, dal.SubAttrMetadata, metadataUpdates, metadataDeletions, &b.bloomUpdates.Metadata, "account metadata"); err != nil {
		return err
	}

	// SubAttrTransaction (0x03)
	if err := flushAttributeAndCache(b.attrs.Transaction, batch, genByte, dal.SubAttrTransaction, transactionUpdates, transactionDeletions, &b.bloomUpdates.Transactions, "transactions"); err != nil {
		return err
	}

	// SubAttrLedger (0x04)
	if err := flushAttributeAndCache(b.attrs.Ledger, batch, genByte, dal.SubAttrLedger, ledgerUpdates, ledgerDeletions, &b.bloomUpdates.Ledgers, "ledgers"); err != nil {
		return err
	}

	// SubAttrBoundary (0x05)
	if err := flushAttributeAndCache(b.attrs.Boundary, batch, genByte, dal.SubAttrBoundary, boundaryUpdates, boundaryDeletions, &b.bloomUpdates.Boundaries, "boundaries"); err != nil {
		return err
	}

	// SubAttrReference (0x06)
	if err := flushAttributeAndCache(b.attrs.References, batch, genByte, dal.SubAttrReference, referenceUpdates, referenceDeletions, &b.bloomUpdates.References, "references"); err != nil {
		return err
	}

	// SubAttrLedgerMetadata (0x07)
	if err := flushAttributeAndCache(b.attrs.LedgerMetadata, batch, genByte, dal.SubAttrLedgerMetadata, ledgerMetadataUpdates, ledgerMetadataDeletions, &b.bloomUpdates.LedgerMetadata, "ledger metadata"); err != nil {
		return err
	}

	// SubAttrSinkConfig (0x08)
	if err := flushAttributeAndCache(b.attrs.SinkConfig, batch, genByte, dal.SubAttrSinkConfig, sinkConfigUpdates, sinkConfigDeletions, &b.bloomUpdates.SinkConfigs, "sink configs"); err != nil {
		return err
	}

	// SubAttrNumscriptVersion (0x09)
	if err := flushAttributeAndCache(b.attrs.NumscriptVersion, batch, genByte, dal.SubAttrNumscriptVersion, numscriptVersionUpdates, numscriptVersionDeletions, &b.bloomUpdates.NumscriptVersions, "numscript versions"); err != nil {
		return err
	}

	// SubAttrNumscriptContent (0x0A)
	if err := flushAttributeAndCache(b.attrs.NumscriptContent, batch, genByte, dal.SubAttrNumscriptContent, numscriptContentUpdates, numscriptContentDeletions, &b.bloomUpdates.NumscriptContents, "numscript contents"); err != nil {
		return err
	}

	// SubAttrPreparedQuery (0x0B)
	if err := flushAttributeAndCache(b.attrs.PreparedQuery, batch, genByte, dal.SubAttrPreparedQuery, preparedQueryUpdates, preparedQueryDeletions, &b.bloomUpdates.PreparedQueries, "prepared queries"); err != nil {
		return err
	}

	// SubAttrIndex (0x0C) — bucket-scoped index registry (per-ledger or bucket).
	if err := flushAttributeAndCache(b.attrs.Index, batch, genByte, dal.SubAttrIndex, indexUpdates, indexDeletions, &b.bloomUpdates.Indexes, "indexes"); err != nil {
		return err
	}

	// ZonePerLedger (0x03), sub-prefix monotone.

	// SubPLReversions (0x01)
	for _, dw := range dirtyWords {
		word := b.fsm.Registry.Reversions[dw.ledgerName].Word(dw.wordIndex)
		if err := saveReversionWord(batch, dw.ledgerName, dw.wordIndex, word); err != nil {
			return fmt.Errorf("saving reversion word for %q: %w", dw.ledgerName, err)
		}
	}

	// SubPLMirrorSourceHead (0x04), MirrorCursor (0x05), MirrorStatus (0x06)
	// — three sub-prefixes drained in order, one pass each. Keys within a
	// sub-prefix sort by ledger name (not sorted here — the monotonicity
	// contract is at the (zone, sub) granularity only).
	for _, w := range b.pendingMirrorSyncs {
		if w.SourceLogCount > 0 {
			if err := SetMirrorSourceHead(batch, w.LedgerName, w.SourceLogCount); err != nil {
				return fmt.Errorf("setting mirror source head for %q: %w", w.LedgerName, err)
			}
		}
	}

	for _, w := range b.pendingMirrorSyncs {
		if w.Cursor > 0 {
			if err := SetMirrorCursor(batch, w.LedgerName, w.Cursor); err != nil {
				return fmt.Errorf("setting mirror cursor for %q: %w", w.LedgerName, err)
			}
		}
	}

	for _, w := range b.pendingMirrorSyncs {
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

	// ZoneCold (0x04), SubColdLog (0x01) only. The higher Cold sub-prefixes
	// (SubColdAudit, SubColdAuditItem, SubColdAppliedProposal) are written by
	// applyProposal AFTER Merge returns, preserving the global Cold
	// sub-prefix monotonicity established by PR #542.
	if err := AppendLogs(batch, createdLogs); err != nil {
		return fmt.Errorf("failed appending pending logs: %w", err)
	}

	// ZoneIdempotency (0x05).
	if err := b.Derived.Idempotency.Merge(batch); err != nil {
		return fmt.Errorf("failed to merge idempotency keys: %w", err)
	}

	// ZoneGlobal (0x06), sub-prefix monotone.

	// SubGlobLedgerInfo (0x03)
	for _, update := range ledgerUpdates {
		if err := SaveLedger(batch, update.New); err != nil {
			return fmt.Errorf("failed to save ledger: %w", err)
		}
	}

	// SubGlobSigningKey (0x04) — Pebble write paired with in-memory keyStore
	// mutation; a batch.Commit failure leaves the keyStore mutated and
	// unsynced with Pebble, matching the prior behaviour.
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

	// SubGlobSigningConfig (0x05)
	if b.pendingSigningConfigUpdate != nil {
		err := SaveSigningConfig(batch, b.pendingSigningConfigUpdate.requireSignatures)
		if err != nil {
			return fmt.Errorf("saving signing config: %w", err)
		}

		b.fsm.sharedState.SetRequireSignatures(b.pendingSigningConfigUpdate.requireSignatures)
	}

	// SubGlobChapters (0x06)
	for _, p := range b.changedChapters {
		err := StoreChapter(batch, p)
		if err != nil {
			return fmt.Errorf("storing chapter %d: %w", p.GetId(), err)
		}
	}

	// SubGlobNextChapterID (0x07) — persist only if chapters were touched.
	if b.chapters != nil {
		if err := StoreNextChapterID(batch, b.chapters.NextChapterID()); err != nil {
			return fmt.Errorf("storing next chapter ID: %w", err)
		}
	}

	// SubGlobMaintenanceMode (0x0B)
	if b.pendingMaintenanceModeUpdate != nil {
		err := SaveMaintenanceMode(batch, b.pendingMaintenanceModeUpdate.enabled)
		if err != nil {
			return fmt.Errorf("saving maintenance mode: %w", err)
		}

		b.fsm.sharedState.SetMaintenanceMode(b.pendingMaintenanceModeUpdate.enabled)
	}

	// SubGlobChapterSchedule (0x0D)
	if b.chapterScheduleUpdate != nil {
		if *b.chapterScheduleUpdate == "" {
			err := batchDeleteChapterSchedule(batch)
			if err != nil {
				return fmt.Errorf("deleting chapter schedule: %w", err)
			}
		} else {
			err := SaveChapterSchedule(batch, *b.chapterScheduleUpdate)
			if err != nil {
				return fmt.Errorf("saving chapter schedule: %w", err)
			}
		}

		b.fsm.Chapters.SetSchedule(*b.chapterScheduleUpdate)
	}

	// SubGlobQueryCheckpoint (0x0E) — saves then deletes, both on the same
	// sub-prefix. The (checkpoint_id BE 8) tail keeps per-call ordering
	// deterministic; the contract is at zone+sub only.
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

	// SubGlobNextQueryCheckpointID (0x0F)
	if b.NextQueryCheckpointID != b.fsm.State.NextQueryCheckpointID {
		if err := storeNextQueryCheckpointID(batch, b.NextQueryCheckpointID); err != nil {
			return fmt.Errorf("storing next query checkpoint ID: %w", err)
		}
	}

	// SubGlobQueryCheckpointSchedule (0x10)
	if b.queryCheckpointScheduleUpdate != nil {
		if *b.queryCheckpointScheduleUpdate == "" {
			err := batchDeleteQueryCheckpointSchedule(batch)
			if err != nil {
				return fmt.Errorf("deleting query checkpoint schedule: %w", err)
			}
		} else {
			err := SaveQueryCheckpointSchedule(batch, *b.queryCheckpointScheduleUpdate)
			if err != nil {
				return fmt.Errorf("saving query checkpoint schedule: %w", err)
			}
		}

		b.fsm.setQueryCheckpointSchedule(*b.queryCheckpointScheduleUpdate)
	}

	// SubGlobNextLedgerID (0x13)
	if b.NextLedgerID != b.fsm.State.NextLedgerID {
		if err := saveNextLedgerID(batch, b.NextLedgerID); err != nil {
			return fmt.Errorf("storing next ledger ID: %w", err)
		}
	}

	// === Phase 4: range purges + FSM state finalisation =======================
	//
	// executePurge and the deletedLedgers block emit DeleteRange calls
	// (range tombstones) on ZoneCold and ZonePerLedger, plus a handful of
	// point writes/deletes (savePendingLedgerCleanup,
	// deletePendingLedgerCleanup, deleteLedgerData). Range tombstones live in
	// a dedicated skiplist, separate from the point-write skiplist, so they
	// do not break the monotonicity invariant established in phase 3. The
	// trailing point writes are bounded by the number of pending cleanups /
	// purge ranges (not the hot-path order count) so any residual back-step
	// they introduce is amortised across batches.

	// Purge archived chapter data (logs + audit entries) if requested.
	for i := range b.purgeRanges {
		err := b.executePurge(batch, &b.purgeRanges[i])
		if err != nil {
			return fmt.Errorf("purging archived chapter %d data: %w", b.purgeRanges[i].chapterID, err)
		}
	}

	// Register pending ledger data cleanups (deferred to purge time). Boundary
	// deletion is handled by MarkLedgerForCleanup adding a Delete to the
	// Derived.Boundaries overlay (flushed in phase 3 above).
	for _, ledgerName := range b.deletedLedgers {
		seq := deleteSequences[ledgerName]

		if _, err := b.getLedgerData(ledgerName); err != nil {
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

		// Clean in-memory reversion bitset and Pebble words — not needed after deletion.
		delete(b.fsm.Registry.Reversions, ledgerName)

		if err := deleteReversionsByLedger(batch, ledgerName); err != nil {
			return fmt.Errorf("deleting reversions for %q: %w", ledgerName, err)
		}
	}

	// In-memory FSM state finalisation.
	b.fsm.State.NextSequenceID = b.NextSequenceID
	b.fsm.State.NextLedgerID = b.NextLedgerID
	b.fsm.State.NextQueryCheckpointID = b.NextQueryCheckpointID

	// Apply changed chapters to Machine's Chapters tracker.
	for _, p := range b.changedChapters {
		b.fsm.Chapters.PutChapter(p)
	}

	// Remove purged chapters from memory.
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
	ws := &WriteSet{
		fsm:     fsm,
		attrs:   fsm.Registry.Attrs,
		Derived: NewDerivedRegistry(fsm.Registry),
	}

	ws.ledgers = newRawAccessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader](ws.Derived.Ledgers)
	ws.boundaries = newRawAccessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader](ws.Derived.Boundaries)
	ws.volumes = newRecorderAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader](
		newRawAccessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader](ws.Derived.Volumes),
	)
	ws.accountMetadata = newRawAccessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader](ws.Derived.AccountMetadata)
	ws.ledgerMetadata = newRawAccessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader](ws.Derived.LedgerMetadata)
	ws.transactionReferences = newRawAccessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader](ws.Derived.References)
	ws.transactionStates = newRawAccessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader](ws.Derived.Transactions)
	ws.preparedQueries = newRawAccessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader](ws.Derived.PreparedQueries)
	ws.indexes = newRawAccessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader](ws.Derived.Indexes)

	return ws
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
	b.chapterScheduleUpdate = nil
	b.queryCheckpointScheduleUpdate = nil
	b.sinkConfigChanged = false
	b.chapters = nil
	b.changedChapters = b.changedChapters[:0]
	b.purgeRanges = b.purgeRanges[:0]
	b.archiveRequests = b.archiveRequests[:0]
	b.pendingMirrorSyncs = b.pendingMirrorSyncs[:0]
	b.deletedLedgers = b.deletedLedgers[:0]
	b.allVolumeUpdates = b.allVolumeUpdates[:0]
	b.keptVolumeUpdates = b.keptVolumeUpdates[:0]
	b.transientVolumes = nil
	b.volumes.Reset()
	for i := range b.purgedByLog {
		b.purgedByLog[i] = nil
	}
	b.purgedByLog = b.purgedByLog[:0]
	b.bloomUpdates.Reset()
	b.pendingQueryCheckpointSaves = b.pendingQueryCheckpointSaves[:0]
	b.pendingQueryCheckpointDeletes = b.pendingQueryCheckpointDeletes[:0]
	b.chapterClosing = false
	b.mirrorConfigChanged = false
	b.queryCheckpointCreated = 0
	b.queryCheckpointDeleted = 0
}

// Absorb implements processing.SignalSink. It maps the log payload a
// processor just produced onto the WriteSet's cross-order accumulators
// (archive queue, purge ranges, sink-config tracking, lifecycle flags,
// schedule updates, ledger cleanup). Called by ProcessOrders right
// after each (order, log) pair is produced; the log IS the source of
// truth for what the framework should signal — there is no second
// vocabulary to keep in sync. Payload types not listed here have no
// cross-order signal (Apply, SaveLedgerMetadata, SealChapter, signing,
// maintenance, …) — their state changes flow through the Scope
// mutations the processor already did.
//
// Hot-path note: zero allocation per signal; the (order, log) pointers
// already exist and the dispatch is a single type switch.
func (b *WriteSet) Absorb(order *raftcmdpb.Order, log *commonpb.Log) {
	switch p := log.GetPayload().GetType().(type) {
	case *commonpb.LogPayload_ArchiveChapter:
		c := p.ArchiveChapter.GetChapter()
		b.archiveRequests = append(b.archiveRequests, ArchiveRequest{
			ChapterID:          c.GetId(),
			StartSequence:      c.GetStartSequence(),
			CloseSequence:      c.GetCloseSequence(),
			StartAuditSequence: c.GetStartAuditSequence(),
			CloseAuditSequence: c.GetCloseAuditSequence(),
		})
	case *commonpb.LogPayload_ConfirmArchiveChapter:
		c := p.ConfirmArchiveChapter.GetChapter()
		b.purgeRanges = append(b.purgeRanges, purgeRange{
			chapterID:          c.GetId(),
			startSequence:      c.GetStartSequence(),
			closeSequence:      c.GetCloseSequence(),
			startAuditSequence: c.GetStartAuditSequence(),
			closeAuditSequence: c.GetCloseAuditSequence(),
		})
	case *commonpb.LogPayload_AddedEventsSink:
		cfg := p.AddedEventsSink.GetConfig()
		b.Derived.SinkConfigs.Put(domain.SinkConfigKey{Name: cfg.GetName()}, cfg)
		b.sinkConfigChanged = true
	case *commonpb.LogPayload_RemovedEventsSink:
		b.Derived.SinkConfigs.Delete(domain.SinkConfigKey{Name: p.RemovedEventsSink.GetName()})
		b.sinkConfigChanged = true
	case *commonpb.LogPayload_SetChapterSchedule:
		cron := p.SetChapterSchedule.GetCron()
		b.chapterScheduleUpdate = &cron
	case *commonpb.LogPayload_DeleteChapterSchedule:
		empty := ""
		b.chapterScheduleUpdate = &empty
	case *commonpb.LogPayload_SetQueryCheckpointSchedule:
		cron := p.SetQueryCheckpointSchedule.GetCron()
		b.queryCheckpointScheduleUpdate = &cron
	case *commonpb.LogPayload_DeleteQueryCheckpointSchedule:
		empty := ""
		b.queryCheckpointScheduleUpdate = &empty
	case *commonpb.LogPayload_DeleteLedger:
		ledger := p.DeleteLedger.GetName()
		b.deletedLedgers = append(b.deletedLedgers, ledger)
		// Remove boundary from the in-memory overlay so that subsequent
		// GetBoundaries calls return domain.ErrNotFound — both within
		// this proposal and in future ones after Merge propagates the
		// deletion.
		b.Derived.Boundaries.Delete(domain.LedgerKey{Name: ledger})
	case *commonpb.LogPayload_CloseChapter:
		// Admission + FSM (ClassifyCheckpointOrderPosition) guarantee
		// CloseChapter is the last order of its proposal, so at most one
		// flip per proposal. applyProposal reads ChapterClosing
		// after writeAuditEntry to carry preAuditHash onto
		// LatestClosingChapter — same effect as the legacy log walk.
		b.chapterClosing = true
	case *commonpb.LogPayload_CreateLedger:
		// Only mirror creations reshape the mirror worker set. Reading
		// order.Mode matches the legacy hasMirrorConfigChange walk and
		// is robust to future log-shape changes.
		if ls := order.GetLedgerScoped(); ls != nil {
			if cl, ok := ls.GetPayload().(*raftcmdpb.LedgerScopedOrder_CreateLedger); ok &&
				cl.CreateLedger.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
				b.mirrorConfigChanged = true
			}
		}
	case *commonpb.LogPayload_PromoteLedger:
		// Promoting a mirror to normal reshapes the mirror worker set
		// the same way a fresh mirror creation does.
		b.mirrorConfigChanged = true
	case *commonpb.LogPayload_CreatedQueryCheckpoint:
		// CreateQueryCheckpoint is also a checkpoint-trigger order, so
		// at most one per proposal — the assignment matches the legacy
		// walk that iterated pendingQueryCheckpointSaves and took the
		// last seen id.
		b.queryCheckpointCreated = p.CreatedQueryCheckpoint.GetCheckpointId()
	case *commonpb.LogPayload_DeletedQueryCheckpoint:
		b.queryCheckpointDeleted = p.DeletedQueryCheckpoint.GetCheckpointId()
	}
}

// ChapterClosing reports whether the CloseChapter intent was
// absorbed during this proposal.
func (b *WriteSet) ChapterClosing() bool {
	return b.chapterClosing
}

// MirrorConfigChanged reports whether a mirror-config change (mirror
// CreateLedger or PromoteLedger) was absorbed during this proposal.
func (b *WriteSet) MirrorConfigChanged() bool {
	return b.mirrorConfigChanged
}

// QueryCheckpointCreated returns the id of the query checkpoint
// created during this proposal (zero when none).
func (b *WriteSet) QueryCheckpointCreated() uint64 {
	return b.queryCheckpointCreated
}

// QueryCheckpointDeleted returns the id of the query checkpoint
// deleted during this proposal (zero when none).
func (b *WriteSet) QueryCheckpointDeleted() uint64 {
	return b.queryCheckpointDeleted
}

// Engine surface: the per-kind Accessor methods, plus the hetero counter,
// chapter, signing-key and bool-state methods. The Accessor methods are
// overridden on gatedScope to layer the coverage gate; the hetero methods
// pass through *WriteSet directly via embedding. The coverage gate
// method (CheckCoverage) is deliberately absent here — it lives on
// gatedScope so a future engine implementation cannot accidentally route
// around the gate.

// Ledgers returns the bare ledger accessor — no coverage gate.
func (b *WriteSet) Ledgers() processing.Accessor[domain.LedgerKey, *commonpb.LedgerInfo, commonpb.LedgerInfoReader] {
	return b.ledgers
}

// Boundaries returns the bare boundaries accessor — no coverage gate.
func (b *WriteSet) Boundaries() processing.Accessor[domain.LedgerKey, *raftcmdpb.LedgerBoundaries, raftcmdpb.LedgerBoundariesReader] {
	return b.boundaries
}

// Volumes returns the volume accessor whose Put records the per-order
// touch slot consumed by Merge.
func (b *WriteSet) Volumes() processing.Accessor[domain.VolumeKey, *raftcmdpb.VolumePair, raftcmdpb.VolumePairReader] {
	return b.volumes
}

// AccountMetadata returns the bare account-metadata accessor.
func (b *WriteSet) AccountMetadata() processing.Accessor[domain.MetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return b.accountMetadata
}

// LedgerMetadata returns the bare ledger-metadata accessor.
func (b *WriteSet) LedgerMetadata() processing.Accessor[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader] {
	return b.ledgerMetadata
}

// TransactionReferences returns the bare transaction-reference accessor.
func (b *WriteSet) TransactionReferences() processing.Accessor[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue, commonpb.TransactionReferenceValueReader] {
	return b.transactionReferences
}

// TransactionStates returns the bare transaction-state accessor.
func (b *WriteSet) TransactionStates() processing.Accessor[domain.TransactionKey, *commonpb.TransactionState, commonpb.TransactionStateReader] {
	return b.transactionStates
}

// PreparedQueries returns the bare prepared-query accessor.
func (b *WriteSet) PreparedQueries() processing.Accessor[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader] {
	return b.preparedQueries
}

// Indexes returns the bare index-registry accessor.
func (b *WriteSet) Indexes() processing.Accessor[domain.IndexKey, *commonpb.Index, commonpb.IndexReader] {
	return b.indexes
}

// getLedgerData is the internal accessor that returns the underlying
// *LedgerInfo pointer. It exists so paths inside the state package
// (Merge, ephemeral purge) can avoid the AsReader/Mutate clone round-trip
// the Scope-facing accessor would otherwise impose. External callers MUST
// go through Ledgers().Get — only state-package code is trusted not to
// mutate the cache pointer in place.
func (b *WriteSet) getLedgerData(name string) (*commonpb.LedgerInfo, error) {
	info, err := b.Derived.Ledgers.Get(domain.LedgerKey{Name: name})
	if err != nil {
		return nil, err
	}

	if info == nil {
		return nil, domain.ErrNotFound
	}

	return info, nil
}

// ResolveNumscriptContent stays as a discrete method on Scope — its
// (ledgerName, name, version) signature does not fit the Accessor trio.
func (b *WriteSet) ResolveNumscriptContent(ledgerName string, name, version string) (commonpb.NumscriptInfoReader, error) {
	info, err := b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{LedgerName: ledgerName, Name: name, Version: version})
	// Treat a cache miss as "doesn't exist" — same pattern as GetPreparedQuery
	// and GetNumscriptLatestVersion. Admission emits a Declare plan for absent
	// NumscriptContent keys (EN-1378), so the Derived store returns ErrNotFound
	// for a never-recorded script; the caller (processCreateTransaction) checks
	// `info == nil` to surface ErrNumscriptNotFound, not ErrStorageOperation.
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}

	if err != nil || info == nil {
		return nil, err
	}

	return info.AsReader(), nil
}

// BeginOrder tags subsequent PutVolume calls with the given zero-based order
// index. Called by ProcessOrders before each handler invocation so the
// WriteSet can attribute volume touches to the order that produced them. See
// purgedByLog for how the recorded touches are consumed at Merge time.
func (b *WriteSet) BeginOrder(orderIndex int) {
	b.volumes.BeginSlot(orderIndex)
}

// storageFault pairs a "should-not-happen" storage/coverage failure hit during
// transient validation with the dirty-volume key that produced it, so
// ValidateTransientVolumes can pick a deterministic one (smallest key) to
// surface even though DirtyValues() ranges in Go's randomized map order.
type storageFault struct {
	key domain.VolumeKey
	err domain.Describable
}

// compareVolumeKeys orders volume keys by (Account, Asset, Color, LedgerName).
// Account and Asset are what the returned error carries; Color segregates the
// per-bucket balances that share an (account, asset) under color-of-money;
// LedgerName is the final tiebreaker giving a total order (map keys are unique
// on all four, so ties never occur) so the winner is fully defined when two
// ledgers share an (account, asset, color).
func compareVolumeKeys(a, b domain.VolumeKey) int {
	return cmp.Or(
		cmp.Compare(a.Account, b.Account),
		cmp.Compare(a.Asset, b.Asset),
		cmp.Compare(a.Color, b.Color),
		cmp.Compare(a.LedgerName, b.LedgerName),
	)
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

	var (
		storageFaults []storageFault
		offenders     []domain.VolumeKey
	)
	for key, vol := range b.Derived.Volumes.DirtyValues() {
		compiled, ok := ledgerTypes[key.LedgerName]
		if !ok {
			info, err := scope.Ledgers().Get(domain.LedgerKey{Name: key.LedgerName})
			if errors.Is(err, domain.ErrNotFound) {
				continue
			}

			if err != nil {
				storageFaults = append(storageFaults, storageFault{key, &domain.ErrStorageOperation{Operation: "loading ledger for transient volume validation", Cause: err}})

				continue
			}

			compiled = accounttype.CompileTypes(info.Mutate().GetAccountTypes())
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
		if err := scope.CheckCoverage(dal.SubAttrVolume, key); err != nil {
			storageFaults = append(storageFaults, storageFault{key, &domain.ErrStorageOperation{Operation: "coverage check on transient base volume", Cause: err}})

			continue
		}

		baseVol, _, baseErr := b.Derived.Volumes.Parent().GetKey(key)
		if baseErr == nil && !isVolumeZeroBalance(baseVol) {
			continue
		}

		if !isVolumeZeroBalance(vol) {
			offenders = append(offenders, key)
		}
	}

	// A storage/coverage fault means the check could not run correctly for at
	// least one key, so surface it ahead of any business offender. Pick the
	// (Account, Asset, LedgerName)-smallest so the choice is deterministic.
	if len(storageFaults) > 0 {
		return slices.MinFunc(storageFaults, func(a, b storageFault) int {
			return compareVolumeKeys(a.key, b.key)
		}).err
	}

	if len(offenders) == 0 {
		return nil
	}

	// One error listing every offending account, sorted by (Account, Asset,
	// Color, LedgerName) and deduplicated to (Account, Asset, Color)
	// granularity — the identity the error exposes. Color is part of the
	// identity: two color buckets of the same (account, asset) are distinct
	// transient cells and must not fuse into one offender (matches the
	// checker's exclusion set, which keys on Color — checker.go). Sorting
	// only the offenders (usually zero) keeps the byte-determinism guarantee
	// off the happy path.
	slices.SortFunc(offenders, compareVolumeKeys)
	accounts := make([]domain.AccountAssetKey, 0, len(offenders))
	for _, key := range offenders {
		account := domain.AccountAssetKey{Account: key.Account, Asset: key.Asset, Color: key.Color}
		if n := len(accounts); n > 0 && accounts[n-1] == account {
			continue
		}

		accounts = append(accounts, account)
	}

	return &domain.ErrTransientAccountNonZero{Accounts: accounts}
}

// GetReverted is the bool-valued reversion probe — stays discrete (no Reader).
func (b *WriteSet) GetReverted(key domain.TransactionKey) (bool, error) {
	return b.Derived.GetReverted(key), nil
}

// PutReverted records a reversion in the pending bitset. Idempotent under
// reverted=false (the FSM never un-reverts).
func (b *WriteSet) PutReverted(key domain.TransactionKey, reverted bool) {
	if reverted {
		b.Derived.PutReverted(key)
	}
}

// GetIdempotencyKey reads through the idempotency overlay with TTL
// filtering. Not part of the Scope contract — production code goes
// through fsm.Registry.Idempotency directly — but exposed on the
// WriteSet so unit tests can drive the overlay end-to-end.
func (b *WriteSet) GetIdempotencyKey(key domain.IdempotencyKey) (commonpb.IdempotencyKeyValueReader, error) {
	value, err := b.Derived.Idempotency.Get(key.Key)
	if err != nil || value == nil {
		return nil, err
	}

	if b.fsm.Registry.Idempotency.IsExpired(value, b.Date.GetData()) {
		return nil, nil
	}

	return value.AsReader(), nil
}

// PutIdempotencyKey stamps the proposal HLC and writes through the
// idempotency overlay. Not part of the Scope contract; kept on the
// WriteSet for parity with GetIdempotencyKey and the unit tests.
func (b *WriteSet) PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	value.CreatedAt = b.Date.GetData() // HLC timestamp
	b.Derived.Idempotency.Put(key.Key, value)
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

func (b *WriteSet) GetSinkConfig(name string) (commonpb.SinkConfigReader, error) {
	cfg, err := b.Derived.SinkConfigs.Get(domain.SinkConfigKey{Name: name})
	if err != nil || cfg == nil {
		return nil, nil
	}

	return cfg.AsReader(), nil
}

func (b *WriteSet) SinkConfigChanged() bool {
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

func (b *WriteSet) GetDate() commonpb.TimestampReader {
	if b.Date == nil {
		return nil
	}

	return b.Date.AsReader()
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

func (b *WriteSet) GetCurrentOpenChapter() (commonpb.ChapterReader, bool) {
	b.ensureChapters()

	p := b.chapters.CurrentOpenChapter()
	if p != nil {
		return p.AsReader(), true
	}

	return nil, false
}

func (b *WriteSet) GetClosingChapters() []commonpb.ChapterReader {
	b.ensureChapters()

	closing := b.chapters.ClosingChapters()
	if closing == nil {
		return nil
	}

	out := make([]commonpb.ChapterReader, len(closing))
	for i, c := range closing {
		out[i] = c.AsReader()
	}

	return out
}

func (b *WriteSet) GetClosingChapterByID(chapterID uint64) (commonpb.ChapterReader, bool) {
	b.ensureChapters()

	c, ok := b.chapters.ClosingChapterByID(chapterID)
	if !ok {
		return nil, false
	}

	return c.AsReader(), true
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
func (b *WriteSet) GetChapterByID(chapterID uint64) (commonpb.ChapterReader, bool) {
	// Check changedChapters (most recently changed first)
	for _, v := range slices.Backward(b.changedChapters) {
		if v.GetId() == chapterID {
			return v.AsReader(), true
		}
	}

	b.ensureChapters()

	c, ok := b.chapters.GetChapterByID(chapterID)
	if !ok {
		return nil, false
	}

	return c.AsReader(), true
}

// UpdateChapter records a chapter modification to be persisted in Merge()
// and rebinds the buffer's in-memory tracker to the caller's pointer. Handlers
// that mutate a chapter via Reader.Mutate() pass the resulting clone here so
// subsequent reads in the same proposal (and the Merge that follows) observe
// the mutation instead of the original cached pointer.
func (b *WriteSet) UpdateChapter(chapter *commonpb.Chapter) {
	b.ensureChapters()
	b.chapters.UpdateChapter(chapter)
	b.changedChapters = append(b.changedChapters, chapter)
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

// Numscript library operations

func (b *WriteSet) GetNumscriptLatestVersion(ledgerName string, name string) (string, error) {
	val, err := b.Derived.NumscriptVersions.Get(domain.NumscriptVersionKey{LedgerName: ledgerName, Name: name})
	// Treat a cache miss as "doesn't exist" — same pattern as GetPreparedQuery.
	// Admission emits a Declare plan for absent NumscriptVersion keys (no
	// typed-nil cache injection); the FSM-side Derived store consequently
	// returns ErrNotFound, which is the legitimate "no version recorded yet"
	// signal — not a storage failure callers should surface as ErrStorageOperation.
	if errors.Is(err, domain.ErrNotFound) {
		return "", nil
	}

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

// TransientVolumes returns the unique transient (account, asset, color)
// volumes per ledger, collected during Merge from the transient volume
// partition.
func (b *WriteSet) TransientVolumes() map[string][]*commonpb.TouchedVolume {
	return b.transientVolumes
}

// collectUniqueVolumes extracts unique (account, asset, color) tuples per
// ledger from volume updates and emits them as deterministically-ordered
// commonpb.TouchedVolume slices. Color is part of the identity so two color
// buckets of the same (account, asset) stay distinct in the audit log.
func collectUniqueVolumes(updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[string][]*commonpb.TouchedVolume {
	type accAssetColor struct{ Account, Asset, Color string }
	seen := make(map[string]map[accAssetColor]struct{})

	for _, update := range updates {
		ledgerName := update.Key.LedgerName
		k := accAssetColor{Account: update.Key.Account, Asset: update.Key.Asset, Color: update.Key.Color}

		if seen[ledgerName] == nil {
			seen[ledgerName] = make(map[accAssetColor]struct{})
		}

		seen[ledgerName][k] = struct{}{}
	}

	result := make(map[string][]*commonpb.TouchedVolume, len(seen))
	for ledgerName, vols := range seen {
		list := make([]accAssetColor, 0, len(vols))
		for k := range vols {
			list = append(list, k)
		}

		sort.Slice(list, func(a, b int) bool {
			if list[a].Account != list[b].Account {
				return list[a].Account < list[b].Account
			}
			if list[a].Asset != list[b].Asset {
				return list[a].Asset < list[b].Asset
			}

			return list[a].Color < list[b].Color
		})

		out := make([]*commonpb.TouchedVolume, len(list))
		for i, k := range list {
			out[i] = &commonpb.TouchedVolume{Account: k.Account, Asset: k.Asset, Color: k.Color}
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
