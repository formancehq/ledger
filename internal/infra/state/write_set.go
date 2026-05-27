package state

import (
	"errors"
	"fmt"

	"github.com/holiman/uint256"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/bloom"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// mergeAndTrackBloom merges a DerivedKeyStore into its parent, writes the updates
// to the Pebble batch via the Attribute AND to the 0xFF cache zone (lean format),
// tracks canonical keys for bloom filter updates, and processes any attribute deletions.
func mergeAndTrackBloom[K attributes.Key, V proto.Message](
	derived *attributes.DerivedKeyStore[K, V],
	attr *attributes.Attribute[V],
	batch *dal.Batch,
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
	pendingPeriodScheduleUpdate          *string
	pendingQueryCheckpointScheduleUpdate *string
	sinkConfigChanged                    bool
	// periods is a lazy clone of fsm.Periods, created on first period access.
	// Nil means no period method was called — Merge() skips period propagation.
	// Period orders (ClosePeriod, SealPeriod, etc.) read period protos and mutate
	// them in-place, so the clone must happen before any read to avoid corrupting
	// the FSM's state. CreateTransaction never touches periods, so the clone is
	// avoided on the hot path.
	periods                        *PeriodTracker
	changedPeriods                 []*commonpb.Period
	purgeRanges                    []purgeRange
	pendingArchives                []ArchiveRequest
	pendingMetadataConvertRequests []MetadataConvertRequest

	// pendingLedgerDeletions holds ledger names scheduled for data cleanup during Merge.
	pendingLedgerDeletions []string

	// allVolumeUpdates includes kept + purged updates (for delta/posting cross-check).
	allVolumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]

	// keptVolumeUpdates excludes ephemeral purged entries (for post-commit Pebble verification).
	keptVolumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]

	// purgedVolumeKeys holds keys of volumes removed by ephemeral purge.
	// Used to exclude these from cross-entry post-commit verification.
	purgedVolumeKeys []domain.VolumeKey

	// transientAccounts holds unique transient account names per ledger,
	// populated during Merge for inclusion in the audit entry.
	transientAccounts map[uint32][]string

	// purgedAccounts holds unique ephemeral account names per ledger whose
	// volumes were purged (zero balance), populated during Merge.
	purgedAccounts map[uint32][]string

	// bloomUpdates collects canonical keys per attribute type during Merge
	// for bloom filter updates before batch.Commit().
	bloomUpdates bloom.BloomUpdates

	// Pending query checkpoint changes for Merge.
	pendingQueryCheckpointSaves   []*raftcmdpb.QueryCheckpointState
	pendingQueryCheckpointDeletes []uint64
}

// purgeRange identifies a period's sequence ranges to delete from Pebble during Merge().
// Log and audit entries have independent sequence counters, so separate ranges are needed.
type purgeRange struct {
	periodID           uint64
	startSequence      uint64 // log sequence range start
	closeSequence      uint64 // log sequence range end
	startAuditSequence uint64 // audit sequence range start
	closeAuditSequence uint64 // audit sequence range end
}

func (b *WriteSet) Merge(batch *dal.Batch, logs []*commonpb.Log) error {
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

	// Transient volumes are NOT written to 0xF1 (attributes) but ARE written to
	// 0xFF (cache). They must survive cache restores after node restart: without
	// the 0xFF entry, a restarted node's cache won't have the transient volume,
	// causing CacheGuaranteed proposals to fail with ErrBalanceNotPreloaded.
	if err := writeCacheOnly(batch, genByte, dal.SubAttrVolume, partResult.transient); err != nil {
		return fmt.Errorf("failed writing transient volumes to cache: %w", err)
	}

	// Trace volume partitions for sentinel diagnostics.
	if b.fsm.sentinelMode {
		b.fsm.sentinelTracer.TraceVolumeUpdates(partResult.kept, partResult.transient, partResult.purged)
	}

	// Collect unique transient/purged account names per ledger for the audit entry.
	if len(partResult.transient) > 0 {
		b.transientAccounts = collectUniqueAccounts(partResult.transient)
	}

	if len(partResult.purged) > 0 {
		b.purgedAccounts = collectUniqueAccounts(partResult.purged)
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
					"ledger":    u.Key.LedgerID,
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
					"ledger":  u.Key.LedgerID,
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

	// Record purged volume keys for cross-entry deduplication in ApplyEntries.
	for _, purged := range partResult.purged {
		b.purgedVolumeKeys = append(b.purgedVolumeKeys, purged.Key)
	}

	// Process AccountMetadata updates
	metadataUpdates, metadataDeletions, err := mergeAndTrackBloom(b.Derived.AccountMetadata, b.attrs.Metadata, batch, genByte, dal.SubAttrMetadata, &b.bloomUpdates.Metadata, "account metadata")
	if err != nil {
		return err
	}

	// Flush pending reversions to the authoritative in-memory bitset and persist only the modified words.
	type dirtyWord struct {
		ledgerID  uint32
		wordIndex uint64
	}

	var dirtyWords []dirtyWord

	for _, txKey := range b.Derived.PendingReversions {
		wi := b.fsm.Registry.SetReverted(txKey)
		dirtyWords = append(dirtyWords, dirtyWord{ledgerID: txKey.LedgerID, wordIndex: wi})
	}

	for _, dw := range dirtyWords {
		word := b.fsm.Registry.Reversions[dw.ledgerID].Word(dw.wordIndex)
		if err := saveReversionWord(batch, dw.ledgerID, dw.wordIndex, word); err != nil {
			return fmt.Errorf("saving reversion word for %d: %w", dw.ledgerID, err)
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

	err = AppendLogs(batch, logs)
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

	if b.pendingPeriodScheduleUpdate != nil {
		if *b.pendingPeriodScheduleUpdate == "" {
			err := batchDeletePeriodSchedule(batch)
			if err != nil {
				return fmt.Errorf("deleting period schedule: %w", err)
			}
		} else {
			err := SavePeriodSchedule(batch, *b.pendingPeriodScheduleUpdate)
			if err != nil {
				return fmt.Errorf("saving period schedule: %w", err)
			}
		}

		b.fsm.Periods.SetSchedule(*b.pendingPeriodScheduleUpdate)
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

	for _, p := range b.changedPeriods {
		err := StorePeriod(batch, p)
		if err != nil {
			return fmt.Errorf("storing period %d: %w", p.GetId(), err)
		}
	}

	// Persist next period ID only if periods were touched.
	if b.periods != nil {
		if err := StoreNextPeriodID(batch, b.periods.NextPeriodID()); err != nil {
			return fmt.Errorf("storing next period ID: %w", err)
		}
	}

	// Purge archived period data (logs + audit entries) if requested
	for i := range b.purgeRanges {
		err := b.executePurge(batch, &b.purgeRanges[i])
		if err != nil {
			return fmt.Errorf("purging archived period %d data: %w", b.purgeRanges[i].periodID, err)
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

	// Register pending ledger data cleanups (deferred to purge time).
	// Find the delete sequence for each pending deletion from the logs.
	if len(b.pendingLedgerDeletions) > 0 {
		deleteSequences := make(map[string]uint64, len(b.pendingLedgerDeletions))

		for _, log := range logs {
			if dl := log.GetPayload().GetDeleteLedger(); dl != nil {
				deleteSequences[dl.GetName()] = log.GetSequence()
			}
		}

		for _, ledgerName := range b.pendingLedgerDeletions {
			seq := deleteSequences[ledgerName]

			info, ok := b.GetLedger(ledgerName)
			if !ok {
				continue // ledger not found (should not happen, but be safe)
			}

			ledgerID := info.GetId()

			if err := savePendingLedgerCleanup(batch, ledgerID, seq); err != nil {
				return fmt.Errorf("saving pending ledger cleanup for %q: %w", ledgerName, err)
			}

			b.fsm.pendingLedgerCleanups[ledgerID] = seq

			// Boundary deletion is handled above via boundaryDeletions
			// (MarkLedgerForCleanup adds a Delete to the Derived.Boundaries overlay).

			// Clean in-memory reversion bitset and Pebble words — not needed after deletion.
			delete(b.fsm.Registry.Reversions, ledgerID)

			if err := deleteReversionsByLedger(batch, ledgerID); err != nil {
				return fmt.Errorf("deleting reversions for %q: %w", ledgerName, err)
			}
		}
	}

	// Persist next query checkpoint ID if it changed.
	if b.NextQueryCheckpointID != b.fsm.nextQueryCheckpointID {
		if err := storeNextQueryCheckpointID(batch, b.NextQueryCheckpointID); err != nil {
			return fmt.Errorf("storing next query checkpoint ID: %w", err)
		}
	}

	// Persist next ledger ID if it changed.
	if b.NextLedgerID != b.fsm.nextLedgerID {
		if err := saveNextLedgerID(batch, b.NextLedgerID); err != nil {
			return fmt.Errorf("storing next ledger ID: %w", err)
		}
	}

	b.fsm.nextSequenceID = b.NextSequenceID
	b.fsm.nextLedgerID = b.NextLedgerID
	b.fsm.nextQueryCheckpointID = b.NextQueryCheckpointID

	// Apply changed periods to Machine's Periods tracker
	for _, p := range b.changedPeriods {
		b.fsm.Periods.PutPeriod(p)
	}

	// Remove purged periods from memory
	for _, pr := range b.purgeRanges {
		b.fsm.Periods.DeletePeriod(pr.periodID)
	}

	// Propagate period tracker state only if periods were touched (lazy clone occurred).
	// On the hot transaction path (CreateTransaction, etc.), b.periods stays nil
	// and the FSM's tracker is already correct.
	if b.periods != nil {
		b.fsm.Periods.SetCurrentOpenPeriod(b.periods.CurrentOpenPeriod())
		b.fsm.Periods.SetClosingPeriods(b.periods.ClosingPeriods())
		b.fsm.Periods.SetNextPeriodID(b.periods.NextPeriodID())
	}

	return nil
}

func NewWriteSet(fsm *Machine) *WriteSet {
	return &WriteSet{
		fsm:     fsm,
		attrs:   fsm.Registry.Attrs,
		Derived: NewDerivedRegistry(fsm.Registry),
	}
}

// Reset prepares the WriteSet for a new proposal, clearing all per-proposal
// state while preserving allocated maps and slice backing arrays.
func (b *WriteSet) Reset(at *commonpb.Timestamp) {
	b.Date = at
	b.NextSequenceID = b.fsm.nextSequenceID
	b.NextAuditSequenceID = b.fsm.nextAuditSequenceID
	b.NextLedgerID = b.fsm.nextLedgerID
	b.NextQueryCheckpointID = b.fsm.nextQueryCheckpointID
	b.Derived.Reset()

	b.pendingSigningKeyUpdates = b.pendingSigningKeyUpdates[:0]
	b.pendingSigningConfigUpdate = nil
	b.pendingMaintenanceModeUpdate = nil
	b.pendingPeriodScheduleUpdate = nil
	b.pendingQueryCheckpointScheduleUpdate = nil
	b.sinkConfigChanged = false
	b.periods = nil
	b.changedPeriods = b.changedPeriods[:0]
	b.purgeRanges = b.purgeRanges[:0]
	b.pendingArchives = b.pendingArchives[:0]
	b.pendingMetadataConvertRequests = b.pendingMetadataConvertRequests[:0]
	b.pendingLedgerDeletions = b.pendingLedgerDeletions[:0]
	b.allVolumeUpdates = b.allVolumeUpdates[:0]
	b.keptVolumeUpdates = b.keptVolumeUpdates[:0]
	b.purgedVolumeKeys = b.purgedVolumeKeys[:0]
	b.transientAccounts = nil
	b.purgedAccounts = nil
	b.bloomUpdates.Reset()
	b.pendingQueryCheckpointSaves = b.pendingQueryCheckpointSaves[:0]
	b.pendingQueryCheckpointDeletes = b.pendingQueryCheckpointDeletes[:0]
}

// Store interface implementation for WriteSet

func (b *WriteSet) GetLedger(name string) (*commonpb.LedgerInfo, bool) {
	info, err := b.Derived.Ledgers.Get(domain.LedgerKey{Name: name})
	if err != nil || info == nil {
		return nil, false
	}

	return info, true
}

// GetLedgerByID finds a LedgerInfo by its numeric ID.
// Checks dirty values first (current batch), then the parent KeyStore (committed state).
func (b *WriteSet) GetLedgerByID(id uint32) (*commonpb.LedgerInfo, bool) {
	// Check dirty values (current batch modifications).
	for _, info := range b.Derived.Ledgers.DirtyValues() {
		if info.GetId() == id {
			return info, true
		}
	}

	// Check committed state via parent KeyStore.
	for _, entry := range b.Derived.Ledgers.Parent().M.Iter() {
		if entry.Data != nil && entry.Data.GetId() == id {
			return entry.Data, true
		}
	}

	return nil, false
}

func (b *WriteSet) PutLedger(name string, info *commonpb.LedgerInfo) {
	b.Derived.Ledgers.Put(domain.LedgerKey{Name: name}, info)
}

func (b *WriteSet) MarkLedgerForCleanup(ledger string) {
	b.pendingLedgerDeletions = append(b.pendingLedgerDeletions, ledger)
	// Remove boundary from the in-memory overlay so that subsequent
	// GetBoundaries calls return (nil, false) — both within this proposal
	// and in future proposals after Merge propagates the deletion.
	b.Derived.Boundaries.Delete(domain.LedgerKey{Name: ledger})
}

func (b *WriteSet) GetBoundaries(ledger string) (raftcmdpb.LedgerBoundariesReader, bool) {
	boundaries, err := b.Derived.Boundaries.Get(domain.LedgerKey{Name: ledger})
	if err != nil || boundaries == nil {
		return nil, false
	}

	return boundaries.AsReader(), true
}

func (b *WriteSet) ResolveNumscriptContent(ledgerID uint32, name, version string) (*commonpb.NumscriptInfo, error) {
	return b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{LedgerID: ledgerID, Name: name, Version: version})
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
}

// ValidateTransientVolumes checks that all transient account volumes have zero balance.
// Must be called after ProcessOrders and before Commit, so that failures are
// treated as business errors (rejected proposals) rather than fatal FSM errors.
//
// Transient validation only applies when the base volume (before this batch) is zero
// or absent. Pre-existing non-zero volumes from before the account was marked transient
// are treated as normal and skip the zero-balance check.
func (b *WriteSet) ValidateTransientVolumes() error {
	ledgerTypes := make(map[uint32][]accounttype.CompiledType)

	for key, vol := range b.Derived.Volumes.DirtyValues() {
		compiled, ok := ledgerTypes[key.LedgerID]
		if !ok {
			info, infoOK := b.GetLedgerByID(key.LedgerID)
			if !infoOK {
				continue
			}

			compiled = accounttype.CompileTypes(info.GetAccountTypes())
			ledgerTypes[key.LedgerID] = compiled
		}

		matched := accounttype.FindMatchingType(key.Account, compiled)
		if matched == nil || matched.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			continue
		}

		// Check if the parent KeyStore has a pre-existing non-zero volume.
		// If so, the account had volumes before being marked transient — skip validation.
		baseVol, _, baseErr := b.fsm.Registry.Volumes.GetKey(key)
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

func (b *WriteSet) PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue) {
	b.Derived.AccountMetadata.Put(key, value)
}

func (b *WriteSet) DeleteAccountMetadata(key domain.MetadataKey) {
	b.Derived.AccountMetadata.Delete(key)
}

func (b *WriteSet) GetLedgerMetadata(key domain.LedgerMetadataKey) (*commonpb.MetadataValue, error) {
	return b.Derived.LedgerMetadata.Get(key)
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

func (b *WriteSet) SetPeriodSchedule(cronExpr string) {
	b.pendingPeriodScheduleUpdate = &cronExpr
}

func (b *WriteSet) DeletePeriodSchedule() {
	empty := ""
	b.pendingPeriodScheduleUpdate = &empty
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

// Period operations

// ensurePeriods clones the FSM's PeriodTracker on first access.
// Period orders (ClosePeriod, SealPeriod, etc.) read period protos and mutate
// them in-place, so the clone must happen before any read to protect the FSM.
// CreateTransaction never calls period methods, so this is never triggered on
// the hot transaction path.
func (b *WriteSet) ensurePeriods() {
	if b.periods == nil {
		b.periods = b.fsm.Periods.Clone()
	}
}

func (b *WriteSet) GetCurrentOpenPeriod() (*commonpb.Period, bool) {
	b.ensurePeriods()

	p := b.periods.CurrentOpenPeriod()
	if p != nil {
		return p, true
	}

	return nil, false
}

func (b *WriteSet) GetClosingPeriods() []*commonpb.Period {
	b.ensurePeriods()

	return b.periods.ClosingPeriods()
}

func (b *WriteSet) GetClosingPeriodByID(periodID uint64) (*commonpb.Period, bool) {
	b.ensurePeriods()

	return b.periods.ClosingPeriodByID(periodID)
}

func (b *WriteSet) SetCurrentOpenPeriod(period *commonpb.Period) {
	b.ensurePeriods()
	b.periods.SetCurrentOpenPeriod(period)
	b.changedPeriods = append(b.changedPeriods, period)
}

func (b *WriteSet) AddClosingPeriod(period *commonpb.Period) {
	b.ensurePeriods()
	b.periods.AddClosingPeriod(period)
	b.changedPeriods = append(b.changedPeriods, period)
}

// RemoveClosingPeriod persists the closing period's final state and removes it from in-memory tracking.
func (b *WriteSet) RemoveClosingPeriod(periodID uint64) {
	b.ensurePeriods()

	if closing, ok := b.periods.ClosingPeriodByID(periodID); ok {
		b.changedPeriods = append(b.changedPeriods, closing)
	}

	b.periods.RemoveClosingPeriod(periodID)
}

func (b *WriteSet) GetNextPeriodID() uint64 {
	b.ensurePeriods()

	return b.periods.NextPeriodID()
}

func (b *WriteSet) IncrementNextPeriodID() uint64 {
	b.ensurePeriods()

	id := b.periods.NextPeriodID()
	b.periods.SetNextPeriodID(id + 1)

	return id
}

// GetPeriodByID looks up a period by ID from in-memory state only.
// It checks changedPeriods first (most recent modifications), then the periods tracker.
func (b *WriteSet) GetPeriodByID(periodID uint64) (*commonpb.Period, bool) {
	// Check changedPeriods (most recently changed first)
	for i := len(b.changedPeriods) - 1; i >= 0; i-- {
		if b.changedPeriods[i].GetId() == periodID {
			return b.changedPeriods[i], true
		}
	}

	b.ensurePeriods()

	return b.periods.GetPeriodByID(periodID)
}

// UpdatePeriod records a period modification to be persisted in Merge().
func (b *WriteSet) UpdatePeriod(period *commonpb.Period) {
	b.changedPeriods = append(b.changedPeriods, period)
}

// SetPurgeRange records sequence ranges to be purged during Merge().
// Log and audit entries have independent sequence counters (audit advances
// slower due to batching), so both ranges are needed for correct purging.
func (b *WriteSet) SetPurgeRange(periodID, startSequence, closeSequence, startAuditSequence, closeAuditSequence uint64) {
	b.purgeRanges = append(b.purgeRanges, purgeRange{
		periodID:           periodID,
		startSequence:      startSequence,
		closeSequence:      closeSequence,
		startAuditSequence: startAuditSequence,
		closeAuditSequence: closeAuditSequence,
	})
}

// SetPendingArchive records a period that needs archiving after the batch is committed.
// The Machine reads this after Merge() to construct and send the ArchiveRequest.
// Can be called multiple times to archive multiple periods in the same batch.
func (b *WriteSet) SetPendingArchive(periodID, startSequence, closeSequence uint64) {
	b.pendingArchives = append(b.pendingArchives, ArchiveRequest{
		PeriodID:      periodID,
		StartSequence: startSequence,
		CloseSequence: closeSequence,
	})
}

// executePurge deletes cold-storable data for a single purge range.
// It also cleans up per-ledger data for any deleted ledgers whose
// DeleteLedger log falls within the purge range.
func (b *WriteSet) executePurge(batch *dal.Batch, pr *purgeRange) error {
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
	}

	// Clean up per-ledger data for deleted ledgers whose delete log
	// falls within this purge range.
	for ledgerID, deleteSeq := range b.fsm.pendingLedgerCleanups {
		if deleteSeq >= pr.startSequence && deleteSeq <= pr.closeSequence {
			if err := deleteLedgerData(batch, ledgerID); err != nil {
				return fmt.Errorf("purging ledger data for ledger %d: %w", ledgerID, err)
			}

			if err := deletePendingLedgerCleanup(batch, ledgerID); err != nil {
				return fmt.Errorf("removing pending cleanup entry for ledger %d: %w", ledgerID, err)
			}

			delete(b.fsm.pendingLedgerCleanups, ledgerID)
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

func (b *WriteSet) GetPreparedQuery(ledgerID uint32, name string) (*commonpb.PreparedQuery, error) {
	pq, err := b.Derived.PreparedQueries.Get(domain.PreparedQueryKey{LedgerID: ledgerID, Name: name})
	// Treat a cache miss as "doesn't exist". A delete in an earlier entry of
	// the same batch will have cleared the cache
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}

	return pq, err
}

func (b *WriteSet) PutPreparedQuery(ledgerID uint32, pq *commonpb.PreparedQuery) {
	b.Derived.PreparedQueries.Put(domain.PreparedQueryKey{LedgerID: ledgerID, Name: pq.GetName()}, pq)
}

func (b *WriteSet) DeletePreparedQuery(ledgerID uint32, name string) {
	b.Derived.PreparedQueries.Delete(domain.PreparedQueryKey{LedgerID: ledgerID, Name: name})
}

// Numscript library operations

func (b *WriteSet) GetNumscriptLatestVersion(ledgerID uint32, name string) (string, error) {
	val, err := b.Derived.NumscriptVersions.Get(domain.NumscriptVersionKey{LedgerID: ledgerID, Name: name})
	if err != nil || val == nil {
		return "", err
	}

	return val.GetVersion(), nil
}

func (b *WriteSet) PutNumscript(ledgerID uint32, info *commonpb.NumscriptInfo) {
	b.Derived.NumscriptVersions.Put(domain.NumscriptVersionKey{LedgerID: ledgerID, Name: info.GetName()}, &commonpb.NumscriptVersionValue{Version: info.GetVersion()})
	b.Derived.NumscriptContents.Put(domain.NumscriptEntryKey{LedgerID: ledgerID, Name: info.GetName(), Version: info.GetVersion()}, info)
}

func (b *WriteSet) DeleteNumscriptLatest(ledgerID uint32, name string) {
	b.Derived.NumscriptVersions.Put(domain.NumscriptVersionKey{LedgerID: ledgerID, Name: name}, &commonpb.NumscriptVersionValue{})
}

func (b *WriteSet) NumscriptVersionExists(ledgerID uint32, name, version string) (bool, error) {
	info, err := b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{LedgerID: ledgerID, Name: name, Version: version})
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

// TransientAccounts returns unique transient account names per ledger,
// collected during Merge from the transient volume partition.
func (b *WriteSet) TransientAccounts() map[uint32][]string {
	return b.transientAccounts
}

// PurgedAccounts returns unique ephemeral account names per ledger whose
// volumes were purged (zero balance), collected during Merge.
func (b *WriteSet) PurgedAccounts() map[uint32][]string {
	return b.purgedAccounts
}

// collectUniqueAccounts extracts unique account names per ledger from
// volume updates.
func collectUniqueAccounts(updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[uint32][]string {
	// Deduplicate: a single account may appear multiple times (one per asset).
	seen := make(map[uint32]map[string]struct{})

	for _, update := range updates {
		ledgerID := update.Key.LedgerID
		account := update.Key.Account

		if seen[ledgerID] == nil {
			seen[ledgerID] = make(map[string]struct{})
		}

		seen[ledgerID][account] = struct{}{}
	}

	result := make(map[uint32][]string, len(seen))
	for ledgerID, accounts := range seen {
		list := make([]string, 0, len(accounts))
		for account := range accounts {
			list = append(list, account)
		}

		result[ledgerID] = list
	}

	return result
}

// Ensure WriteSet implements InMemoryStore.
var _ processing.InMemoryStore = (*WriteSet)(nil)
