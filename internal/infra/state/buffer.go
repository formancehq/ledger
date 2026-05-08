package state

import (
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
	bloomSlice *[][]byte,
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
		*bloomSlice = append(*bloomSlice, update.CanonicalKey)
	}

	for _, deletion := range deletions {
		if err := attr.Delete(batch, deletion.CanonicalKey); err != nil {
			return nil, nil, fmt.Errorf("failed deleting %s attribute: %w", label, err)
		}

		if err := deleteCacheEntry(batch, cacheType, deletion.ID); err != nil {
			return nil, nil, fmt.Errorf("failed deleting %s cache entry: %w", label, err)
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

type Buffered struct {
	fsm                                  *Machine
	attrs                                *attributes.Attributes
	Date                                 *commonpb.Timestamp
	NextSequenceID                       uint64
	NextAuditSequenceID                  uint64
	NextQueryCheckpointID                uint64
	LastLogHash                          []byte
	Derived                              *DerivedRegistry
	pendingSigningKeyUpdates             []signingKeyUpdate
	pendingSigningConfigUpdate           *signingConfigUpdate
	pendingMaintenanceModeUpdate         *maintenanceModeUpdate
	pendingPeriodScheduleUpdate          *string
	pendingQueryCheckpointScheduleUpdate *string
	sinkConfigChanged                    bool
	periods                              *PeriodTracker
	changedPeriods                       []*commonpb.Period
	purgeRanges                          []purgeRange
	pendingArchives                      []ArchiveRequest
	pendingMetadataConvertRequests       []MetadataConvertRequest
	pendingAccountMigrateRequests        []AccountMigrateRequest

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
	transientAccounts map[string][]string

	// purgedAccounts holds unique ephemeral account names per ledger whose
	// volumes were purged (zero balance), populated during Merge.
	purgedAccounts map[string][]string

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

func (b *Buffered) Merge(batch *dal.Batch, logs []*commonpb.Log) error {
	// gen0 byte for incremental 0xFF cache writes.
	genByte := byte(b.fsm.Registry.Cache.CurrentGeneration() % 2)

	// Process Ledger updates
	ledgerUpdates, _, err := mergeAndTrackBloom(b.Derived.Ledgers, b.attrs.Ledger, batch, genByte, dal.AttributeCodeLedger, &b.bloomUpdates.Ledgers, "ledgers")
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
	if err := mergeSimpleWithCache(b.attrs.Volume, batch, genByte, dal.AttributeCodeVolume, partResult.kept); err != nil {
		return fmt.Errorf("failed merging volume attributes: %w", err)
	}

	for _, update := range partResult.kept {
		b.bloomUpdates.Volumes = append(b.bloomUpdates.Volumes, update.CanonicalKey)
	}

	if err := b.applyEphemeralPurge(batch, partResult.purged); err != nil {
		return fmt.Errorf("failed purging ephemeral volumes: %w", err)
	}

	// Evict transient volumes from the in-memory KeyStore. Merge() pushed them
	// to the parent, but they must not persist across batches.
	b.evictTransientVolumes(partResult.transient)

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
	metadataUpdates, metadataDeletions, err := mergeAndTrackBloom(b.Derived.AccountMetadata, b.attrs.Metadata, batch, genByte, dal.AttributeCodeMetadata, &b.bloomUpdates.Metadata, "account metadata")
	if err != nil {
		return err
	}

	// Flush pending reversions to the authoritative in-memory bitset and persist only the modified words.
	type dirtyWord struct {
		ledger    string
		wordIndex uint64
	}

	var dirtyWords []dirtyWord

	for _, txKey := range b.Derived.PendingReversions {
		wi := b.fsm.Registry.SetReverted(txKey)
		dirtyWords = append(dirtyWords, dirtyWord{ledger: txKey.Ledger, wordIndex: wi})
	}

	for _, dw := range dirtyWords {
		word := b.fsm.Registry.Reversions[dw.ledger].Word(dw.wordIndex)
		if err := SaveReversionWord(batch, dw.ledger, dw.wordIndex, word); err != nil {
			return fmt.Errorf("saving reversion word for %s: %w", dw.ledger, err)
		}
	}

	// Process IdempotencyKeys updates
	if _, _, err := mergeAndTrackBloom(b.Derived.IdempotencyKeys, b.attrs.IdempotencyKeys, batch, genByte, dal.AttributeCodeIdempotency, &b.bloomUpdates.Idempotency, "idempotency keys"); err != nil {
		return err
	}

	// Process References updates
	referenceUpdates, _, err := mergeAndTrackBloom(b.Derived.References, b.attrs.References, batch, genByte, dal.AttributeCodeReference, &b.bloomUpdates.References, "references")
	if err != nil {
		return err
	}

	// Update per-ledger attribute counters in boundaries before merging them.
	b.updateBoundaryCounters(volumeUpdates, partResult.purged, partResult.transient, metadataUpdates, metadataDeletions, referenceUpdates)

	// Process Boundary updates (after counted attributes so counters are included).
	if _, _, err := mergeAndTrackBloom(b.Derived.Boundaries, b.attrs.Boundary, batch, genByte, dal.AttributeCodeBoundary, &b.bloomUpdates.Boundaries, "boundaries"); err != nil {
		return err
	}

	// Process Transaction state updates
	if _, _, err := mergeAndTrackBloom(b.Derived.Transactions, b.attrs.Transaction, batch, genByte, dal.AttributeCodeTransaction, &b.bloomUpdates.Transactions, "transactions"); err != nil {
		return err
	}

	err = AppendLogs(batch, logs...)
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
			err := BatchDeletePeriodSchedule(batch)
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
			err := BatchDeleteQueryCheckpointSchedule(batch)
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
	if _, _, err := mergeAndTrackBloom(b.Derived.SinkConfigs, b.attrs.SinkConfig, batch, genByte, dal.AttributeCodeSinkConfig, &b.bloomUpdates.SinkConfigs, "sink configs"); err != nil {
		return err
	}

	// Process NumscriptVersion updates
	if _, _, err := mergeAndTrackBloom(b.Derived.NumscriptVersions, b.attrs.NumscriptVersion, batch, genByte, dal.AttributeCodeNumscriptVersion, &b.bloomUpdates.NumscriptVersions, "numscript versions"); err != nil {
		return err
	}

	// Process NumscriptContent updates
	if _, _, err := mergeAndTrackBloom(b.Derived.NumscriptContents, b.attrs.NumscriptContent, batch, genByte, dal.AttributeCodeNumscriptContent, &b.bloomUpdates.NumscriptContents, "numscript contents"); err != nil {
		return err
	}

	// Process PreparedQuery updates
	if _, _, err := mergeAndTrackBloom(b.Derived.PreparedQueries, b.attrs.PreparedQuery, batch, genByte, dal.AttributeCodePreparedQuery, &b.bloomUpdates.PreparedQueries, "prepared queries"); err != nil {
		return err
	}

	for _, p := range b.changedPeriods {
		err := StorePeriod(batch, p)
		if err != nil {
			return fmt.Errorf("storing period %d: %w", p.GetId(), err)
		}
	}

	if err := StoreNextPeriodID(batch, b.periods.NextPeriodID()); err != nil {
		return fmt.Errorf("storing next period ID: %w", err)
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
		if err := SaveQueryCheckpoint(batch, cp); err != nil {
			return fmt.Errorf("saving query checkpoint %d: %w", cp.GetCheckpointId(), err)
		}
	}

	for _, cpID := range b.pendingQueryCheckpointDeletes {
		if err := DeleteQueryCheckpointFromBatch(batch, cpID); err != nil {
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

			if err := SavePendingLedgerCleanup(batch, ledgerName, seq); err != nil {
				return fmt.Errorf("saving pending ledger cleanup for %q: %w", ledgerName, err)
			}

			b.fsm.pendingLedgerCleanups[ledgerName] = seq

			// Boundary deletion is handled above via boundaryDeletions
			// (MarkLedgerForCleanup adds a Delete to the Derived.Boundaries overlay).

			// Clean in-memory reversion bitset and Pebble words — not needed after deletion.
			delete(b.fsm.Registry.Reversions, ledgerName)

			if err := DeleteReversionsByLedger(batch, ledgerName); err != nil {
				return fmt.Errorf("deleting reversions for %q: %w", ledgerName, err)
			}
		}
	}

	// Persist next query checkpoint ID if it changed.
	if b.NextQueryCheckpointID != b.fsm.nextQueryCheckpointID {
		if err := StoreNextQueryCheckpointID(batch, b.NextQueryCheckpointID); err != nil {
			return fmt.Errorf("storing next query checkpoint ID: %w", err)
		}
	}

	b.fsm.nextSequenceID = b.NextSequenceID
	b.fsm.nextQueryCheckpointID = b.NextQueryCheckpointID
	b.fsm.lastLogHash = b.LastLogHash

	// Apply changed periods to Machine's Periods tracker
	for _, p := range b.changedPeriods {
		b.fsm.Periods.PutPeriod(p)
	}

	// Remove purged periods from memory
	for _, pr := range b.purgeRanges {
		b.fsm.Periods.DeletePeriod(pr.periodID)
	}

	b.fsm.Periods.SetCurrentOpenPeriod(b.periods.CurrentOpenPeriod())
	b.fsm.Periods.SetClosingPeriods(b.periods.ClosingPeriods())
	b.fsm.Periods.SetNextPeriodID(b.periods.NextPeriodID())

	return nil
}

func NewBuffer(at *commonpb.Timestamp, fsm *Machine) *Buffered {
	return &Buffered{
		fsm:                   fsm,
		attrs:                 fsm.Registry.Attrs,
		Date:                  at,
		Derived:               NewDerivedRegistry(fsm.Registry),
		NextSequenceID:        fsm.nextSequenceID,
		NextAuditSequenceID:   fsm.nextAuditSequenceID,
		NextQueryCheckpointID: fsm.nextQueryCheckpointID,
		LastLogHash:           fsm.lastLogHash,
		periods:               fsm.Periods.Clone(),
	}
}

// Store interface implementation for Buffered

func (b *Buffered) GetLedger(name string) (*commonpb.LedgerInfo, bool) {
	info, err := b.Derived.Ledgers.Get(domain.LedgerKey{Name: name})
	if err != nil || info == nil {
		return nil, false
	}

	return info, true
}

func (b *Buffered) PutLedger(name string, info *commonpb.LedgerInfo) {
	b.Derived.Ledgers.Put(domain.LedgerKey{Name: name}, info)
}

func (b *Buffered) MarkLedgerForCleanup(ledger string) {
	b.pendingLedgerDeletions = append(b.pendingLedgerDeletions, ledger)
	// Remove boundary from the in-memory overlay so that subsequent
	// GetBoundaries calls return (nil, false) — both within this proposal
	// and in future proposals after Merge propagates the deletion.
	b.Derived.Boundaries.Delete(domain.LedgerKey{Name: ledger})
}

func (b *Buffered) GetBoundaries(ledger string) (*raftcmdpb.LedgerBoundaries, bool) {
	boundaries, err := b.Derived.Boundaries.Get(domain.LedgerKey{Name: ledger})
	if err != nil || boundaries == nil {
		return nil, false
	}

	return boundaries, true
}

func (b *Buffered) ResolveNumscriptContent(ledger, name, version string) (*commonpb.NumscriptInfo, error) {
	return b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{Ledger: ledger, Name: name, Version: version})
}

func (b *Buffered) PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
	b.Derived.Boundaries.Put(domain.LedgerKey{Name: ledger}, boundaries)
}

func (b *Buffered) GetVolume(key domain.VolumeKey) (*raftcmdpb.VolumePair, error) {
	return b.Derived.Volumes.Get(key)
}

func (b *Buffered) PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
	b.Derived.Volumes.Put(key, value)
}

// ValidateTransientVolumes checks that all transient account volumes have zero balance.
// Must be called after ProcessOrders and before Commit, so that failures are
// treated as business errors (rejected proposals) rather than fatal FSM errors.
//
// Transient validation only applies when the base volume (before this batch) is zero
// or absent. Pre-existing non-zero volumes from before the account was marked transient
// are treated as normal and skip the zero-balance check.
func (b *Buffered) ValidateTransientVolumes() error {
	ledgerTypes := make(map[string][]accounttype.CompiledType)

	for key, vol := range b.Derived.Volumes.DirtyValues() {
		compiled, ok := ledgerTypes[key.Ledger]
		if !ok {
			info, infoOK := b.GetLedger(key.Ledger)
			if !infoOK {
				continue
			}

			compiled = accounttype.CompileTypes(info.GetAccountTypes())
			ledgerTypes[key.Ledger] = compiled
		}

		matched := accounttype.FindMatchingType(key.Account, compiled)
		if matched == nil || matched.GetPersistence() != commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			continue
		}

		// Check if the parent KeyStore has a pre-existing non-zero volume.
		// If so, the account had volumes before being marked transient — skip validation.
		baseVol, _, baseErr := b.fsm.Registry.Volumes.Get(key.Bytes())
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

func (b *Buffered) GetAccountMetadata(key domain.MetadataKey) (*commonpb.MetadataValue, error) {
	return b.Derived.AccountMetadata.Get(key)
}

func (b *Buffered) PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue) {
	b.Derived.AccountMetadata.Put(key, value)
}

func (b *Buffered) DeleteAccountMetadata(key domain.MetadataKey) {
	b.Derived.AccountMetadata.Delete(key)
}

func (b *Buffered) GetReverted(key domain.TransactionKey) (bool, error) {
	return b.Derived.GetReverted(key), nil
}

func (b *Buffered) PutReverted(key domain.TransactionKey, reverted bool) {
	if reverted {
		b.Derived.PutReverted(key)
	}
}

func (b *Buffered) GetIdempotencyKey(key domain.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error) {
	return b.Derived.IdempotencyKeys.Get(key)
}

func (b *Buffered) PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	b.Derived.IdempotencyKeys.Put(key, value)
}

func (b *Buffered) GetTransactionReference(key domain.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error) {
	return b.Derived.References.Get(key)
}

func (b *Buffered) PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	b.Derived.References.Put(key, value)
}

func (b *Buffered) GetTransactionState(key domain.TransactionKey) (*commonpb.TransactionState, error) {
	return b.Derived.Transactions.Get(key)
}

func (b *Buffered) PutTransactionState(key domain.TransactionKey, state *commonpb.TransactionState) {
	b.Derived.Transactions.Put(key, state)
}

func (b *Buffered) AddSigningKey(keyID string, publicKey []byte, parentKeyID string) {
	b.pendingSigningKeyUpdates = append(b.pendingSigningKeyUpdates, signingKeyUpdate{
		keyID:       keyID,
		publicKey:   publicKey,
		parentKeyID: parentKeyID,
	})
}

func (b *Buffered) RemoveSigningKey(keyID string) {
	b.pendingSigningKeyUpdates = append(b.pendingSigningKeyUpdates, signingKeyUpdate{
		keyID:  keyID,
		remove: true,
	})
}

// GetSigningKeyChildren returns all key IDs that have keyID as their parent.
// It checks the committed KeyStore and accounts for pending additions/removals.
func (b *Buffered) GetSigningKeyChildren(keyID string) []string {
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

func (b *Buffered) SetRequireSignatures(require bool) {
	b.pendingSigningConfigUpdate = &signingConfigUpdate{
		requireSignatures: require,
	}
}

func (b *Buffered) SetMaintenanceMode(enabled bool) {
	b.pendingMaintenanceModeUpdate = &maintenanceModeUpdate{
		enabled: enabled,
	}
}

func (b *Buffered) SetPeriodSchedule(cronExpr string) {
	b.pendingPeriodScheduleUpdate = &cronExpr
}

func (b *Buffered) DeletePeriodSchedule() {
	empty := ""
	b.pendingPeriodScheduleUpdate = &empty
}

func (b *Buffered) SetQueryCheckpointSchedule(cronExpr string) {
	b.pendingQueryCheckpointScheduleUpdate = &cronExpr
}

func (b *Buffered) DeleteQueryCheckpointSchedule() {
	empty := ""
	b.pendingQueryCheckpointScheduleUpdate = &empty
}

func (b *Buffered) GetSinkConfig(name string) (*commonpb.SinkConfig, error) {
	cfg, err := b.Derived.SinkConfigs.Get(domain.SinkConfigKey{Name: name})
	if err != nil {
		return nil, nil
	}

	return cfg, nil
}

func (b *Buffered) AddSinkConfig(config *commonpb.SinkConfig) {
	b.Derived.SinkConfigs.Put(domain.SinkConfigKey{Name: config.GetName()}, config)
	b.sinkConfigChanged = true
}

func (b *Buffered) RemoveSinkConfig(name string) {
	b.Derived.SinkConfigs.Delete(domain.SinkConfigKey{Name: name})
	b.sinkConfigChanged = true
}

func (b *Buffered) HasPendingSinkChanges() bool {
	return b.sinkConfigChanged
}

// AllVolumeUpdates returns all volume updates (kept + purged) captured during Merge.
// Used for delta/posting cross-check which needs purged ephemeral entries too.
func (b *Buffered) AllVolumeUpdates() []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	return b.allVolumeUpdates
}

// KeptVolumeUpdates returns only kept volume updates (excluding ephemeral purges).
// Used for post-commit Pebble verification where purged entries are intentionally absent.
func (b *Buffered) KeptVolumeUpdates() []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	return b.keptVolumeUpdates
}

func (b *Buffered) GetNextSequenceID() uint64 {
	return b.NextSequenceID
}

func (b *Buffered) GetNextAuditSequenceID() uint64 {
	return b.NextAuditSequenceID
}

func (b *Buffered) IncrementNextSequenceID() uint64 {
	id := b.NextSequenceID
	b.NextSequenceID++

	return id
}

func (b *Buffered) GetDate() *commonpb.Timestamp {
	return b.Date
}

func (b *Buffered) GetLastLogHash() []byte {
	return b.LastLogHash
}

func (b *Buffered) SetLastLogHash(hash []byte) {
	b.LastLogHash = hash
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

func (b *Buffered) GetCurrentOpenPeriod() (*commonpb.Period, bool) {
	p := b.periods.CurrentOpenPeriod()
	if p != nil {
		return p, true
	}

	return nil, false
}

func (b *Buffered) GetClosingPeriods() []*commonpb.Period {
	return b.periods.ClosingPeriods()
}

func (b *Buffered) GetClosingPeriodByID(periodID uint64) (*commonpb.Period, bool) {
	return b.periods.ClosingPeriodByID(periodID)
}

func (b *Buffered) SetCurrentOpenPeriod(period *commonpb.Period) {
	b.periods.SetCurrentOpenPeriod(period)
	b.changedPeriods = append(b.changedPeriods, period)
}

func (b *Buffered) AddClosingPeriod(period *commonpb.Period) {
	b.periods.AddClosingPeriod(period)
	b.changedPeriods = append(b.changedPeriods, period)
}

// RemoveClosingPeriod persists the closing period's final state and removes it from in-memory tracking.
func (b *Buffered) RemoveClosingPeriod(periodID uint64) {
	if closing, ok := b.periods.ClosingPeriodByID(periodID); ok {
		b.changedPeriods = append(b.changedPeriods, closing)
	}

	b.periods.RemoveClosingPeriod(periodID)
}

func (b *Buffered) GetNextPeriodID() uint64 {
	return b.periods.NextPeriodID()
}

func (b *Buffered) IncrementNextPeriodID() uint64 {
	id := b.periods.NextPeriodID()
	b.periods.SetNextPeriodID(id + 1)

	return id
}

// GetPeriodByID looks up a period by ID from in-memory state only.
// It checks changedPeriods first (most recent modifications), then the periods tracker.
func (b *Buffered) GetPeriodByID(periodID uint64) (*commonpb.Period, bool) {
	// Check changedPeriods (most recently changed first)
	for i := len(b.changedPeriods) - 1; i >= 0; i-- {
		if b.changedPeriods[i].GetId() == periodID {
			return b.changedPeriods[i], true
		}
	}

	return b.periods.GetPeriodByID(periodID)
}

// UpdatePeriod records a period modification to be persisted in Merge().
func (b *Buffered) UpdatePeriod(period *commonpb.Period) {
	b.changedPeriods = append(b.changedPeriods, period)
}

// SetPurgeRange records sequence ranges to be purged during Merge().
// Log and audit entries have independent sequence counters (audit advances
// slower due to batching), so both ranges are needed for correct purging.
func (b *Buffered) SetPurgeRange(periodID, startSequence, closeSequence, startAuditSequence, closeAuditSequence uint64) {
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
func (b *Buffered) SetPendingArchive(periodID, startSequence, closeSequence uint64) {
	b.pendingArchives = append(b.pendingArchives, ArchiveRequest{
		PeriodID:      periodID,
		StartSequence: startSequence,
		CloseSequence: closeSequence,
	})
}

// executePurge deletes cold-storable data for a single purge range.
// It also cleans up per-ledger data for any deleted ledgers whose
// DeleteLedger log falls within the purge range.
func (b *Buffered) executePurge(batch *dal.Batch, pr *purgeRange) error {
	// Logs: purge using log sequence range.
	logStart := dal.NewKeyBuilder().PutByte(dal.KeyPrefixLog).PutUint64(pr.startSequence).Build()
	logEnd := dal.NewKeyBuilder().PutByte(dal.KeyPrefixLog).PutUint64(pr.closeSequence + 1).Build()

	if err := batch.DeleteRange(logStart, logEnd, nil); err != nil {
		return fmt.Errorf("purging logs [%d, %d]: %w", pr.startSequence, pr.closeSequence, err)
	}

	// Audit: purge using audit sequence range (independent counter, advances slower).
	if pr.closeAuditSequence >= pr.startAuditSequence {
		auditStart := dal.NewKeyBuilder().PutByte(dal.KeyPrefixAudit).PutUint64(pr.startAuditSequence).Build()
		auditEnd := dal.NewKeyBuilder().PutByte(dal.KeyPrefixAudit).PutUint64(pr.closeAuditSequence + 1).Build()

		if err := batch.DeleteRange(auditStart, auditEnd, nil); err != nil {
			return fmt.Errorf("purging audit [%d, %d]: %w", pr.startAuditSequence, pr.closeAuditSequence, err)
		}
	}

	// Clean up per-ledger data for deleted ledgers whose delete log
	// falls within this purge range.
	for ledgerName, deleteSeq := range b.fsm.pendingLedgerCleanups {
		if deleteSeq >= pr.startSequence && deleteSeq <= pr.closeSequence {
			if err := DeleteLedgerData(batch, ledgerName); err != nil {
				return fmt.Errorf("purging ledger data for %q: %w", ledgerName, err)
			}

			if err := DeletePendingLedgerCleanup(batch, ledgerName); err != nil {
				return fmt.Errorf("removing pending cleanup entry for %q: %w", ledgerName, err)
			}

			delete(b.fsm.pendingLedgerCleanups, ledgerName)
		}
	}

	return nil
}

func (b *Buffered) AddMetadataConvertRequest(ledgerName string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType) {
	b.pendingMetadataConvertRequests = append(b.pendingMetadataConvertRequests, MetadataConvertRequest{
		LedgerName: ledgerName,
		TargetType: targetType,
		Key:        key,
		Type:       metadataType,
	})
}

// MetadataConvertRequests returns the accumulated metadata conversion requests.
func (b *Buffered) MetadataConvertRequests() []MetadataConvertRequest {
	return b.pendingMetadataConvertRequests
}

func (b *Buffered) AddAccountMigrateRequest(ledgerName, accountTypeName, oldPattern, targetPattern string) {
	b.pendingAccountMigrateRequests = append(b.pendingAccountMigrateRequests, AccountMigrateRequest{
		LedgerName:      ledgerName,
		AccountTypeName: accountTypeName,
		OldPattern:      oldPattern,
		TargetPattern:   targetPattern,
	})
}

// AccountMigrateRequests returns the accumulated account migration requests.
func (b *Buffered) AccountMigrateRequests() []AccountMigrateRequest {
	return b.pendingAccountMigrateRequests
}

// HasPurges returns true if the buffer contains any pending purge ranges.
func (b *Buffered) HasPurges() bool {
	return len(b.purgeRanges) > 0
}

func (b *Buffered) GetPreparedQuery(ledger, name string) (*commonpb.PreparedQuery, error) {
	return b.Derived.PreparedQueries.Get(domain.PreparedQueryKey{Ledger: ledger, Name: name})
}

func (b *Buffered) PutPreparedQuery(pq *commonpb.PreparedQuery) {
	b.Derived.PreparedQueries.Put(domain.PreparedQueryKey{Ledger: pq.GetLedger(), Name: pq.GetName()}, pq)
}

func (b *Buffered) DeletePreparedQuery(ledger, name string) {
	b.Derived.PreparedQueries.Delete(domain.PreparedQueryKey{Ledger: ledger, Name: name})
}

// Numscript library operations

func (b *Buffered) GetNumscriptLatestVersion(ledger, name string) (string, error) {
	val, err := b.Derived.NumscriptVersions.Get(domain.NumscriptVersionKey{Ledger: ledger, Name: name})
	if err != nil || val == nil {
		return "", err
	}

	return val.GetVersion(), nil
}

func (b *Buffered) PutNumscript(info *commonpb.NumscriptInfo) {
	b.Derived.NumscriptVersions.Put(domain.NumscriptVersionKey{Ledger: info.GetLedger(), Name: info.GetName()}, &commonpb.NumscriptVersionValue{Version: info.GetVersion()})
	b.Derived.NumscriptContents.Put(domain.NumscriptEntryKey{Ledger: info.GetLedger(), Name: info.GetName(), Version: info.GetVersion()}, info)
}

func (b *Buffered) DeleteNumscriptLatest(ledger, name string) {
	b.Derived.NumscriptVersions.Put(domain.NumscriptVersionKey{Ledger: ledger, Name: name}, &commonpb.NumscriptVersionValue{})
}

func (b *Buffered) NumscriptVersionExists(ledger, name, version string) (bool, error) {
	info, err := b.Derived.NumscriptContents.Get(domain.NumscriptEntryKey{Ledger: ledger, Name: name, Version: version})
	if err != nil {
		// Not in cache — treat as not existing (admission ensures preloading)
		return false, nil
	}

	return info != nil, nil
}

func (b *Buffered) GetNextQueryCheckpointID() uint64 {
	return b.NextQueryCheckpointID
}

func (b *Buffered) IncrementNextQueryCheckpointID() uint64 {
	id := b.NextQueryCheckpointID
	b.NextQueryCheckpointID++

	return id
}

// SaveQueryCheckpoint stores a query checkpoint for Merge.
func (b *Buffered) SaveQueryCheckpoint(cp *raftcmdpb.QueryCheckpointState) {
	b.pendingQueryCheckpointSaves = append(b.pendingQueryCheckpointSaves, cp)
}

// DeleteQueryCheckpoint marks a query checkpoint for deletion during Merge.
func (b *Buffered) DeleteQueryCheckpoint(checkpointID uint64) {
	b.pendingQueryCheckpointDeletes = append(b.pendingQueryCheckpointDeletes, checkpointID)
}

// BloomUpdates returns the canonical keys collected during Merge for bloom filter updates.
func (b *Buffered) BloomUpdates() *bloom.BloomUpdates {
	return &b.bloomUpdates
}

// PurgedVolumeKeys returns the keys of volumes that were purged by ephemeral purge.
// Used to exclude these keys from post-commit Pebble verification when a later entry
// in the same ApplyEntries batch purges a volume that was written by an earlier entry.
func (b *Buffered) PurgedVolumeKeys() []domain.VolumeKey {
	return b.purgedVolumeKeys
}

// TransientAccounts returns unique transient account names per ledger,
// collected during Merge from the transient volume partition.
func (b *Buffered) TransientAccounts() map[string][]string {
	return b.transientAccounts
}

// PurgedAccounts returns unique ephemeral account names per ledger whose
// volumes were purged (zero balance), collected during Merge.
func (b *Buffered) PurgedAccounts() map[string][]string {
	return b.purgedAccounts
}

// collectUniqueAccounts extracts unique account names per ledger from
// volume updates.
func collectUniqueAccounts(updates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]) map[string][]string {
	// Deduplicate: a single account may appear multiple times (one per asset).
	seen := make(map[string]map[string]struct{})

	for _, update := range updates {
		ledger := update.Key.Ledger
		account := update.Key.Account

		if seen[ledger] == nil {
			seen[ledger] = make(map[string]struct{})
		}

		seen[ledger][account] = struct{}{}
	}

	result := make(map[string][]string, len(seen))
	for ledger, accounts := range seen {
		list := make([]string, 0, len(accounts))
		for account := range accounts {
			list = append(list, account)
		}

		result[ledger] = list
	}

	return result
}

// Ensure Buffered implements InMemoryStore.
var _ processing.InMemoryStore = (*Buffered)(nil)
