package state

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/holiman/uint256"
	"google.golang.org/protobuf/proto"
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

// auditConfigUpdate represents a pending audit config change.
type auditConfigUpdate struct {
	enabled bool
}

type Buffered struct {
	fsm                            *Machine
	attrs                          *attributes.Attributes
	Date                           *commonpb.Timestamp
	NextSequenceID                 uint64
	LastLogHash                    []byte
	Ledgers                        *attributes.DerivedKeyStore[domain.LedgerKey, *commonpb.LedgerInfo]
	Boundaries                     *attributes.DerivedKeyStore[domain.LedgerKey, *raftcmdpb.LedgerBoundaries]
	Volumes                        *attributes.DerivedKeyStore[domain.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata                *attributes.DerivedKeyStore[domain.MetadataKey, *commonpb.MetadataValue]
	Reversions                     *attributes.DerivedKeyStore[domain.TransactionKey, bool]
	IdempotencyKeys                *attributes.DerivedKeyStore[domain.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References                     *attributes.DerivedKeyStore[domain.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	SinkConfigs                    *attributes.DerivedKeyStore[domain.SinkConfigKey, *commonpb.SinkConfig]
	TransactionsUpdates            map[domain.TransactionKey][]*commonpb.TransactionUpdate
	PendingLogs                    []*commonpb.Log
	pendingSigningKeyUpdates       []signingKeyUpdate
	pendingSigningConfigUpdate     *signingConfigUpdate
	pendingMaintenanceModeUpdate   *maintenanceModeUpdate
	pendingAuditConfigUpdate       *auditConfigUpdate
	pendingPeriodScheduleUpdate    *string
	sinkConfigChanged              bool
	allPeriods                     map[uint64]*commonpb.Period
	currentOpenPeriod              *commonpb.Period
	closingPeriod                  *commonpb.Period
	changedPeriods                 []*commonpb.Period
	NextPeriodID                   uint64
	purgeRanges                    []purgeRange
	pendingArchives                []ArchiveRequest
	pendingMetadataConvertRequests []MetadataConvertRequest
}

// purgeRange identifies a period's sequence range to delete from Pebble during Merge().
type purgeRange struct {
	periodID      uint64
	startSequence uint64
	closeSequence uint64
}

func (b *Buffered) Merge(index uint64, batch *dal.Batch) error {
	// Process Ledger updates
	ledgerUpdates, _, err := b.Ledgers.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge ledgers: %w", err)
	}
	for _, update := range ledgerUpdates {
		if err := b.attrs.Ledger.SetBase(batch, index, update.CanonicalKey, update.New); err != nil {
			return fmt.Errorf("failed setting ledger base: %w", err)
		}
		if err := SaveLedger(batch, update.New); err != nil {
			return fmt.Errorf("failed to save ledger: %w", err)
		}
		if err := b.attrs.Ledger.DeleteOldest(batch, index, update.CanonicalKey); err != nil {
			return fmt.Errorf("compacting old ledger base: %w", err)
		}
	}

	// Process Boundary updates — track dirty keys for deferred Pebble write at generation rotation
	boundaryUpdates, _, err := b.Boundaries.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge boundaries: %w", err)
	}
	for _, update := range boundaryUpdates {
		b.fsm.dirtyBoundaryKeys[string(update.CanonicalKey)] = update.New
	}

	// Process Volume updates and track dirty volume keys inline
	volumeUpdates, _, err := b.Volumes.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge volumes: %w", err)
	}
	for _, update := range volumeUpdates {
		// Normalize for Pebble storage: the Known/Diff distinction is an in-memory
		// optimization only. In Pebble, values are always stored in InputKnown/OutputKnown.
		storePair := &raftcmdpb.VolumePair{
			InputKnown:  coalesceVolumeSide(update.New.InputKnown, update.New.InputDiff),
			OutputKnown: coalesceVolumeSide(update.New.OutputKnown, update.New.OutputDiff),
		}

		// If the original VolumePair had Known values, write as SetBase (absolute).
		// Otherwise, write as AddDiff (cumulative delta).
		if update.New.InputKnown != nil || update.New.OutputKnown != nil {
			if err := b.attrs.Volume.SetBase(batch, index, update.CanonicalKey, storePair); err != nil {
				return fmt.Errorf("could not set volume base: %w", err)
			}
		} else {
			if err := b.attrs.Volume.AddDiff(batch, index, update.CanonicalKey, storePair); err != nil {
				return fmt.Errorf("failed adding volume diff: %w", err)
			}
		}
		b.fsm.dirtyVolumeKeys[0][string(update.CanonicalKey)]++
	}

	// Defensive check: double-entry invariant.
	if err := checkDoubleEntryInvariant(volumeUpdates); err != nil {
		return err
	}

	accountMetadataUpdates, accountMetadataDeletions, err := b.AccountMetadata.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge account metadata: %w", err)
	}
	for _, update := range accountMetadataUpdates {
		err := b.attrs.Metadata.AddDiff(batch, index, update.CanonicalKey, update.New)
		if err != nil {
			return fmt.Errorf("failed adding diff between old and new attribute: %v", err)
		}
	}
	for _, deletion := range accountMetadataDeletions {
		if err := b.attrs.Metadata.Delete(batch, deletion.CanonicalKey); err != nil {
			return fmt.Errorf("failed deleting metadata attribute: %v", err)
		}
	}


	// Process Reversions updates
	reversionUpdates, _, err := b.Reversions.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge reversions: %w", err)
	}
	for _, update := range reversionUpdates {
		// Reverted status is a simple boolean, we store it as a base value
		err := b.attrs.Reverted.SetBase(batch, index, update.CanonicalKey, &commonpb.RevertedValue{Reverted: update.New})
		if err != nil {
			return fmt.Errorf("failed setting reverted base: %w", err)
		}
		if err := b.attrs.Reverted.DeleteOldest(batch, index, update.CanonicalKey); err != nil {
			return fmt.Errorf("compacting old reverted base: %w", err)
		}
	}

	// Process IdempotencyKeys updates
	idempotencyUpdates, _, err := b.IdempotencyKeys.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge idempotency keys: %w", err)
	}
	for _, update := range idempotencyUpdates {
		// Idempotency keys are immutable once set, store as base value
		err := b.attrs.IdempotencyKeys.SetBase(batch, index, update.CanonicalKey, update.New)
		if err != nil {
			return fmt.Errorf("failed setting idempotency key base: %w", err)
		}
		if err := b.attrs.IdempotencyKeys.DeleteOldest(batch, index, update.CanonicalKey); err != nil {
			return fmt.Errorf("compacting old idempotency key base: %w", err)
		}
	}

	// Process References updates
	referenceUpdates, _, err := b.References.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge references: %w", err)
	}
	for _, update := range referenceUpdates {
		// References are immutable once set, store as base value
		err := b.attrs.References.SetBase(batch, index, update.CanonicalKey, update.New)
		if err != nil {
			return fmt.Errorf("failed setting reference base: %w", err)
		}
		if err := b.attrs.References.DeleteOldest(batch, index, update.CanonicalKey); err != nil {
			return fmt.Errorf("compacting old reference base: %w", err)
		}
	}

	for key, updates := range b.TransactionsUpdates {
		for _, update := range updates {
			err := StoreTransactionUpdate(batch, key, update)
			if err != nil {
				return fmt.Errorf("failed storing transaction update for ledger %s: %w", key.Ledger, err)
			}
		}
	}

	err = AppendLogs(batch, b.PendingLogs...)
	if err != nil {
		return fmt.Errorf("failed appending pending logs: %w", err)
	}

	// Apply signing key updates to Pebble batch and in-memory KeyStore
	for _, update := range b.pendingSigningKeyUpdates {
		if update.remove {
			if err := DeleteSigningKey(batch, update.keyID); err != nil {
				return fmt.Errorf("deleting signing key: %w", err)
			}
			if b.fsm.keyStore != nil {
				b.fsm.keyStore.RemovePublicKey(update.keyID)
			}
		} else {
			if err := SaveSigningKey(batch, update.keyID, update.publicKey, update.parentKeyID); err != nil {
				return fmt.Errorf("saving signing key: %w", err)
			}
			if b.fsm.keyStore != nil {
				b.fsm.keyStore.AddPublicKey(update.keyID, update.publicKey, update.parentKeyID)
			}
		}
	}
	if b.pendingSigningConfigUpdate != nil {
		if err := SaveSigningConfig(batch, b.pendingSigningConfigUpdate.requireSignatures); err != nil {
			return fmt.Errorf("saving signing config: %w", err)
		}
		b.fsm.sharedState.SetRequireSignatures(b.pendingSigningConfigUpdate.requireSignatures)
	}
	if b.pendingMaintenanceModeUpdate != nil {
		if err := SaveMaintenanceMode(batch, b.pendingMaintenanceModeUpdate.enabled); err != nil {
			return fmt.Errorf("saving maintenance mode: %w", err)
		}
		b.fsm.sharedState.SetMaintenanceMode(b.pendingMaintenanceModeUpdate.enabled)
	}
	if b.pendingAuditConfigUpdate != nil {
		if err := SaveAuditConfig(batch, b.pendingAuditConfigUpdate.enabled); err != nil {
			return fmt.Errorf("saving audit config: %w", err)
		}
		b.fsm.sharedState.SetAuditEnabled(b.pendingAuditConfigUpdate.enabled)
	}
	if b.pendingPeriodScheduleUpdate != nil {
		if *b.pendingPeriodScheduleUpdate == "" {
			if err := BatchDeletePeriodSchedule(batch); err != nil {
				return fmt.Errorf("deleting period schedule: %w", err)
			}
		} else {
			if err := SavePeriodSchedule(batch, *b.pendingPeriodScheduleUpdate); err != nil {
				return fmt.Errorf("saving period schedule: %w", err)
			}
		}
		b.fsm.periodSchedule = *b.pendingPeriodScheduleUpdate
		b.fsm.scheduleChanged.Notify()
	}

	sinkUpdates, sinkDeletions, err := b.SinkConfigs.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge sink configs: %w", err)
	}
	for _, update := range sinkUpdates {
		if err := SaveSinkConfig(batch, update.New); err != nil {
			return fmt.Errorf("saving sink config %q: %w", update.Key.Name, err)
		}
	}
	for _, deletion := range sinkDeletions {
		if err := DeleteSinkConfig(batch, deletion.Key.Name); err != nil {
			return fmt.Errorf("deleting sink config %q: %w", deletion.Key.Name, err)
		}
	}

	for _, p := range b.changedPeriods {
		if err := StorePeriod(batch, p); err != nil {
			return fmt.Errorf("storing period %d: %w", p.Id, err)
		}
	}
	if err := StoreNextPeriodID(batch, b.NextPeriodID); err != nil {
		return fmt.Errorf("storing next period ID: %w", err)
	}

	// Purge archived period data (logs + audit entries) if requested
	for i := range b.purgeRanges {
		if err := b.executePurge(batch, &b.purgeRanges[i]); err != nil {
			return fmt.Errorf("purging archived period %d data: %w", b.purgeRanges[i].periodID, err)
		}
	}

	b.PendingLogs = nil
	b.fsm.nextSequenceID = b.NextSequenceID
	b.fsm.lastLogHash = b.LastLogHash

	// Apply changed periods to Machine's allPeriods
	for _, p := range b.changedPeriods {
		b.fsm.allPeriods[p.Id] = p
	}

	// Remove purged periods from memory
	for _, pr := range b.purgeRanges {
		delete(b.fsm.allPeriods, pr.periodID)
	}

	b.fsm.currentOpenPeriod = b.currentOpenPeriod
	b.fsm.closingPeriod = b.closingPeriod
	b.fsm.nextPeriodID = b.NextPeriodID

	return nil
}

func NewBuffer(at *commonpb.Timestamp, fsm *Machine) *Buffered {
	// Clone allPeriods from Machine; currentOpenPeriod/closingPeriod point into the cloned map.
	clonedAll := make(map[uint64]*commonpb.Period, len(fsm.allPeriods))
	for id, p := range fsm.allPeriods {
		clonedAll[id] = proto.CloneOf(p)
	}

	var clonedOpen, clonedClosing *commonpb.Period
	if fsm.currentOpenPeriod != nil {
		clonedOpen = clonedAll[fsm.currentOpenPeriod.Id]
	}
	if fsm.closingPeriod != nil {
		clonedClosing = clonedAll[fsm.closingPeriod.Id]
	}

	return &Buffered{
		fsm:                 fsm,
		attrs:               fsm.Attrs,
		Date:                at,
		Ledgers:             attributes.NewDerivedKeyStore(fsm.Ledgers, (*commonpb.LedgerInfo).CloneVT),
		Boundaries:          attributes.NewDerivedKeyStore(fsm.Boundaries, (*raftcmdpb.LedgerBoundaries).CloneVT),
		NextSequenceID:      fsm.nextSequenceID,
		LastLogHash:         fsm.lastLogHash,
		Volumes:             attributes.NewDerivedKeyStore(fsm.Volumes, (*raftcmdpb.VolumePair).CloneVT),
		AccountMetadata:     attributes.NewDerivedKeyStore(fsm.AccountMetadata, (*commonpb.MetadataValue).CloneVT),
		Reversions:          attributes.NewDerivedKeyStore(fsm.Reversions, nil), // bool is a value type, no clone needed
		IdempotencyKeys:     attributes.NewDerivedKeyStore(fsm.IdempotencyKeys, (*commonpb.IdempotencyKeyValue).CloneVT),
		References:          attributes.NewDerivedKeyStore(fsm.References, (*commonpb.TransactionReferenceValue).CloneVT),
		SinkConfigs:         attributes.NewDerivedKeyStore(fsm.SinkConfigs, (*commonpb.SinkConfig).CloneVT),
		TransactionsUpdates: make(map[domain.TransactionKey][]*commonpb.TransactionUpdate),
		allPeriods:          clonedAll,
		currentOpenPeriod:   clonedOpen,
		closingPeriod:       clonedClosing,
		NextPeriodID:        fsm.nextPeriodID,
	}
}

// Store interface implementation for Buffered

func (b *Buffered) GetLedger(name string) (*commonpb.LedgerInfo, bool) {
	info, err := b.Ledgers.Get(domain.LedgerKey{Name: name})
	if err != nil || info == nil {
		return nil, false
	}
	return info, true
}

func (b *Buffered) PutLedger(name string, info *commonpb.LedgerInfo) {
	b.Ledgers.Put(domain.LedgerKey{Name: name}, info)
}

func (b *Buffered) GetBoundaries(ledger string) (*raftcmdpb.LedgerBoundaries, bool) {
	boundaries, err := b.Boundaries.Get(domain.LedgerKey{Name: ledger})
	if err != nil || boundaries == nil {
		return nil, false
	}
	return boundaries, true
}

func (b *Buffered) PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
	b.Boundaries.Put(domain.LedgerKey{Name: ledger}, boundaries)
}

func (b *Buffered) GetVolume(key domain.VolumeKey) (*raftcmdpb.VolumePair, error) {
	return b.Volumes.Get(key)
}

func (b *Buffered) PutVolume(key domain.VolumeKey, value *raftcmdpb.VolumePair) {
	b.Volumes.Put(key, value)
}

func (b *Buffered) GetAccountMetadata(key domain.MetadataKey) (*commonpb.MetadataValue, error) {
	return b.AccountMetadata.Get(key)
}

func (b *Buffered) PutAccountMetadata(key domain.MetadataKey, value *commonpb.MetadataValue) {
	b.AccountMetadata.Put(key, value)
}

func (b *Buffered) DeleteAccountMetadata(key domain.MetadataKey) {
	b.AccountMetadata.Delete(key)
}

func (b *Buffered) GetReverted(key domain.TransactionKey) (bool, error) {
	return b.Reversions.Get(key)
}

func (b *Buffered) PutReverted(key domain.TransactionKey, reverted bool) {
	b.Reversions.Put(key, reverted)
}

func (b *Buffered) GetIdempotencyKey(key domain.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error) {
	return b.IdempotencyKeys.Get(key)
}

func (b *Buffered) PutIdempotencyKey(key domain.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	b.IdempotencyKeys.Put(key, value)
}

func (b *Buffered) GetTransactionReference(key domain.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error) {
	return b.References.Get(key)
}

func (b *Buffered) PutTransactionReference(key domain.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	b.References.Put(key, value)
}

func (b *Buffered) AddTransactionUpdate(key domain.TransactionKey, update *commonpb.TransactionUpdate) {
	b.TransactionsUpdates[key] = append(b.TransactionsUpdates[key], update)
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

func (b *Buffered) SetAuditEnabled(enabled bool) {
	b.pendingAuditConfigUpdate = &auditConfigUpdate{
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

func (b *Buffered) GetSinkConfig(name string) (*commonpb.SinkConfig, error) {
	cfg, err := b.SinkConfigs.Get(domain.SinkConfigKey{Name: name})
	if err != nil {
		return nil, nil
	}
	return cfg, nil
}

func (b *Buffered) AddSinkConfig(config *commonpb.SinkConfig) {
	b.SinkConfigs.Put(domain.SinkConfigKey{Name: config.Name}, config)
	b.sinkConfigChanged = true
}

func (b *Buffered) RemoveSinkConfig(name string) {
	b.SinkConfigs.Delete(domain.SinkConfigKey{Name: name})
	b.sinkConfigChanged = true
}

func (b *Buffered) HasPendingSinkChanges() bool {
	return b.sinkConfigChanged
}

func (b *Buffered) GetNextSequenceID() uint64 {
	return b.NextSequenceID
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

// coalesceVolumeSide returns Known if set, otherwise Diff.
// Used to normalize a VolumePair for Pebble storage where
// the Known/Diff distinction is irrelevant.
func coalesceVolumeSide(known, diff *commonpb.Uint256) *commonpb.Uint256 {
	if known != nil {
		return known
	}
	return diff
}

// addVolumeSideDelta extracts the net delta for one side (input or output) of a VolumePair update.
// Uses the provided tmp and scratch uint256.Ints for intermediate computations to avoid heap allocations.
func addVolumeSideDelta(acc *uint256.Int, tmp *uint256.Int, scratch *uint256.Int, newKnown, newDiff *commonpb.Uint256, oldKnown, oldDiff *commonpb.Uint256) {
	if newKnown != nil {
		newKnown.IntoUint256(tmp)
		if oldKnown != nil {
			oldKnown.IntoUint256(scratch)
			tmp.Sub(tmp, scratch)
			acc.Add(acc, tmp)
			return
		}
		acc.Add(acc, tmp)
		return
	}
	if newDiff != nil {
		newDiff.IntoUint256(tmp)
		if oldDiff != nil {
			oldDiff.IntoUint256(scratch)
			tmp.Sub(tmp, scratch)
			acc.Add(acc, tmp)
			return
		}
		acc.Add(acc, tmp)
	}
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
		var oldInputKnown, oldInputDiff, oldOutputKnown, oldOutputDiff *commonpb.Uint256
		if update.Old.IsDefined() {
			old := update.Old.Value()
			if old != nil {
				oldInputKnown = old.InputKnown
				oldInputDiff = old.InputDiff
				oldOutputKnown = old.OutputKnown
				oldOutputDiff = old.OutputDiff
			}
		}
		addVolumeSideDelta(&inputSum, &tmp, &scratch, update.New.InputKnown, update.New.InputDiff, oldInputKnown, oldInputDiff)
		addVolumeSideDelta(&outputSum, &tmp, &scratch, update.New.OutputKnown, update.New.OutputDiff, oldOutputKnown, oldOutputDiff)
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
	if b.currentOpenPeriod != nil {
		return b.currentOpenPeriod, true
	}
	return nil, false
}

func (b *Buffered) GetClosingPeriod() (*commonpb.Period, bool) {
	if b.closingPeriod != nil {
		return b.closingPeriod, true
	}
	return nil, false
}

func (b *Buffered) SetCurrentOpenPeriod(period *commonpb.Period) {
	b.currentOpenPeriod = period
	b.changedPeriods = append(b.changedPeriods, period)
}

func (b *Buffered) SetClosingPeriod(period *commonpb.Period) {
	b.closingPeriod = period
	b.changedPeriods = append(b.changedPeriods, period)
}

// ClearClosingPeriod persists the closing period's final state and removes it from in-memory tracking.
func (b *Buffered) ClearClosingPeriod() {
	if b.closingPeriod != nil {
		b.changedPeriods = append(b.changedPeriods, b.closingPeriod)
	}
	b.closingPeriod = nil
}

func (b *Buffered) GetNextPeriodID() uint64 {
	return b.NextPeriodID
}

func (b *Buffered) IncrementNextPeriodID() uint64 {
	id := b.NextPeriodID
	b.NextPeriodID++
	return id
}

// GetPeriodByID looks up a period by ID from in-memory state only.
// It checks changedPeriods first (most recent modifications), then the allPeriods map.
func (b *Buffered) GetPeriodByID(periodID uint64) (*commonpb.Period, bool) {
	// Check changedPeriods (most recently changed first)
	for i := len(b.changedPeriods) - 1; i >= 0; i-- {
		if b.changedPeriods[i].Id == periodID {
			return b.changedPeriods[i], true
		}
	}

	p, ok := b.allPeriods[periodID]
	return p, ok
}

// UpdatePeriod records a period modification to be persisted in Merge().
func (b *Buffered) UpdatePeriod(period *commonpb.Period) {
	b.changedPeriods = append(b.changedPeriods, period)
}

// SetPurgeRange records a sequence range to be purged (logs + audit entries) during Merge().
// periodID identifies the archived period to remove from the in-memory map.
// Can be called multiple times to purge multiple periods in the same batch.
func (b *Buffered) SetPurgeRange(periodID, startSequence, closeSequence uint64) {
	b.purgeRanges = append(b.purgeRanges, purgeRange{
		periodID:      periodID,
		startSequence: startSequence,
		closeSequence: closeSequence,
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
func (b *Buffered) executePurge(batch *dal.Batch, pr *purgeRange) error {
	// Sequence-keyed prefixes (logs, audit): efficient range delete.
	for _, prefix := range dal.ColdSequencePrefixes {
		start := dal.NewKeyBuilder().PutByte(prefix).PutUInt64(pr.startSequence).Build()
		end := dal.NewKeyBuilder().PutByte(prefix).PutUInt64(pr.closeSequence + 1).Build()
		if err := batch.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("purging prefix 0x%02x [%d, %d]: %w", prefix, pr.startSequence, pr.closeSequence, err)
		}
	}

	// Transaction updates: iterate and point-delete by byLog range.
	if err := PurgeTransactionUpdates(batch, pr.startSequence, pr.closeSequence); err != nil {
		return fmt.Errorf("purging transaction updates [%d, %d]: %w", pr.startSequence, pr.closeSequence, err)
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

// Ensure Buffered implements InMemoryStore
var _ processing.InMemoryStore = (*Buffered)(nil)
