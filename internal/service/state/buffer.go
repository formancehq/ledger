package state

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"google.golang.org/protobuf/proto"
)

type Buffered struct {
	fsm                 *Machine
	attrs               *attributes.Attributes
	Date                *commonpb.Timestamp
	NextLedgerID        uint32
	NextSequenceID      uint64
	LastLogHash         []byte
	Ledgers             *attributes.DerivedKeyStore[data.LedgerKey, *commonpb.LedgerInfo]
	Boundaries          *attributes.DerivedKeyStore[data.LedgerKey, *raftcmdpb.LedgerBoundaries]
	Volumes             *attributes.DerivedKeyStore[data.VolumeKey, *raftcmdpb.VolumePair]
	AccountMetadata     *attributes.DerivedKeyStore[data.MetadataKey, *commonpb.MetadataValue]
	LedgerMetadata      *attributes.DerivedKeyStore[data.LedgerMetadataKey, *commonpb.MetadataValue]
	Reversions          *attributes.DerivedKeyStore[data.TransactionKey, bool]
	IdempotencyKeys     *attributes.DerivedKeyStore[data.IdempotencyKey, *commonpb.IdempotencyKeyValue]
	References          *attributes.DerivedKeyStore[data.TransactionReferenceKey, *commonpb.TransactionReferenceValue]
	TransactionsUpdates map[data.TransactionKey][]*commonpb.TransactionUpdate
	PendingLogs         []*commonpb.Log
}

func (b *Buffered) Merge(index uint64, batch *data.Batch) error {
	// Process Ledger updates
	ledgerUpdates, _, err := b.Ledgers.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge ledgers: %w", err)
	}
	for _, update := range ledgerUpdates {
		if err := b.attrs.Ledger.SetBase(batch, index, update.CanonicalKey, update.New); err != nil {
			return fmt.Errorf("failed setting ledger base: %w", err)
		}
		if err := batch.SaveLedger(update.New); err != nil {
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

	ledgerMetadataUpdates, _, err := b.LedgerMetadata.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge ledger metadata: %w", err)
	}
	for _, update := range ledgerMetadataUpdates {
		err := b.attrs.LedgerMetadata.AddDiff(batch, index, update.CanonicalKey, update.New)
		if err != nil {
			return fmt.Errorf("failed adding diff for ledger metadata: %v", err)
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
			err := batch.StoreTransactionUpdate(key, update)
			if err != nil {
				return fmt.Errorf("failed storing transaction update for ledger %d: %w", key.LedgerID, err)
			}
		}
	}

	err = batch.AppendLogs(b.PendingLogs...)
	if err != nil {
		return fmt.Errorf("failed appending pending logs: %w", err)
	}

	b.PendingLogs = nil
	b.fsm.nextLedgerID = b.NextLedgerID
	b.fsm.nextSequenceID = b.NextSequenceID
	b.fsm.lastLogHash = b.LastLogHash

	return nil
}

func NewBuffer(at *commonpb.Timestamp, fsm *Machine) *Buffered {
	return &Buffered{
		fsm:                 fsm,
		attrs:               fsm.Attrs,
		Date:                at,
		Ledgers:             attributes.NewDerivedKeyStore(fsm.Ledgers, proto.CloneOf),
		Boundaries:          attributes.NewDerivedKeyStore(fsm.Boundaries, proto.CloneOf),
		NextLedgerID:        fsm.nextLedgerID,
		NextSequenceID:      fsm.nextSequenceID,
		LastLogHash:         fsm.lastLogHash,
		Volumes:             attributes.NewDerivedKeyStore(fsm.Volumes, proto.CloneOf),
		AccountMetadata:     attributes.NewDerivedKeyStore(fsm.AccountMetadata, proto.CloneOf),
		LedgerMetadata:      attributes.NewDerivedKeyStore(fsm.LedgerMetadata, proto.CloneOf),
		Reversions:          attributes.NewDerivedKeyStore(fsm.Reversions, nil), // bool is a value type, no clone needed
		IdempotencyKeys:     attributes.NewDerivedKeyStore(fsm.IdempotencyKeys, proto.CloneOf),
		References:          attributes.NewDerivedKeyStore(fsm.References, proto.CloneOf),
		TransactionsUpdates: make(map[data.TransactionKey][]*commonpb.TransactionUpdate),
	}
}

// Store interface implementation for Buffered

func (b *Buffered) GetLedger(name string) (*commonpb.LedgerInfo, bool) {
	info, err := b.Ledgers.Get(data.LedgerKey{Name: name})
	if err != nil || info == nil {
		return nil, false
	}
	return info, true
}

func (b *Buffered) PutLedger(name string, info *commonpb.LedgerInfo) {
	b.Ledgers.Put(data.LedgerKey{Name: name}, info)
}

func (b *Buffered) GetBoundaries(ledger string) (*raftcmdpb.LedgerBoundaries, bool) {
	boundaries, err := b.Boundaries.Get(data.LedgerKey{Name: ledger})
	if err != nil || boundaries == nil {
		return nil, false
	}
	return boundaries, true
}

func (b *Buffered) PutBoundaries(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
	b.Boundaries.Put(data.LedgerKey{Name: ledger}, boundaries)
}

func (b *Buffered) GetVolume(key data.VolumeKey) (*raftcmdpb.VolumePair, error) {
	return b.Volumes.Get(key)
}

func (b *Buffered) PutVolume(key data.VolumeKey, value *raftcmdpb.VolumePair) {
	b.Volumes.Put(key, value)
}

func (b *Buffered) GetAccountMetadata(key data.MetadataKey) (*commonpb.MetadataValue, error) {
	return b.AccountMetadata.Get(key)
}

func (b *Buffered) PutAccountMetadata(key data.MetadataKey, value *commonpb.MetadataValue) {
	b.AccountMetadata.Put(key, value)
}

func (b *Buffered) DeleteAccountMetadata(key data.MetadataKey) {
	b.AccountMetadata.Delete(key)
}

func (b *Buffered) PutLedgerMetadata(key data.LedgerMetadataKey, value *commonpb.MetadataValue) {
	b.LedgerMetadata.Put(key, value)
}

func (b *Buffered) GetReverted(key data.TransactionKey) (bool, error) {
	return b.Reversions.Get(key)
}

func (b *Buffered) PutReverted(key data.TransactionKey, reverted bool) {
	b.Reversions.Put(key, reverted)
}

func (b *Buffered) GetIdempotencyKey(key data.IdempotencyKey) (*commonpb.IdempotencyKeyValue, error) {
	return b.IdempotencyKeys.Get(key)
}

func (b *Buffered) PutIdempotencyKey(key data.IdempotencyKey, value *commonpb.IdempotencyKeyValue) {
	b.IdempotencyKeys.Put(key, value)
}

func (b *Buffered) GetTransactionReference(key data.TransactionReferenceKey) (*commonpb.TransactionReferenceValue, error) {
	return b.References.Get(key)
}

func (b *Buffered) PutTransactionReference(key data.TransactionReferenceKey, value *commonpb.TransactionReferenceValue) {
	b.References.Put(key, value)
}

func (b *Buffered) AddTransactionUpdate(key data.TransactionKey, update *commonpb.TransactionUpdate) {
	b.TransactionsUpdates[key] = append(b.TransactionsUpdates[key], update)
}

func (b *Buffered) GetNextLedgerID() uint32 {
	return b.NextLedgerID
}

func (b *Buffered) IncrementNextLedgerID() uint32 {
	id := b.NextLedgerID
	b.NextLedgerID++
	return id
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
func coalesceVolumeSide(known, diff *commonpb.BigInt) *commonpb.BigInt {
	if known != nil {
		return known
	}
	return diff
}

// addVolumeSideDelta extracts the net delta for one side (input or output) of a VolumePair update.
// Uses the provided tmp and scratch big.Ints for intermediate computations to avoid heap allocations.
func addVolumeSideDelta(acc *big.Int, tmp *big.Int, scratch *big.Int, newKnown, newDiff *commonpb.BigInt, oldKnown, oldDiff *commonpb.BigInt) {
	if newKnown != nil {
		newKnown.ValueInto(tmp)
		if oldKnown != nil {
			oldKnown.ValueInto(scratch)
			tmp.Sub(tmp, scratch)
			acc.Add(acc, tmp)
			return
		}
		acc.Add(acc, tmp)
		return
	}
	if newDiff != nil {
		newDiff.ValueInto(tmp)
		if oldDiff != nil {
			oldDiff.ValueInto(scratch)
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
	volumeUpdates []attributes.Update[data.VolumeKey, *raftcmdpb.VolumePair],
) error {
	var (
		inputSum  big.Int
		outputSum big.Int
		tmp       big.Int
		scratch   big.Int
	)

	for _, update := range volumeUpdates {
		var oldInputKnown, oldInputDiff, oldOutputKnown, oldOutputDiff *commonpb.BigInt
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

	if inputSum.Cmp(&outputSum) != 0 {
		return &ErrDoubleEntryInvariantViolated{
			InputSum:  &inputSum,
			OutputSum: &outputSum,
		}
	}

	return nil
}

// Ensure Buffered implements Store
var _ processing.Store = (*Buffered)(nil)
