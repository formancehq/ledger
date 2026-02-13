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
	Input               *attributes.DerivedKeyStore[data.VolumeKey, *raftcmdpb.VolumeHolder]
	Output              *attributes.DerivedKeyStore[data.VolumeKey, *raftcmdpb.VolumeHolder]
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

	// Process Boundary updates
	boundaryUpdates, _, err := b.Boundaries.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge boundaries: %w", err)
	}
	for _, update := range boundaryUpdates {
		if err := b.attrs.Boundary.SetBase(batch, index, update.CanonicalKey, update.New); err != nil {
			return fmt.Errorf("failed setting boundary base: %w", err)
		}
		if err := b.attrs.Boundary.DeleteOldest(batch, index, update.CanonicalKey); err != nil {
			return fmt.Errorf("compacting old boundary base: %w", err)
		}
	}

	// Process Input updates
	inputUpdates, _, err := b.Input.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge input: %w", err)
	}
	for _, update := range inputUpdates {
		// If we know the absolute value (Known is set), use SetBase.
		// Otherwise, we only have a diff (DiffSinceBaseIndex), use AddDiff.
		if update.New.Known != nil {
			if err := b.attrs.Input.SetBase(batch, index, update.CanonicalKey, update.New.Known); err != nil {
				return fmt.Errorf("could not set input base: %w", err)
			}
		} else {
			if err := b.attrs.Input.AddDiff(batch, index, update.CanonicalKey, update.New.DiffSinceBaseIndex); err != nil {
				return fmt.Errorf("failed adding input diff: %w", err)
			}
		}
	}

	// Process Output updates
	outputUpdates, _, err := b.Output.Merge()
	if err != nil {
		return fmt.Errorf("failed to merge output: %w", err)
	}
	for _, update := range outputUpdates {
		// If we know the absolute value (Known is set), use SetBase.
		// Otherwise, we only have a diff (DiffSinceBaseIndex), use AddDiff.
		if update.New.Known != nil {
			if err := b.attrs.Output.SetBase(batch, index, update.CanonicalKey, update.New.Known); err != nil {
				return fmt.Errorf("could not set output base: %w", err)
			}
		} else {
			if err := b.attrs.Output.AddDiff(batch, index, update.CanonicalKey, update.New.DiffSinceBaseIndex); err != nil {
				return fmt.Errorf("failed adding output diff: %w", err)
			}
		}
	}

	// Track dirty volume keys for memory-only compaction.
	for _, update := range inputUpdates {
		b.fsm.dirtyVolumeKeys[0][string(update.CanonicalKey)] = struct{}{}
	}
	for _, update := range outputUpdates {
		b.fsm.dirtyVolumeKeys[0][string(update.CanonicalKey)] = struct{}{}
	}

	// Defensive check: double-entry invariant.
	// The sum of all input deltas must equal the sum of all output deltas,
	// because every posting moves the same amount from source (output) to destination (input).
	if err := checkDoubleEntryInvariant(inputUpdates, outputUpdates); err != nil {
		return err
	}

	// Index all accounts involved in volume updates.
	// Collect unique (ledgerID, account) pairs from both Input and Output updates.
	type accountKey struct {
		ledgerID uint32
		account  string
	}
	seenAccounts := make(map[accountKey]struct{})
	for _, update := range inputUpdates {
		seenAccounts[accountKey{update.Key.LedgerID, update.Key.Account}] = struct{}{}
	}
	for _, update := range outputUpdates {
		seenAccounts[accountKey{update.Key.LedgerID, update.Key.Account}] = struct{}{}
	}
	for ak := range seenAccounts {
		if err := batch.IndexAccount(ak.ledgerID, ak.account); err != nil {
			return fmt.Errorf("indexing account %s: %w", ak.account, err)
		}
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
			// todo: use transaction id as key for better locality
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
		Input:               attributes.NewDerivedKeyStore(fsm.Input, proto.CloneOf),
		Output:              attributes.NewDerivedKeyStore(fsm.Output, proto.CloneOf),
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

func (b *Buffered) GetInput(key data.VolumeKey) (*raftcmdpb.VolumeHolder, error) {
	return b.Input.Get(key)
}

func (b *Buffered) PutInput(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
	b.Input.Put(key, value)
}

func (b *Buffered) GetOutput(key data.VolumeKey) (*raftcmdpb.VolumeHolder, error) {
	return b.Output.Get(key)
}

func (b *Buffered) PutOutput(key data.VolumeKey, value *raftcmdpb.VolumeHolder) {
	b.Output.Put(key, value)
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

// volumeDelta computes the net delta for a single volume update by comparing New vs Old.
func volumeDelta(update attributes.Update[data.VolumeKey, *raftcmdpb.VolumeHolder]) *big.Int {
	vh := update.New
	if vh.Known != nil {
		result := vh.Known.Value()
		if update.Old.IsDefined() {
			old := update.Old.Value()
			if old != nil && old.Known != nil {
				return new(big.Int).Sub(result, old.Known.Value())
			}
		}
		return new(big.Int).Set(result)
	}
	if vh.DiffSinceBaseIndex != nil {
		result := vh.DiffSinceBaseIndex.Value()
		if update.Old.IsDefined() {
			old := update.Old.Value()
			if old != nil && old.DiffSinceBaseIndex != nil {
				return new(big.Int).Sub(result, old.DiffSinceBaseIndex.Value())
			}
		}
		return new(big.Int).Set(result)
	}
	return big.NewInt(0)
}

// checkDoubleEntryInvariant verifies that the sum of input deltas equals the sum of output deltas.
// This is a fundamental accounting invariant: every posting moves the same amount from a source
// account (output) to a destination account (input), so the totals must always balance.
func checkDoubleEntryInvariant(
	inputUpdates []attributes.Update[data.VolumeKey, *raftcmdpb.VolumeHolder],
	outputUpdates []attributes.Update[data.VolumeKey, *raftcmdpb.VolumeHolder],
) error {
	inputSum := big.NewInt(0)
	for _, update := range inputUpdates {
		inputSum.Add(inputSum, volumeDelta(update))
	}

	outputSum := big.NewInt(0)
	for _, update := range outputUpdates {
		outputSum.Add(outputSum, volumeDelta(update))
	}

	if inputSum.Cmp(outputSum) != 0 {
		return &ErrDoubleEntryInvariantViolated{
			InputSum:  inputSum,
			OutputSum: outputSum,
		}
	}

	return nil
}

// Ensure Buffered implements Store
var _ processing.Store = (*Buffered)(nil)
