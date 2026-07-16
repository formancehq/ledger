package admission

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// foldCallerAccountMetadata folds a CreateTransaction's caller-supplied account
// metadata into the batch overlay. The FSM merges this metadata with precedence
// over any script-produced set_account_meta for the same key
// (processCreateTransaction), so callers must invoke this AFTER folding the
// script's own metadata writes.
func foldCallerAccountMetadata(effects *batchEffects, ledgerName string, ct *raftcmdpb.CreateTransactionOrder) {
	for account, mm := range ct.GetAccountMetadata() {
		for k, v := range mm.GetValues() {
			effects.setMetadata(domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
				Key:        k,
			}, commonpb.MetadataValueToString(v))
		}
	}
}

// batchEffects accumulates the state changes of the orders already resolved in
// the current atomic batch, so a later order's Numscript resolution reads the
// same state the FSM will see (EN-1406 P1-1). The FSM applies batch orders
// sequentially against a single mutated WriteSet; admission mirrors that by
// layering these effects over the pre-batch Pebble snapshot. Every preceding
// *mutating* order contributes: CreateTransaction (script + postings + caller
// AccountMetadata), RevertTransaction (reversed-posting balance deltas), and
// account-targeted AddMetadata / DeleteMetadata.
//
// balanceDeltas holds (input−output) deltas per (ledger, account, asset) — the
// quantity balance() returns. metadataWrites holds the latest write per
// account-metadata key (a later order sees an earlier order's write), where a
// write is either a set (value, present) or a delete tombstone (deleted) so a
// following meta() resolves absent exactly as the FSM would after the delete.
//
// createdReferences and revertedTxs track the intra-batch effects that drive the
// FSM's skip decisions (matchOrderSkip), so admission predicts them with the
// same visibility the FSM has when it reaches each order: a reference a
// preceding order in the same batch registered, and a transaction a preceding
// revert marked reverted. Only SUCCESSFUL predecessors record here — a
// predecessor that would itself be skipped contributes nothing (see
// resolveScriptsAndEnrichNeeds), exactly as the FSM's dropped overlay leaves no
// reference/reverted marker behind.
type batchEffects struct {
	balanceDeltas     map[domain.VolumeKey]*big.Int
	metadataWrites    map[domain.MetadataKey]metadataWrite
	createdReferences map[domain.TransactionReferenceKey]struct{}
	revertedTxs       map[domain.TransactionKey]struct{}
}

// metadataWrite records the outcome of a preceding order's account-metadata
// mutation within the batch: either a set to value, or a deletion (deleted).
type metadataWrite struct {
	value   string
	deleted bool
}

func newBatchEffects() *batchEffects {
	return &batchEffects{
		balanceDeltas:     make(map[domain.VolumeKey]*big.Int),
		metadataWrites:    make(map[domain.MetadataKey]metadataWrite),
		createdReferences: make(map[domain.TransactionReferenceKey]struct{}),
		revertedTxs:       make(map[domain.TransactionKey]struct{}),
	}
}

// recordReference notes that a preceding successful CreateTransaction registered
// a transaction reference, so a later same-batch create carrying the same
// reference is predicted to hit TRANSACTION_REFERENCE_CONFLICT — matching the
// FSM, which sees the reference in its mutated WriteSet.
func (b *batchEffects) recordReference(key domain.TransactionReferenceKey) {
	b.createdReferences[key] = struct{}{}
}

// hasReference reports whether a preceding same-batch order registered the
// reference.
func (b *batchEffects) hasReference(key domain.TransactionReferenceKey) bool {
	_, ok := b.createdReferences[key]

	return ok
}

// recordReverted notes that a preceding successful RevertTransaction marked a
// transaction reverted, so a later same-batch revert of the same tx is predicted
// to hit TRANSACTION_ALREADY_REVERTED — matching the FSM's reverted bitset once
// the earlier revert applies.
func (b *batchEffects) recordReverted(key domain.TransactionKey) {
	b.revertedTxs[key] = struct{}{}
}

// hasReverted reports whether a preceding same-batch revert already reverted the
// transaction.
func (b *batchEffects) hasReverted(key domain.TransactionKey) bool {
	_, ok := b.revertedTxs[key]

	return ok
}

// setMetadata records that a preceding order wrote value to an account-metadata
// key (last write within the batch wins).
func (b *batchEffects) setMetadata(key domain.MetadataKey, value string) {
	b.metadataWrites[key] = metadataWrite{value: value}
}

// deleteMetadata records that a preceding order deleted an account-metadata key,
// so a later meta() resolves absent even if the pre-batch snapshot holds a value.
func (b *batchEffects) deleteMetadata(key domain.MetadataKey) {
	b.metadataWrites[key] = metadataWrite{deleted: true}
}

// addBalanceDelta accumulates a (input−output) delta for a volume key.
func (b *batchEffects) addBalanceDelta(key domain.VolumeKey, delta *big.Int) {
	if delta == nil || delta.Sign() == 0 {
		return
	}

	if existing, ok := b.balanceDeltas[key]; ok {
		existing.Add(existing, delta)

		return
	}

	b.balanceDeltas[key] = new(big.Int).Set(delta)
}

// mergeDiscovery folds a resolved order's effects into the batch accumulator.
func (b *batchEffects) mergeDiscovery(deltas map[domain.VolumeKey]*big.Int, metaWrites map[domain.MetadataKey]string) {
	for key, delta := range deltas {
		b.addBalanceDelta(key, delta)
	}

	// A later write for the same key wins (last-writer within the batch).
	for key, value := range metaWrites {
		b.setMetadata(key, value)
	}
}

// admissionValueSource reads balances and account metadata at admission time
// for Numscript dependency resolution. Reads go through the shared Pebble
// snapshot (a.store, a *dal.PebbleGetter) using the same attribute codecs the
// FSM cache is built from — this is the admission/preload read path, NOT the
// FSM hot path (invariant #3 concerns only the apply path) — with the current
// batch's accumulated effects layered on top so an order sees its predecessors'
// writes (EN-1406 P1-1).
//
// Absent volumes resolve to a zero balance (a fresh account); absent metadata
// resolves to "not present". These reads are recorded by the resolver's
// RecordingStore and hashed into the order's inputs_resolution_hash so the FSM
// can detect any value that changed before apply.
type admissionValueSource struct {
	admission  *Admission
	ledgerName string
	effects    *batchEffects
}

func (s *admissionValueSource) Balance(account, asset string) (*big.Int, error) {
	// #1560 (EN-1406) rejects colored/scoped balances upstream, so dependency
	// resolution reads only the uncolored bucket ("").
	key := domain.NewVolumeKey(s.ledgerName, account, asset, "")

	vol, err := s.admission.attrs.Volume.Get(s.admission.store, key.Bytes())
	if err != nil {
		return nil, err
	}

	balance := new(big.Int)
	if vol != nil && vol.GetInput() != nil && vol.GetOutput() != nil {
		var inputVal, outputVal uint256.Int
		vol.GetInput().IntoUint256(&inputVal)
		vol.GetOutput().IntoUint256(&outputVal)
		balance.Sub(inputVal.ToBig(), outputVal.ToBig())
	}
	// vol == nil (or partially materialised) is a fresh account with a zero
	// balance base.

	// Layer earlier same-batch orders' net effect on this volume.
	if s.effects != nil {
		if delta, ok := s.effects.balanceDeltas[key]; ok {
			balance.Add(balance, delta)
		}
	}

	return balance, nil
}

func (s *admissionValueSource) Metadata(account, key string) (string, bool, error) {
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: s.ledgerName, Account: account},
		Key:        key,
	}

	// An earlier same-batch order's metadata mutation wins over the pre-batch
	// snapshot (EN-1406 P1-1): a set returns its value as present, a delete
	// resolves the key as absent even if the snapshot still holds a value.
	if s.effects != nil {
		if write, ok := s.effects.metadataWrites[metaKey]; ok {
			if write.deleted {
				return "", false, nil
			}

			return write.value, true, nil
		}
	}

	value, err := s.admission.attrs.Metadata.Get(s.admission.store, metaKey.Bytes())
	if err != nil {
		return "", false, err
	}

	if value == nil {
		// Absent key: Attribute.Get returns nil for a Pebble miss.
		return "", false, nil
	}

	// Present key. Numscript sees the verbatim stored value — declared_type is an
	// index hint only and MUST NOT influence resolution. Presence is driven ONLY
	// by nil-ness: an empty string is a valid stored metadata value
	// (ValidateMetadataString accepts ""), and MetadataValueToString returns ""
	// for both a real StringValue("") and an untyped/nil value. Returning
	// present=false on str=="" would make a valid meta() read of an empty string
	// resolve as absent, diverging from the FSM-side scopeValueSource and
	// poisoning the resolution hash with the absent sentinel.
	return commonpb.MetadataValueToString(value), true, nil
}
