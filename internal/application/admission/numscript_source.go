package admission

import (
	"maps"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// batchEffects accumulates the state changes of the orders already resolved in
// the current atomic batch, so a later order's Numscript resolution reads the
// same state the FSM will see (EN-1406 P1-1). The FSM applies batch orders
// sequentially against a single mutated WriteSet; admission mirrors that by
// layering these effects over the pre-batch Pebble snapshot.
//
// balanceDeltas holds (input−output) deltas per (ledger, account, asset) — the
// quantity balance() returns. metadataWrites holds the latest raw value written
// per account-metadata key (a later order sees an earlier order's write).
type batchEffects struct {
	balanceDeltas  map[domain.VolumeKey]*big.Int
	metadataWrites map[domain.MetadataKey]string
}

func newBatchEffects() *batchEffects {
	return &batchEffects{
		balanceDeltas:  make(map[domain.VolumeKey]*big.Int),
		metadataWrites: make(map[domain.MetadataKey]string),
	}
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
	maps.Copy(b.metadataWrites, metaWrites)
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
	key := domain.NewVolumeKey(s.ledgerName, account, asset)

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

	// An earlier same-batch order's set_account_meta wins over the pre-batch
	// snapshot (EN-1406 P1-1).
	if s.effects != nil {
		if value, ok := s.effects.metadataWrites[metaKey]; ok {
			return value, true, nil
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
