package admission

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// admissionValueSource reads balances and account metadata at admission time
// for Numscript dependency resolution. Reads go through the shared Pebble
// snapshot (a.store, a *dal.PebbleGetter) using the same attribute codecs the
// FSM cache is built from — this is the admission/preload read path, NOT the
// FSM hot path (invariant #3 concerns only the apply path).
//
// Absent volumes resolve to a zero balance (a fresh account); absent metadata
// resolves to "not present". These reads are recorded by the resolver's
// RecordingStore and hashed into the order's inputs_resolution_hash so the FSM
// can detect any value that changed before apply.
type admissionValueSource struct {
	admission  *Admission
	ledgerName string
}

func (s *admissionValueSource) Balance(account, asset string) (*big.Int, error) {
	key := domain.NewVolumeKey(s.ledgerName, account, asset)

	vol, err := s.admission.attrs.Volume.Get(s.admission.store, key.Bytes())
	if err != nil {
		return nil, err
	}

	if vol == nil || vol.GetInput() == nil || vol.GetOutput() == nil {
		// Absent (or partially materialised) volume — a fresh account with a
		// zero balance.
		return new(big.Int), nil
	}

	var inputVal, outputVal uint256.Int
	vol.GetInput().IntoUint256(&inputVal)
	vol.GetOutput().IntoUint256(&outputVal)

	return new(big.Int).Sub(inputVal.ToBig(), outputVal.ToBig()), nil
}

func (s *admissionValueSource) Metadata(account, key string) (string, bool, error) {
	metaKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: s.ledgerName, Account: account},
		Key:        key,
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
