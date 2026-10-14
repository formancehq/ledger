package accounttype

import (
	"crypto/sha256"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// LedgerTypeDigest binds a lifecycle inventory to the account-type rules
// observed in the same snapshot. A full ledger value is deliberately hashed:
// unrelated ledger changes can cause a retry, but cannot hide a reclassification.
func LedgerTypeDigest(info *commonpb.LedgerInfo) ([]byte, error) {
	if info == nil {
		sum := sha256.Sum256(nil)

		return sum[:], nil
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(info)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(encoded)

	return sum[:], nil
}
