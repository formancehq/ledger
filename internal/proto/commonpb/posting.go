package commonpb

import (
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// MarshalJSON implements json.Marshaler for Posting. Color is always emitted
// (even when empty) so clients can distinguish the uncolored bucket from an
// older response shape that predates the dimension — same contract as
// VolumeEntry and accountVolumeJSON.
func (x *Posting) MarshalJSON() ([]byte, error) {
	return json.Marshal(&struct {
		Source      string   `json:"source"`
		Destination string   `json:"destination"`
		Amount      *Uint256 `json:"amount,omitempty"`
		Asset       string   `json:"asset"`
		Color       string   `json:"color"`
	}{
		Source:      x.GetSource(),
		Destination: x.GetDestination(),
		Amount:      x.GetAmount(),
		Asset:       x.GetAsset(),
		Color:       x.GetColor(),
	})
}

// NewPosting creates a new uncolored Posting. Use NewColoredPosting to set a
// non-empty color. Converts the *big.Int amount to *Uint256 via uint256.Int
// intermediary.
func NewPosting(source, destination, asset string, amount *big.Int) *Posting {
	return NewColoredPosting(source, destination, asset, "", amount)
}

// NewColoredPosting creates a new Posting with an explicit color. Color is
// the empty string for the uncolored bucket.
func NewColoredPosting(source, destination, asset, color string, amount *big.Int) *Posting {
	var u uint256.Int
	if overflow := u.SetFromBig(amount); overflow {
		panic("commonpb.NewColoredPosting: amount exceeds 256 bits")
	}

	return &Posting{
		Source:      source,
		Destination: destination,
		Amount:      NewUint256(&u),
		Asset:       asset,
		Color:       color,
	}
}
