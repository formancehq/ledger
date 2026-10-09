package commonpb

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
)

// UnmarshalJSON accepts exact integer tokens and canonical unsigned decimal
// strings for posting amounts. This input contract is independent of the HTTP
// response header and does not broaden the decoder of unrelated Uint256 values.
func (x *Posting) UnmarshalJSON(data []byte) error {
	var aux struct {
		Source      string        `json:"source"`
		Destination string        `json:"destination"`
		Amount      json.RawValue `json:"amount"`
		Asset       string        `json:"asset"`
		Color       string        `json:"color"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	var amount *Uint256
	if len(aux.Amount) != 0 && !bytes.Equal(aux.Amount, []byte("null")) {
		decimal := string(aux.Amount)
		if aux.Amount[0] == '"' {
			if err := json.Unmarshal(aux.Amount, &decimal); err != nil {
				return fmt.Errorf("invalid posting amount: %w", err)
			}
		}
		if err := validateCanonicalDecimalString(decimal, false); err != nil {
			return fmt.Errorf("invalid posting amount: %w", err)
		}
		var value uint256.Int
		if err := value.SetFromDecimal(decimal); err != nil {
			return fmt.Errorf("invalid posting amount: %w", err)
		}
		amount = NewUint256(&value)
	}

	x.Source = aux.Source
	x.Destination = aux.Destination
	x.Amount = amount
	x.Asset = aux.Asset
	x.Color = aux.Color

	return nil
}

// MarshalJSON implements json.Marshaler for Posting. Color is always emitted
// (even when empty) so clients can distinguish the uncolored bucket from an
// older response shape that predates the dimension — same contract as
// VolumeEntry and accountVolumeJSON.
func (x *Posting) MarshalJSON() ([]byte, error) {
	return json.Marshal(x.jsonValue())
}

type postingJSON struct {
	Source      string `json:"source"`
	Destination string `json:"destination"`
	Amount      any    `json:"amount,omitempty"`
	Asset       string `json:"asset"`
	Color       string `json:"color"`
}

func (x *Posting) jsonValue() postingJSON {
	value := postingJSON{
		Source:      x.GetSource(),
		Destination: x.GetDestination(),
		Asset:       x.GetAsset(),
		Color:       x.GetColor(),
	}
	if x.GetAmount() != nil {
		value.Amount = x.GetAmount()
	}

	return value
}

// MarshalJSONTo preserves the public posting shape for option-aware callers.
func (x *Posting) MarshalJSONTo(enc *jsontext.Encoder) error {
	return json.MarshalEncode(enc, x.jsonValue())
}

// PostingAmountsAsStrings overrides only posting amounts for one encoding call.
// Uint256 values elsewhere and the protobuf message itself are unchanged.
func PostingAmountsAsStrings() jsonv2.Options {
	return jsonv2.WithMarshalers(postingAmountsAsStringMarshalers())
}

func postingAmountsAsStringMarshalers() *jsonv2.Marshalers {
	return jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, x *Posting) error {
		value := x.jsonValue()
		if x.GetAmount() != nil {
			value.Amount = x.GetAmount().Dec()
		}

		return json.MarshalEncode(enc, value)
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
