package commonpb

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
)

// These options and callbacks are immutable. Sharing them reuses json/v2's
// type dispatch cache; each encoder still carries its own selected mode.
var (
	monetaryNumbers = jsonv2.WithMarshalers(volumeAmountMarshalers(false))
	monetaryStrings = jsonv2.WithMarshalers(jsonv2.JoinMarshalers(postingAmountsAsStringMarshalers(), volumeAmountMarshalers(true)))
)

// MonetaryAmountsAsNumbers renders HTTP volume amounts and balances as exact
// decimal JSON number tokens. Legacy marshaling outside HTTP remains unchanged.
func MonetaryAmountsAsNumbers() jsonv2.Options {
	return monetaryNumbers
}

// MonetaryAmountsAsStrings renders HTTP posting amounts, volume amounts and
// balances as canonical decimal strings for clients without arbitrary precision.
func MonetaryAmountsAsStrings() jsonv2.Options {
	return monetaryStrings
}

func volumeAmountMarshalers(quoted bool) *jsonv2.Marshalers {
	writeDecimal := func(enc *jsontext.Encoder, decimal string) error {
		if quoted {
			return enc.WriteToken(jsontext.String(decimal))
		}

		return enc.WriteValue(jsontext.Value(decimal))
	}

	return jsonv2.JoinMarshalers(
		jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, value *BigUint) error {
			decimal, err := value.Dec()
			if err != nil {
				return err
			}

			return writeDecimal(enc, decimal)
		}),
		jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, value *SignedBigInt) error {
			decimal, err := value.Dec()
			if err != nil {
				return err
			}

			return writeDecimal(enc, decimal)
		}),
	)
}
