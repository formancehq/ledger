package ledgerpb

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
)

// These options and callbacks are immutable. Sharing them reuses json/v2's
// type dispatch cache; each encoder still carries its own selected mode.
var (
	bigIntegerNumbers = jsonv2.WithMarshalers(bigIntegerMarshalers(false))
	bigIntegerStrings = jsonv2.WithMarshalers(bigIntegerMarshalers(true))
)

// BigIntegersAsNumbers renders arbitrary-precision integers as exact
// decimal JSON number tokens. Legacy marshaling outside HTTP remains unchanged.
func BigIntegersAsNumbers() jsonv2.Options {
	return bigIntegerNumbers
}

// BigIntegersAsStrings renders arbitrary-precision integers as canonical decimal
// strings for clients without arbitrary precision.
func BigIntegersAsStrings() jsonv2.Options {
	return bigIntegerStrings
}

func bigIntegerMarshalers(quoted bool) *jsonv2.Marshalers {
	writeDecimal := func(enc *jsontext.Encoder, decimal string) error {
		if quoted {
			return enc.WriteToken(jsontext.String(decimal))
		}

		return enc.WriteValue(jsontext.Value(decimal))
	}

	return jsonv2.JoinMarshalers(
		jsonv2.MarshalToFunc(func(enc *jsontext.Encoder, value *Uint256) error {
			return writeDecimal(enc, value.Dec())
		}),
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
