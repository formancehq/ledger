package ledgerpb

import "math/big"

func (b *BigInt) UnmarshalJSON(data []byte) error {
	v := big.NewInt(0)
	v.SetString(string(data), 10)
	b.Data = marshalBigIntBytes(v)
	return nil
}

func (b *BigInt) MarshalJSON() ([]byte, error) {
	return []byte(b.Value().String()), nil
}

func (b *BigInt) Value() *big.Int {
	return unmarshalBigIntBytes(b.Data)
}

func NewBigInt(v *big.Int) *BigInt {
	return &BigInt{Data: marshalBigIntBytes(v)}
}

// Neg returns a new BigInt with the negated value.
func (b *BigInt) Neg() *BigInt {
	if len(b.Data) <= 1 {
		// Zero or empty: -0 = 0
		return &BigInt{Data: []byte{0}}
	}
	// Copy data and flip sign byte (0 -> 1, 1 -> 0)
	data := make([]byte, len(b.Data))
	copy(data, b.Data)
	data[0] ^= 1
	return &BigInt{Data: data}
}

// marshalBigIntBytes encodes a big.Int to bytes with sign preservation.
// Format: first byte is sign (0 = positive/zero, 1 = negative), followed by absolute value bytes.
func marshalBigIntBytes(x *big.Int) []byte {
	if x == nil {
		return []byte{0} // convention: nil => 0
	}
	sign := byte(0)
	if x.Sign() < 0 {
		sign = 1
	}
	mag := new(big.Int).Abs(x).Bytes()
	out := make([]byte, 1+len(mag))
	out[0] = sign
	copy(out[1:], mag)
	return out
}

// unmarshalBigIntBytes decodes bytes to a big.Int with sign preservation.
func unmarshalBigIntBytes(b []byte) *big.Int {
	if len(b) == 0 {
		return new(big.Int)
	}
	sign := b[0]
	x := new(big.Int).SetBytes(b[1:])
	if sign == 1 && x.Sign() != 0 {
		x.Neg(x)
	}
	return x
}
