package commonpb

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

// ValueInto decodes the stored value into dst without allocating a new big.Int.
// It reuses dst's internal storage when possible. Returns dst for convenience.
func (b *BigInt) ValueInto(dst *big.Int) *big.Int {
	unmarshalBigIntBytesInto(b.Data, dst)
	return dst
}

// SetFromBigInt encodes v into the existing BigInt, reusing the Data buffer
// when it has sufficient capacity to avoid allocation.
func (b *BigInt) SetFromBigInt(v *big.Int) {
	b.Data = marshalBigIntBytesReuse(v, b.Data)
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
// Note: big.Int.Bytes() already returns the absolute value, so no Abs() call is needed.
func marshalBigIntBytes(x *big.Int) []byte {
	if x == nil {
		return []byte{0} // convention: nil => 0
	}
	sign := byte(0)
	if x.Sign() < 0 {
		sign = 1
	}
	mag := x.Bytes() // Bytes() returns absolute value as big-endian
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

// unmarshalBigIntBytesInto decodes bytes into an existing big.Int without allocating.
// Uses SetBytes which reuses dst's internal nat buffer when it has sufficient capacity.
func unmarshalBigIntBytesInto(b []byte, dst *big.Int) {
	if len(b) == 0 {
		dst.SetInt64(0)
		return
	}
	sign := b[0]
	dst.SetBytes(b[1:])
	if sign == 1 && dst.Sign() != 0 {
		dst.Neg(dst)
	}
}

// marshalBigIntBytesReuse encodes a big.Int to bytes, reusing buf when it has
// sufficient capacity. Uses FillBytes to write directly into the buffer,
// avoiding the intermediate allocation from Bytes().
func marshalBigIntBytesReuse(x *big.Int, buf []byte) []byte {
	if x == nil || x.Sign() == 0 {
		if cap(buf) >= 1 {
			buf = buf[:1]
			buf[0] = 0
			return buf
		}
		return []byte{0}
	}
	sign := byte(0)
	if x.Sign() < 0 {
		sign = 1
	}
	byteLen := (x.BitLen() + 7) / 8
	needed := 1 + byteLen
	if cap(buf) >= needed {
		buf = buf[:needed]
	} else {
		buf = make([]byte, needed)
	}
	buf[0] = sign
	x.FillBytes(buf[1:])
	return buf
}
