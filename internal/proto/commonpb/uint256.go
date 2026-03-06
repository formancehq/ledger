package commonpb

import (
	"math/big"

	"github.com/holiman/uint256"
)

// IntoUint256 copies the 4 limbs from the proto message into dst.
// Zero-allocation: direct uint64 copies from proto fields to uint256.Int array.
func (u *Uint256) IntoUint256(dst *uint256.Int) {
	if u == nil {
		dst.Clear()

		return
	}

	dst[0] = u.GetV0()
	dst[1] = u.GetV1()
	dst[2] = u.GetV2()
	dst[3] = u.GetV3()
}

// SetFromUint256 copies the 4 limbs from v into the proto message fields.
// Zero-allocation: direct uint64 copies from uint256.Int array to proto fields.
func (u *Uint256) SetFromUint256(v *uint256.Int) {
	u.V0 = v[0]
	u.V1 = v[1]
	u.V2 = v[2]
	u.V3 = v[3]
}

// NewUint256 creates a new Uint256 proto message from a uint256.Int.
func NewUint256(v *uint256.Int) *Uint256 {
	return &Uint256{
		V0: v[0],
		V1: v[1],
		V2: v[2],
		V3: v[3],
	}
}

// NewUint256FromUint64 creates a new Uint256 from a single uint64 value.
// Convenience function for tests and simple cases.
func NewUint256FromUint64(v uint64) *Uint256 {
	return &Uint256{V0: v}
}

// ToBigInt converts the Uint256 to a *big.Int.
// Allocates: use only on display/non-hot-paths.
func (u *Uint256) ToBigInt() *big.Int {
	if u == nil || u.IsZero() {
		return new(big.Int)
	}

	var v uint256.Int

	v[0] = u.GetV0()
	v[1] = u.GetV1()
	v[2] = u.GetV2()
	v[3] = u.GetV3()

	return v.ToBig()
}

// IsZero returns true if all 4 limbs are zero.
func (u *Uint256) IsZero() bool {
	if u == nil {
		return true
	}

	return u.GetV0() == 0 && u.GetV1() == 0 && u.GetV2() == 0 && u.GetV3() == 0
}

// Dec returns the decimal string representation of the value.
func (u *Uint256) Dec() string {
	if u == nil {
		return "0"
	}

	var v uint256.Int

	v[0] = u.GetV0()
	v[1] = u.GetV1()
	v[2] = u.GetV2()
	v[3] = u.GetV3()

	return v.Dec()
}

// MarshalJSON encodes the Uint256 as a decimal string (no quotes) for JSON.
func (u *Uint256) MarshalJSON() ([]byte, error) {
	return []byte(u.Dec()), nil
}

// UnmarshalJSON decodes a decimal string (no quotes) into the Uint256.
func (u *Uint256) UnmarshalJSON(data []byte) error {
	var v uint256.Int

	err := v.SetFromDecimal(string(data))
	if err != nil {
		return err
	}

	u.V0 = v[0]
	u.V1 = v[1]
	u.V2 = v[2]
	u.V3 = v[3]

	return nil
}
