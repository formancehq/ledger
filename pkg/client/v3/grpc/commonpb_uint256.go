package grpc

import (
	"encoding/json"
	"fmt"
	"math/big"
)

// NewUint256FromUint64 creates a new Uint256 from a single uint64 value.
// Convenience function for tests and simple cases.
func NewUint256FromUint64(v uint64) *Uint256 {
	return &Uint256{V0: v}
}

// ToBigInt converts the Uint256 to a *big.Int.
// Allocates: use only on display/non-hot-paths.
func (u *Uint256) ToBigInt() *big.Int {
	value := new(big.Int)
	if u == nil {
		return value
	}
	for _, limb := range [...]uint64{u.GetV3(), u.GetV2(), u.GetV1(), u.GetV0()} {
		value.Lsh(value, 64)
		value.Or(value, new(big.Int).SetUint64(limb))
	}
	return value
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
	return u.ToBigInt().String()
}

// MarshalJSON encodes the Uint256 as a decimal string (no quotes) for JSON.
func (u *Uint256) MarshalJSON() ([]byte, error) {
	return []byte(u.Dec()), nil
}

// UnmarshalJSON decodes a decimal string (no quotes) into the Uint256.
func (u *Uint256) UnmarshalJSON(data []byte) error {
	// A uint256 has at most 78 decimal digits. Bound input before big.Int
	// parsing so an untrusted JSON number cannot demand unbounded work.
	if len(data) > 78 || !json.Valid(data) {
		return fmt.Errorf("invalid Uint256 decimal %q", data)
	}
	value, ok := new(big.Int).SetString(string(data), 10)
	if !ok || value.Sign() < 0 || value.BitLen() > 256 {
		return fmt.Errorf("invalid Uint256 decimal %q", data)
	}
	u.V0 = value.Uint64()
	value.Rsh(value, 64)
	u.V1 = value.Uint64()
	value.Rsh(value, 64)
	u.V2 = value.Uint64()
	value.Rsh(value, 64)
	u.V3 = value.Uint64()
	return nil
}
