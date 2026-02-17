package attributes

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/zeebo/xxh3"
)

// U128 is a comparable 128-bit identifier usable as a map key.
// Layout: bytes 0-7 = Hi (big-endian), bytes 8-15 = Lo (big-endian)
type U128 [16]byte

// NewU128 creates a U128 from high and low 64-bit values.
func NewU128(hi, lo uint64) U128 {
	var u U128
	binary.BigEndian.PutUint64(u[0:8], hi)
	binary.BigEndian.PutUint64(u[8:16], lo)
	return u
}

// U128FromBytes creates a U128 from a byte slice.
// If b is shorter than 16 bytes, the result is zero-padded.
// If b is longer than 16 bytes, only the first 16 bytes are used.
func U128FromBytes(b []byte) U128 {
	var u U128
	copy(u[:], b)
	return u
}

// Hi returns the high 64 bits.
func (u U128) Hi() uint64 {
	return binary.BigEndian.Uint64(u[0:8])
}

// Lo returns the low 64 bits.
func (u U128) Lo() uint64 {
	return binary.BigEndian.Uint64(u[8:16])
}

// Bytes returns a copy of the 16-byte representation.
func (u U128) Bytes() []byte {
	b := make([]byte, 16)
	copy(b, u[:])
	return b
}

// Hex returns the hexadecimal string representation.
func (u U128) Hex() string {
	return hex.EncodeToString(u[:])
}

// Equal returns true if u and v are equal.
func (u U128) Equal(v U128) bool { return u == v }

// HashU128 computes a deterministic 128-bit ID from canonical bytes using XXH3-128.
func HashU128(seeds Seeds, canonical []byte) U128 {
	u := xxh3.Hash128Seed(canonical, seeds.IDSeed)
	return NewU128(u.Hi, u.Lo)
}

// Tag64 computes a secondary fingerprint from canonical bytes using XXH3-64.
// It is used to detect rare collisions locally without storing original keys.
func Tag64(seeds Seeds, canonical []byte) uint64 {
	return xxh3.HashSeed(canonical, seeds.TagSeed)
}

// MakeKey returns (u128, tag64) from canonical bytes.
func MakeKey(seeds Seeds, canonical []byte) (U128, uint64) {
	return HashU128(seeds, canonical), Tag64(seeds, canonical)
}
