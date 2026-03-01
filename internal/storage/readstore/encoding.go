package readstore

import "encoding/binary"

// Type tags for sortable value encoding in bbolt keys.
// These ensure values of different types sort into distinct regions.
const (
	TypeTagString byte = 'S'
	TypeTagInt    byte = 'I'
	TypeTagUint   byte = 'U'
	TypeTagBool   byte = 'B'
	TypeTagNull   byte = 'N'
)

// EncodeString encodes a string value for use in bbolt keys.
// The value is followed by a null terminator to allow prefix-free parsing.
func EncodeString(dst []byte, value string) []byte {
	dst = append(dst, TypeTagString)
	dst = append(dst, value...)
	dst = append(dst, 0x00)
	return dst
}

// EncodeInt64 encodes a signed int64 for use in sortable bbolt keys.
// The sign bit is XOR'd so that negative values sort before positive
// values in unsigned byte order (big-endian).
func EncodeInt64(dst []byte, v int64) []byte {
	dst = append(dst, TypeTagInt)
	var buf [8]byte
	// XOR with 0x8000000000000000 flips the sign bit:
	//   -9223372036854775808 → 0x0000000000000000
	//   0                    → 0x8000000000000000
	//   9223372036854775807  → 0xFFFFFFFFFFFFFFFF
	binary.BigEndian.PutUint64(buf[:], uint64(v)^0x8000000000000000)
	dst = append(dst, buf[:]...)
	return dst
}

// DecodeInt64 decodes a signed int64 from a sortable encoding.
// The input must be exactly 8 bytes (without the type tag).
func DecodeInt64(b []byte) int64 {
	return int64(binary.BigEndian.Uint64(b) ^ 0x8000000000000000)
}

// EncodeUint64 encodes an unsigned uint64 for use in sortable bbolt keys.
// Big-endian encoding naturally produces the correct sort order.
func EncodeUint64(dst []byte, v uint64) []byte {
	dst = append(dst, TypeTagUint)
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	dst = append(dst, buf[:]...)
	return dst
}

// DecodeUint64 decodes an unsigned uint64 from a sortable encoding.
// The input must be exactly 8 bytes (without the type tag).
func DecodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// EncodeBool encodes a boolean value for use in bbolt keys.
func EncodeBool(dst []byte, v bool) []byte {
	dst = append(dst, TypeTagBool)
	if v {
		dst = append(dst, 0x01)
	} else {
		dst = append(dst, 0x00)
	}
	return dst
}

// EncodeNull encodes a null marker with the original raw string value.
// This allows ExistsCondition to scan null entries.
func EncodeNull(dst []byte, rawValue string) []byte {
	dst = append(dst, TypeTagNull)
	dst = append(dst, rawValue...)
	dst = append(dst, 0x00)
	return dst
}

// EncodeTxID appends a transaction ID as 8-byte big-endian.
func EncodeTxID(dst []byte, txID uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], txID)
	return append(dst, buf[:]...)
}

// DecodeTxID decodes a transaction ID from 8 big-endian bytes.
func DecodeTxID(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
