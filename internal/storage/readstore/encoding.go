package readstore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Type tags for sortable value encoding in Pebble keys.
// These ensure values of different types sort into distinct regions.
const (
	TypeTagString byte = 'S'
	TypeTagInt    byte = 'I'
	TypeTagUint   byte = 'U'
	TypeTagBool   byte = 'B'
	TypeTagNull   byte = 'N'
)

// EncodeString encodes a string value for use in Pebble keys.
// The value is followed by a null terminator to allow prefix-free parsing.
func EncodeString(dst []byte, value string) []byte {
	dst = append(dst, TypeTagString)
	dst = append(dst, value...)
	dst = append(dst, 0x00)

	return dst
}

// EncodeInt64 encodes a signed int64 for use in sortable Pebble keys.
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

// EncodeUint64 encodes an unsigned uint64 for use in sortable Pebble keys.
// Big-endian encoding naturally produces the correct sort order.
func EncodeUint64(dst []byte, v uint64) []byte {
	dst = append(dst, TypeTagUint)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	dst = append(dst, buf[:]...)

	return dst
}

// EncodeBool encodes a boolean value for use in Pebble keys.
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

// DecodeValue decodes an encoded metadata value starting at data[0].
// Returns the decoded MetadataValue and the number of bytes consumed.
func DecodeValue(data []byte) (*commonpb.MetadataValue, int, error) {
	if len(data) == 0 {
		return nil, 0, errors.New("empty data")
	}

	switch data[0] {
	case TypeTagString:
		s, n, err := DecodeString(data[1:])
		if err != nil {
			return nil, 0, err
		}

		return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: s}}, 1 + n, nil

	case TypeTagInt:
		v, n, err := DecodeInt64(data[1:])
		if err != nil {
			return nil, 0, err
		}

		return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_IntValue{IntValue: v}}, 1 + n, nil

	case TypeTagUint:
		v, n, err := DecodeUint64(data[1:])
		if err != nil {
			return nil, 0, err
		}

		return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_UintValue{UintValue: v}}, 1 + n, nil

	case TypeTagBool:
		v, n, err := DecodeBool(data[1:])
		if err != nil {
			return nil, 0, err
		}

		return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_BoolValue{BoolValue: v}}, 1 + n, nil

	case TypeTagNull:
		s, n, err := DecodeNull(data[1:])
		if err != nil {
			return nil, 0, err
		}

		return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{Original: s}}}, 1 + n, nil

	default:
		return nil, 0, fmt.Errorf("unknown type tag: 0x%02x", data[0])
	}
}

// DecodeString decodes a null-terminated string.
// data starts AFTER the type tag.
func DecodeString(data []byte) (string, int, error) {
	idx := bytes.IndexByte(data, 0x00)
	if idx < 0 {
		return "", 0, errors.New("string value missing null terminator")
	}

	return string(data[:idx]), idx + 1, nil
}

// DecodeInt64 decodes a sign-XOR'd big-endian int64.
// data starts AFTER the type tag.
func DecodeInt64(data []byte) (int64, int, error) {
	if len(data) < 8 {
		return 0, 0, fmt.Errorf("int64 value too short: need 8, got %d", len(data))
	}

	raw := binary.BigEndian.Uint64(data[:8])

	return int64(raw ^ 0x8000000000000000), 8, nil
}

// DecodeUint64 decodes a big-endian uint64.
// data starts AFTER the type tag.
func DecodeUint64(data []byte) (uint64, int, error) {
	if len(data) < 8 {
		return 0, 0, fmt.Errorf("uint64 value too short: need 8, got %d", len(data))
	}

	return binary.BigEndian.Uint64(data[:8]), 8, nil
}

// DecodeBool decodes a single-byte boolean.
// data starts AFTER the type tag.
func DecodeBool(data []byte) (bool, int, error) {
	if len(data) < 1 {
		return false, 0, errors.New("bool value too short")
	}

	return data[0] == 0x01, 1, nil
}

// DecodeNull decodes a null-terminated original value.
// data starts AFTER the type tag.
func DecodeNull(data []byte) (string, int, error) {
	idx := bytes.IndexByte(data, 0x00)
	if idx < 0 {
		return "", 0, errors.New("null value missing null terminator")
	}

	return string(data[:idx]), idx + 1, nil
}

// EncodeTxID appends a transaction ID as 8-byte big-endian.
func EncodeTxID(dst []byte, txID uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], txID)

	return append(dst, buf[:]...)
}
