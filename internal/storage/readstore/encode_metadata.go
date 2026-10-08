package readstore

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// EncodeMetadataValue encodes a MetadataValue into a sortable byte sequence
// suitable for use as part of a Pebble key. The encoding includes a type tag
// and a value encoding that preserves sort order within each type.
func EncodeMetadataValue(dst []byte, v *ledgerpb.MetadataValue) []byte {
	if v == nil {
		return EncodeNull(dst, "")
	}

	switch t := v.GetType().(type) {
	case *ledgerpb.MetadataValue_StringValue:
		return EncodeString(dst, t.StringValue)
	case *ledgerpb.MetadataValue_IntValue:
		return EncodeInt64(dst, t.IntValue)
	case *ledgerpb.MetadataValue_DatetimeValue:
		// Datetime shares the order-preserving int64 encoding so datetime and
		// int64 index keys stay byte-interchangeable; the inspect handler
		// re-derives datetime-ness from the declared schema type.
		return EncodeInt64(dst, t.DatetimeValue)
	case *ledgerpb.MetadataValue_UintValue:
		return EncodeUint64(dst, t.UintValue)
	case *ledgerpb.MetadataValue_BoolValue:
		return EncodeBool(dst, t.BoolValue)
	case *ledgerpb.MetadataValue_NullValue:
		original := ""
		if t.NullValue != nil {
			original = t.NullValue.GetOriginal()
		}

		return EncodeNull(dst, original)
	default:
		return EncodeNull(dst, "")
	}
}
