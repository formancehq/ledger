package readstore

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// EncodeMetadataValue encodes a MetadataValue into a sortable byte sequence
// suitable for use as part of a Pebble key. The encoding includes a type tag
// and a value encoding that preserves sort order within each type.
func EncodeMetadataValue(dst []byte, v *commonpb.MetadataValue) []byte {
	if v == nil {
		return EncodeNull(dst, "")
	}

	switch t := v.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return EncodeString(dst, t.StringValue)
	case *commonpb.MetadataValue_IntValue:
		return EncodeInt64(dst, t.IntValue)
	case *commonpb.MetadataValue_UintValue:
		return EncodeUint64(dst, t.UintValue)
	case *commonpb.MetadataValue_BoolValue:
		return EncodeBool(dst, t.BoolValue)
	case *commonpb.MetadataValue_NullValue:
		original := ""
		if t.NullValue != nil {
			original = t.NullValue.GetOriginal()
		}

		return EncodeNull(dst, original)
	default:
		return EncodeNull(dst, "")
	}
}
