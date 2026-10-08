package protohelpers

import (
	"math"
	"testing"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// FuzzConvertMetadataValue fuzzes the metadata type conversion matrix.
// It generates arbitrary (value, target type) pairs and verifies that
// the conversion never panics and produces a valid ledgerpb.MetadataValue.
func FuzzConvertMetadataValue(f *testing.F) {
	// Seed: (value type tag, raw value, target type enum)
	// Tag: 0=string, 1=int, 2=uint, 3=bool, 4=null, 5=nil
	f.Add(byte(0), "hello", int(ledgerpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(0), "42", int(ledgerpb.MetadataType_METADATA_TYPE_INT64))
	f.Add(byte(0), "true", int(ledgerpb.MetadataType_METADATA_TYPE_BOOL))
	f.Add(byte(0), "-1", int(ledgerpb.MetadataType_METADATA_TYPE_UINT64))
	f.Add(byte(1), "42", int(ledgerpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(1), "-128", int(ledgerpb.MetadataType_METADATA_TYPE_INT8))
	f.Add(byte(1), "128", int(ledgerpb.MetadataType_METADATA_TYPE_INT8))
	f.Add(byte(2), "255", int(ledgerpb.MetadataType_METADATA_TYPE_UINT8))
	f.Add(byte(2), "256", int(ledgerpb.MetadataType_METADATA_TYPE_UINT8))
	f.Add(byte(3), "true", int(ledgerpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(3), "false", int(ledgerpb.MetadataType_METADATA_TYPE_INT64))
	f.Add(byte(4), "hello", int(ledgerpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(4), "42", int(ledgerpb.MetadataType_METADATA_TYPE_INT64))
	f.Add(byte(5), "", int(ledgerpb.MetadataType_METADATA_TYPE_STRING))

	maxType := int(ledgerpb.MetadataType_METADATA_TYPE_DATETIME) + 1

	f.Fuzz(func(t *testing.T, tag byte, raw string, targetInt int) {
		// Build the source ledgerpb.MetadataValue based on the tag.
		var value *ledgerpb.MetadataValue

		switch tag % 6 {
		case 0:
			value = ledgerpb.NewStringValue(raw)
		case 1:
			// Use a bounded int64 from the raw string length as seed.
			n := int64(len(raw)) - int64(math.MaxInt8)
			value = ledgerpb.NewIntValue(n)
		case 2:
			n := uint64(len(raw))
			value = ledgerpb.NewUintValue(n)
		case 3:
			value = ledgerpb.NewBoolValue(len(raw)%2 == 0)
		case 4:
			value = ledgerpb.NewNullValue(raw)
		case 5:
			value = nil
		}

		// Clamp target to valid range.
		target := ledgerpb.MetadataType(targetInt % maxType)
		if target < 0 {
			target = -target
		}

		// Must not panic.
		result := ConvertMetadataValue(value, target)

		// Result must always be non-nil (nil input produces NullValue).
		if result == nil {
			t.Fatal("ConvertMetadataValue returned nil")
		}
	})
}
