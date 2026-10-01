package protohelpers

import (
	"math"
	"testing"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// FuzzConvertMetadataValue fuzzes the metadata type conversion matrix.
// It generates arbitrary (value, target type) pairs and verifies that
// the conversion never panics and produces a valid commonpb.MetadataValue.
func FuzzConvertMetadataValue(f *testing.F) {
	// Seed: (value type tag, raw value, target type enum)
	// Tag: 0=string, 1=int, 2=uint, 3=bool, 4=null, 5=nil
	f.Add(byte(0), "hello", int(commonpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(0), "42", int(commonpb.MetadataType_METADATA_TYPE_INT64))
	f.Add(byte(0), "true", int(commonpb.MetadataType_METADATA_TYPE_BOOL))
	f.Add(byte(0), "-1", int(commonpb.MetadataType_METADATA_TYPE_UINT64))
	f.Add(byte(1), "42", int(commonpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(1), "-128", int(commonpb.MetadataType_METADATA_TYPE_INT8))
	f.Add(byte(1), "128", int(commonpb.MetadataType_METADATA_TYPE_INT8))
	f.Add(byte(2), "255", int(commonpb.MetadataType_METADATA_TYPE_UINT8))
	f.Add(byte(2), "256", int(commonpb.MetadataType_METADATA_TYPE_UINT8))
	f.Add(byte(3), "true", int(commonpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(3), "false", int(commonpb.MetadataType_METADATA_TYPE_INT64))
	f.Add(byte(4), "hello", int(commonpb.MetadataType_METADATA_TYPE_STRING))
	f.Add(byte(4), "42", int(commonpb.MetadataType_METADATA_TYPE_INT64))
	f.Add(byte(5), "", int(commonpb.MetadataType_METADATA_TYPE_STRING))

	maxType := int(commonpb.MetadataType_METADATA_TYPE_DATETIME) + 1

	f.Fuzz(func(t *testing.T, tag byte, raw string, targetInt int) {
		// Build the source commonpb.MetadataValue based on the tag.
		var value *commonpb.MetadataValue

		switch tag % 6 {
		case 0:
			value = commonpb.NewStringValue(raw)
		case 1:
			// Use a bounded int64 from the raw string length as seed.
			n := int64(len(raw)) - int64(math.MaxInt8)
			value = commonpb.NewIntValue(n)
		case 2:
			n := uint64(len(raw))
			value = commonpb.NewUintValue(n)
		case 3:
			value = commonpb.NewBoolValue(len(raw)%2 == 0)
		case 4:
			value = commonpb.NewNullValue(raw)
		case 5:
			value = nil
		}

		// Clamp target to valid range.
		target := commonpb.MetadataType(targetInt % maxType)
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
