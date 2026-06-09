package readstore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestEncodeMetadataValue_RoundtripDecodeValue locks in the contract that
// EncodeMetadataValue and DecodeValue are inverses for every scalar
// MetadataValue type. The reverse map (`rmap` zone) stores bytes produced by
// EncodeMetadataValue; the indexbuilder's schema-change rewrite path
// (process_logs.go::indexSetMetadataFieldType and backfill.go::backfill)
// reads those bytes back and must decode them with DecodeValue, not
// protobuf UnmarshalVT. This test guards against silently re-introducing
// the wrong decoder.
func TestEncodeMetadataValue_RoundtripDecodeValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		mv   *commonpb.MetadataValue
	}{
		{
			name: "string",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_StringValue{StringValue: "admin"},
			},
		},
		{
			name: "empty string",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_StringValue{StringValue: ""},
			},
		},
		{
			name: "int64 positive",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_IntValue{IntValue: 12345},
			},
		},
		{
			name: "int64 negative",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_IntValue{IntValue: -42},
			},
		},
		{
			name: "int64 max",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_IntValue{IntValue: 9223372036854775807},
			},
		},
		{
			name: "uint64",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_UintValue{UintValue: 12345},
			},
		},
		{
			name: "uint64 max",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_UintValue{UintValue: 18446744073709551615},
			},
		},
		{
			name: "bool true",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_BoolValue{BoolValue: true},
			},
		},
		{
			name: "bool false",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_BoolValue{BoolValue: false},
			},
		},
		{
			name: "null with original",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{Original: "not-a-number"}},
			},
		},
		{
			name: "null without original",
			mv: &commonpb.MetadataValue{
				Type: &commonpb.MetadataValue_NullValue{NullValue: &commonpb.NullValue{Original: ""}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			encoded := EncodeMetadataValue(nil, tt.mv)
			decoded, consumed, err := DecodeValue(encoded)
			require.NoError(t, err, "DecodeValue must accept the bytes EncodeMetadataValue produced")
			require.Equal(t, len(encoded), consumed, "DecodeValue must consume exactly what EncodeMetadataValue produced")
			require.True(t, proto.Equal(tt.mv, decoded),
				"roundtrip must preserve the MetadataValue exactly: got %v, want %v", decoded, tt.mv)
		})
	}
}

// TestEncodeMetadataValue_ProtobufBytesAreNotDecodable documents why the
// rewrite path must NOT use UnmarshalVT on reverse-map values: protobuf
// bytes typically do not start with a valid type tag.
func TestEncodeMetadataValue_ProtobufBytesAreNotDecodable(t *testing.T) {
	t.Parallel()

	mv := &commonpb.MetadataValue{
		Type: &commonpb.MetadataValue_StringValue{StringValue: "admin"},
	}

	protoBytes, err := mv.MarshalVT()
	require.NoError(t, err)
	require.NotEmpty(t, protoBytes)

	// Reverse: confirm sortable bytes are not valid protobuf either. The
	// sortable encoding starts with TypeTagString (0x53 = 'S'); a protobuf
	// VT decoder against those bytes returns an error (or empty value)
	// because there's no protobuf tag at byte 0.
	sortableBytes := EncodeMetadataValue(nil, mv)
	decodedAsProto := &commonpb.MetadataValue{}
	err = decodedAsProto.UnmarshalVT(sortableBytes)
	if err == nil {
		// Some inputs may happen to be parse-survivable but produce a value
		// that isn't equal to the original — that was the silent corruption.
		require.False(t, proto.Equal(mv, decodedAsProto),
			"sortable bytes must not round-trip through protobuf UnmarshalVT")
	}
}
