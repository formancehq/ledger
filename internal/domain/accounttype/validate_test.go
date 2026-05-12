package accounttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

func TestValidateSegmentTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		segTypes map[string]commonpb.SegmentValueType
		wantErr  string
	}{
		{
			name:     "valid uuid type",
			pattern:  "users:{id}:checking",
			segTypes: map[string]commonpb.SegmentValueType{"id": commonpb.SegmentValueType_SEGMENT_VALUE_UUID},
		},
		{
			name:     "valid uint64 type",
			pattern:  "orders:{seq}",
			segTypes: map[string]commonpb.SegmentValueType{"seq": commonpb.SegmentValueType_SEGMENT_VALUE_UINT64},
		},
		{
			name:     "valid bytes type",
			pattern:  "wallets:{hash}",
			segTypes: map[string]commonpb.SegmentValueType{"hash": commonpb.SegmentValueType_SEGMENT_VALUE_BYTES},
		},
		{
			name:     "string type is no-op",
			pattern:  "users:{id}",
			segTypes: map[string]commonpb.SegmentValueType{"id": commonpb.SegmentValueType_SEGMENT_VALUE_STRING},
		},
		{
			name:     "empty segment types",
			pattern:  "users:{id}",
			segTypes: nil,
		},
		{
			name:     "unknown variable name",
			pattern:  "users:{id}",
			segTypes: map[string]commonpb.SegmentValueType{"unknown": commonpb.SegmentValueType_SEGMENT_VALUE_UUID},
			wantErr:  "unknown variable",
		},
		{
			name:     "uuid with compatible explicit regex",
			pattern:  "users:{id:^[0-9a-f-]+$}:checking",
			segTypes: map[string]commonpb.SegmentValueType{"id": commonpb.SegmentValueType_SEGMENT_VALUE_UUID},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			segments, err := ParsePattern(tt.pattern)
			require.NoError(t, err)

			err = ValidateSegmentTypes(segments, tt.segTypes)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestValidateSegmentTypes_ImplicitRegex(t *testing.T) {
	t.Parallel()

	t.Run("uuid regex rejects non-uuid", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("users:{id}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]commonpb.SegmentValueType{
			"id": commonpb.SegmentValueType_SEGMENT_VALUE_UUID,
		}))

		_, ok := MatchAddress("users:not-a-uuid", segments)
		assert.False(t, ok)

		_, ok = MatchAddress("users:550e8400-e29b-41d4-a716-446655440000", segments)
		assert.True(t, ok)
	})

	t.Run("uint64 regex rejects non-numeric", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("orders:{seq}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]commonpb.SegmentValueType{
			"seq": commonpb.SegmentValueType_SEGMENT_VALUE_UINT64,
		}))

		_, ok := MatchAddress("orders:abc", segments)
		assert.False(t, ok)

		_, ok = MatchAddress("orders:12345", segments)
		assert.True(t, ok)
	})

	t.Run("bytes regex rejects odd-length hex", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("wallets:{hash}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]commonpb.SegmentValueType{
			"hash": commonpb.SegmentValueType_SEGMENT_VALUE_BYTES,
		}))

		_, ok := MatchAddress("wallets:abc", segments)
		assert.False(t, ok, "odd-length hex should not match")

		_, ok = MatchAddress("wallets:deadbeef", segments)
		assert.True(t, ok)
	})
}
