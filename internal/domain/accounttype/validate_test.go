package accounttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestValidateSegmentTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		pattern  string
		segTypes map[string]*commonpb.SegmentType
		wantErr  string
	}{
		{
			name:    "uuid type",
			pattern: "users:{id}:checking",
			segTypes: map[string]*commonpb.SegmentType{
				"id": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
			},
		},
		{
			name:    "uint64 type",
			pattern: "orders:{seq}",
			segTypes: map[string]*commonpb.SegmentType{
				"seq": {Constraint: &commonpb.SegmentType_Uint64{Uint64: &commonpb.Uint64Constraint{}}},
			},
		},
		{
			name:    "bytes type",
			pattern: "wallets:{hash}",
			segTypes: map[string]*commonpb.SegmentType{
				"hash": {Constraint: &commonpb.SegmentType_Bytes{Bytes: &commonpb.BytesConstraint{}}},
			},
		},
		{
			name:    "regex constraint",
			pattern: "users:{role}",
			segTypes: map[string]*commonpb.SegmentType{
				"role": {Constraint: &commonpb.SegmentType_Regex{Regex: "admin|user|guest"}},
			},
		},
		{
			name:     "empty segment types",
			pattern:  "users:{id}",
			segTypes: nil,
		},
		{
			name:    "unknown variable name",
			pattern: "users:{id}",
			segTypes: map[string]*commonpb.SegmentType{
				"unknown": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
			},
			wantErr: "unknown variable",
		},
		{
			name:    "invalid regex",
			pattern: "users:{id}",
			segTypes: map[string]*commonpb.SegmentType{
				"id": {Constraint: &commonpb.SegmentType_Regex{Regex: "[invalid"}},
			},
			wantErr: "invalid regex",
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

	t.Run("uuid rejects non-uuid", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("users:{id}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]*commonpb.SegmentType{
			"id": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
		}))

		_, ok := MatchAddress("users:not-a-uuid", segments)
		assert.False(t, ok)

		_, ok = MatchAddress("users:550e8400-e29b-41d4-a716-446655440000", segments)
		assert.True(t, ok)

		_, ok = MatchAddress("users:550E8400-E29B-41D4-A716-446655440000", segments)
		assert.True(t, ok, "uppercase UUID should match")

		_, ok = MatchAddress("users:550E8400-e29b-41D4-a716-446655440000", segments)
		assert.True(t, ok, "mixed-case UUID should match")
	})

	t.Run("uint64 rejects non-numeric", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("orders:{seq}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]*commonpb.SegmentType{
			"seq": {Constraint: &commonpb.SegmentType_Uint64{Uint64: &commonpb.Uint64Constraint{}}},
		}))

		_, ok := MatchAddress("orders:abc", segments)
		assert.False(t, ok)

		_, ok = MatchAddress("orders:12345", segments)
		assert.True(t, ok)
	})

	t.Run("bytes rejects odd-length hex", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("wallets:{hash}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]*commonpb.SegmentType{
			"hash": {Constraint: &commonpb.SegmentType_Bytes{Bytes: &commonpb.BytesConstraint{}}},
		}))

		_, ok := MatchAddress("wallets:abc", segments)
		assert.False(t, ok, "odd-length hex should not match")

		_, ok = MatchAddress("wallets:deadbeef", segments)
		assert.True(t, ok)

		_, ok = MatchAddress("wallets:DEADBEEF", segments)
		assert.True(t, ok, "uppercase hex should match")

		_, ok = MatchAddress("wallets:DeAdBeEf", segments)
		assert.True(t, ok, "mixed-case hex should match")
	})

	t.Run("regex constrains values", func(t *testing.T) {
		t.Parallel()

		segments, err := ParsePattern("users:{role}")
		require.NoError(t, err)

		require.NoError(t, ValidateSegmentTypes(segments, map[string]*commonpb.SegmentType{
			"role": {Constraint: &commonpb.SegmentType_Regex{Regex: "admin|user|guest"}},
		}))

		_, ok := MatchAddress("users:admin", segments)
		assert.True(t, ok)

		_, ok = MatchAddress("users:hacker", segments)
		assert.False(t, ok)
	})
}
