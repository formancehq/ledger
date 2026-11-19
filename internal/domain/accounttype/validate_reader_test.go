package accounttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestSegmentConstraintReaderValidationParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		segmentType *commonpb.SegmentType
		valid       string
		invalid     string
		constrained bool
	}{
		{"regex", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Regex{Regex: "admin|user"}}, "admin", "superadmin", true},
		{"empty regex", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Regex{}}, "", "admin", true},
		{"uuid", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}}, "550e8400-e29b-41d4-a716-446655440000", "not-a-uuid", true},
		{"nil uuid payload", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Uuid{}}, "550e8400-e29b-41d4-a716-446655440000", "not-a-uuid", true},
		{"uint64", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Uint64{Uint64: &commonpb.Uint64Constraint{}}}, "12345", "12a", true},
		{"nil uint64 payload", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Uint64{}}, "12345", "12a", true},
		{"bytes", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Bytes{Bytes: &commonpb.BytesConstraint{}}}, "deadbeef", "abc", true},
		{"nil bytes payload", &commonpb.SegmentType{Constraint: &commonpb.SegmentType_Bytes{}}, "deadbeef", "abc", true},
		{"unset constraint", &commonpb.SegmentType{}, "arbitrary", "", false},
		{"nil segment type", nil, "arbitrary", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			at := &commonpb.AccountType{
				Name: "users", Pattern: "users:{id}",
				SegmentTypes: map[string]*commonpb.SegmentType{"id": tt.segmentType},
			}
			segments, err := ParsePattern(at.GetPattern())
			require.NoError(t, err)
			require.NoError(t, ValidateSegmentTypes(segments, at.GetSegmentTypes()))

			// Exercise the actual immutable ledger reader used to compile cached types.
			ledger := &commonpb.LedgerInfo{AccountTypes: map[string]*commonpb.AccountType{"users": at}}
			compiled := CompileTypesReader(ledger.AsReader().GetAccountTypes())
			require.Len(t, compiled, 1)

			for _, candidate := range [][]PatternSegment{segments, compiled[0].Segments} {
				matcher := candidate[1].Matcher
				if tt.constrained {
					require.NotNil(t, matcher, "selected constraints install a matcher even when their message payload is nil")
					assert.True(t, matcher(tt.valid))
					assert.False(t, matcher(tt.invalid))
				} else {
					assert.Nil(t, matcher)
				}
				_, matches := MatchAddress("users:"+tt.invalid, candidate)
				assert.False(t, matches)
				_, matches = MatchAddress("users:"+tt.valid, candidate)
				assert.Equal(t, tt.valid != "", matches, "address matching still rejects empty segments")
			}
		})
	}
}

func TestSegmentConstraintReaderErrorParity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		types   map[string]*commonpb.SegmentType
		wantErr string
	}{
		{
			name:    "unknown names sorted",
			types:   map[string]*commonpb.SegmentType{"zzz": {}, "aaa": nil},
			wantErr: `segment_types references unknown variable "aaa"`,
		},
		{
			name: "invalid regex sorted",
			types: map[string]*commonpb.SegmentType{
				"z": {Constraint: &commonpb.SegmentType_Regex{Regex: "["}},
				"a": {Constraint: &commonpb.SegmentType_Regex{Regex: "("}},
			},
			wantErr: `segment_types variable "a": invalid regex "("`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			at := &commonpb.AccountType{Pattern: "users:{a}:{z}", SegmentTypes: tt.types}
			for range 64 {
				rawSegments, err := ParsePattern(at.GetPattern())
				require.NoError(t, err)
				readerSegments, err := ParsePattern(at.GetPattern())
				require.NoError(t, err)
				rawErr := ValidateSegmentTypes(rawSegments, tt.types)
				readerErr := validateSegmentTypesReader(readerSegments, at.AsReader().GetSegmentTypes())
				require.ErrorContains(t, rawErr, tt.wantErr)
				require.EqualError(t, readerErr, rawErr.Error())
			}

			ledger := &commonpb.LedgerInfo{AccountTypes: map[string]*commonpb.AccountType{"invalid": at}}
			assert.Empty(t, CompileTypesReader(ledger.AsReader().GetAccountTypes()), "invalid constraints must not become unconstrained compiled types")
		})
	}
}

func TestCompileTypesReaderSorted(t *testing.T) {
	t.Parallel()

	ledger := &commonpb.LedgerInfo{AccountTypes: map[string]*commonpb.AccountType{
		"zebra":   {Name: "zebra", Pattern: "zebra:{id}"},
		"alpha":   {Name: "alpha", Pattern: "alpha:{id}"},
		"nil":     nil,
		"invalid": {Name: "invalid", Pattern: ""},
	}}
	compiled := CompileTypesReader(ledger.AsReader().GetAccountTypes())
	require.Len(t, compiled, 2)
	assert.Equal(t, "alpha", compiled[0].Original.GetName())
	assert.Equal(t, "zebra", compiled[1].Original.GetName())
	assert.Empty(t, CompileTypesReader(nil))
}
