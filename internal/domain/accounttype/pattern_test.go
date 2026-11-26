package accounttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestParsePattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    []PatternSegment
		wantErr string
	}{
		{
			name:    "simple fixed",
			pattern: "platform:fees",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "platform"},
				{Kind: SegmentFixed, Value: "fees"},
			},
		},
		{
			name:    "single fixed",
			pattern: "world",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "world"},
			},
		},
		{
			name:    "variable",
			pattern: "users:{id}:checking",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "users"},
				{Kind: SegmentVariable, Value: "id"},
				{Kind: SegmentFixed, Value: "checking"},
			},
		},
		{
			name:    "multiple variables",
			pattern: "org:{orgId}:departments:{deptId}:main",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "org"},
				{Kind: SegmentVariable, Value: "orgId"},
				{Kind: SegmentFixed, Value: "departments"},
				{Kind: SegmentVariable, Value: "deptId"},
				{Kind: SegmentFixed, Value: "main"},
			},
		},
		{
			name:    "variable only",
			pattern: "{type}",
			want: []PatternSegment{
				{Kind: SegmentVariable, Value: "type"},
			},
		},
		{
			name:    "fixed with hyphens and underscores",
			pattern: "my-app:fee_type",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "my-app"},
				{Kind: SegmentFixed, Value: "fee_type"},
			},
		},
		// Error cases
		{
			name:    "empty pattern",
			pattern: "",
			wantErr: "pattern must not be empty",
		},
		{
			name:    "empty segment",
			pattern: "users::checking",
			wantErr: "empty segment",
		},
		{
			name:    "invalid fixed segment",
			pattern: "users:ch@cking",
			wantErr: "invalid fixed segment",
		},
		{
			name:    "unclosed variable",
			pattern: "users:{id",
			wantErr: "unclosed variable",
		},
		{
			name:    "empty variable name",
			pattern: "users:{}:checking",
			wantErr: "empty variable name",
		},
		{
			name:    "invalid variable name starting with digit",
			pattern: "users:{1id}:checking",
			wantErr: "invalid variable name",
		},
		{
			name:    "duplicate variable name",
			pattern: "users:{id}:{id}",
			wantErr: "duplicate variable name",
		},
		{
			name:    "too many variables",
			pattern: "{a}:{b}:{c}:{d}:{e}:{f}:{g}:{h}:{i}:{j}:{k}",
			wantErr: "more than 10 variables",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := ParsePattern(tt.pattern)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}
			require.NoError(t, err)
			require.Len(t, got, len(tt.want))
			for i, seg := range got {
				assert.Equal(t, tt.want[i].Kind, seg.Kind)
				assert.Equal(t, tt.want[i].Value, seg.Value)
			}
		})
	}
}

func TestMatchAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pattern      string
		address      string
		wantMatch    bool
		wantBindings map[string]string // nil means no bindings expected
	}{
		{
			name:      "fixed match",
			pattern:   "platform:fees",
			address:   "platform:fees",
			wantMatch: true,
		},
		{
			name:      "fixed mismatch",
			pattern:   "platform:fees",
			address:   "platform:revenue",
			wantMatch: false,
		},
		{
			name:         "variable match",
			pattern:      "users:{id}:checking",
			address:      "users:alice:checking",
			wantMatch:    true,
			wantBindings: map[string]string{"id": "alice"},
		},
		{
			name:         "variable match numeric",
			pattern:      "users:{id}:checking",
			address:      "users:123:checking",
			wantMatch:    true,
			wantBindings: map[string]string{"id": "123"},
		},
		{
			name:      "wrong segment count fewer",
			pattern:   "users:{id}:checking",
			address:   "users:checking",
			wantMatch: false,
		},
		{
			name:      "wrong segment count more",
			pattern:   "users:{id}:checking",
			address:   "users:alice:checking:extra",
			wantMatch: false,
		},
		{
			name:      "fixed segment mismatch",
			pattern:   "users:{id}:checking",
			address:   "users:alice:savings",
			wantMatch: false,
		},
		{
			name:         "multiple variables",
			pattern:      "org:{orgId}:dept:{deptId}",
			address:      "org:acme:dept:engineering",
			wantMatch:    true,
			wantBindings: map[string]string{"orgId": "acme", "deptId": "engineering"},
		},
		{
			name:      "single fixed",
			pattern:   "world",
			address:   "world",
			wantMatch: true,
		},
		{
			name:         "fees with variable",
			pattern:      "fees:{type}",
			address:      "fees:card",
			wantMatch:    true,
			wantBindings: map[string]string{"type": "card"},
		},
		{
			name:      "fees variable does not match extra",
			pattern:   "fees:{type}",
			address:   "fees:card:extra",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			segments, err := ParsePattern(tt.pattern)
			require.NoError(t, err)

			bindings, ok := MatchAddress(tt.address, segments)
			assert.Equal(t, tt.wantMatch, ok)
			if tt.wantMatch {
				for k, v := range tt.wantBindings {
					got, found := bindings.Get(k)
					assert.True(t, found, "missing binding %q", k)
					assert.Equal(t, v, got, "binding %q", k)
				}
			}
		})
	}
}

func BenchmarkMatchAddress(b *testing.B) {
	segments3Fixed, _ := ParsePattern("platform:fees:main")
	segments3Var, _ := ParsePattern("users:{id}:checking")
	segments5Var, _ := ParsePattern("org:{a}:dept:{b}:team:{c}:proj:{d}:env:{e}")

	segmentsUUID, _ := ParsePattern("player:{id}:wallet")
	_ = ValidateSegmentTypes(segmentsUUID, map[string]*commonpb.SegmentType{
		"id": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
	})

	cases := []struct {
		name     string
		address  string
		segments []PatternSegment
	}{
		{"no_match_segment_count", "a:b", segments3Fixed},
		{"no_match_fixed", "platform:other:main", segments3Fixed},
		{"match_all_fixed", "platform:fees:main", segments3Fixed},
		{"match_with_variable", "users:alice:checking", segments3Var},
		{"match_with_uuid", "player:550e8400-e29b-41d4-a716-446655440000:wallet", segmentsUUID},
		{"match_5_variables", "org:acme:dept:eng:team:core:proj:ledger:env:prod", segments5Var},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				MatchAddress(tc.address, tc.segments)
			}
		})
	}
}

func TestSpecificity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
		want    int
	}{
		{"platform:fees", 2},
		{"fees:{type}", 1},
		{"users:{id}:checking", 2},
		{"{a}:{b}:{c}", 0},
		{"fees:card", 2},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			t.Parallel()
			segments, err := ParsePattern(tt.pattern)
			require.NoError(t, err)
			assert.Equal(t, tt.want, Specificity(segments))
		})
	}
}

func BenchmarkMatchWithConstraint(b *testing.B) {
	uuid := "550e8400-e29b-41d4-a716-446655440000"

	// Native UUID matcher via segment_types.
	nativeSegs, _ := ParsePattern("player:{id}:wallet")
	_ = ValidateSegmentTypes(nativeSegs, map[string]*commonpb.SegmentType{
		"id": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
	})

	// Regex UUID matcher via segment_types regex constraint.
	regexSegs, _ := ParsePattern("player:{id}:wallet")
	_ = ValidateSegmentTypes(regexSegs, map[string]*commonpb.SegmentType{
		"id": {Constraint: &commonpb.SegmentType_Regex{Regex: "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"}},
	})

	addr := "player:" + uuid + ":wallet"
	badAddr := "player:not-a-uuid:wallet"

	b.Run("uuid_native/match", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			MatchAddress(addr, nativeSegs)
		}
	})

	b.Run("uuid_regex/match", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			MatchAddress(addr, regexSegs)
		}
	})

	b.Run("uuid_native/reject", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			MatchAddress(badAddr, nativeSegs)
		}
	})

	b.Run("uuid_regex/reject", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			MatchAddress(badAddr, regexSegs)
		}
	})

	// Native uint64 vs regex.
	nativeUint, _ := ParsePattern("customer:{id}")
	_ = ValidateSegmentTypes(nativeUint, map[string]*commonpb.SegmentType{
		"id": {Constraint: &commonpb.SegmentType_Uint64{Uint64: &commonpb.Uint64Constraint{}}},
	})

	regexUint, _ := ParsePattern("customer:{id}")
	_ = ValidateSegmentTypes(regexUint, map[string]*commonpb.SegmentType{
		"id": {Constraint: &commonpb.SegmentType_Regex{Regex: "[0-9]+"}},
	})

	b.Run("uint64_native/match", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			MatchAddress("customer:12345678", nativeUint)
		}
	})

	b.Run("uint64_regex/match", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			MatchAddress("customer:12345678", regexUint)
		}
	})
}

func TestValidatePattern(t *testing.T) {
	t.Parallel()

	assert.NoError(t, ValidatePattern("users:{id}:checking"))
	assert.Error(t, ValidatePattern(""))
	assert.Error(t, ValidatePattern("users:{id}:{id}"))
}
