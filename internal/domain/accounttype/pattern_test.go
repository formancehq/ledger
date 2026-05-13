package accounttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
			name:    "variable without regex",
			pattern: "users:{id}:checking",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "users"},
				{Kind: SegmentVariable, Value: "id"},
				{Kind: SegmentFixed, Value: "checking"},
			},
		},
		{
			name:    "variable with regex",
			pattern: "banks:{iban:^[A-Z]{2}[0-9]{14}$}:main",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "banks"},
				{Kind: SegmentVariable, Value: "iban", Pattern: "^[A-Z]{2}[0-9]{14}$"},
				{Kind: SegmentFixed, Value: "main"},
			},
		},
		{
			name:    "multiple variables",
			pattern: "org:{orgId}:departments:{deptId:^[0-9]+$}:main",
			want: []PatternSegment{
				{Kind: SegmentFixed, Value: "org"},
				{Kind: SegmentVariable, Value: "orgId"},
				{Kind: SegmentFixed, Value: "departments"},
				{Kind: SegmentVariable, Value: "deptId", Pattern: "^[0-9]+$"},
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
			name:    "invalid regex in variable",
			pattern: "users:{id:[invalid}:checking",
			wantErr: "invalid regex",
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
				assert.Equal(t, tt.want[i].Pattern, seg.Pattern)
				if seg.Pattern != "" {
					assert.NotNil(t, seg.CompiledRegexp, "CompiledRegexp should be set for pattern %q", seg.Pattern)
				} else {
					assert.Nil(t, seg.CompiledRegexp)
				}
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
			name:         "regex match",
			pattern:      "banks:{iban:^[A-Z]{2}[0-9]{14}$}:main",
			address:      "banks:FR76300060000112:main",
			wantMatch:    true,
			wantBindings: map[string]string{"iban": "FR76300060000112"},
		},
		{
			name:      "regex mismatch",
			pattern:   "banks:{iban:^[A-Z]{2}[0-9]{14}$}:main",
			address:   "banks:invalid:main",
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
	segmentsRegex, _ := ParsePattern("banks:{iban:^[A-Z]{2}[0-9]{14}$}:main")
	segments5Var, _ := ParsePattern("org:{a}:dept:{b}:team:{c}:proj:{d}:env:{e}")

	cases := []struct {
		name     string
		address  string
		segments []PatternSegment
	}{
		{"no_match_segment_count", "a:b", segments3Fixed},
		{"no_match_fixed", "platform:other:main", segments3Fixed},
		{"match_all_fixed", "platform:fees:main", segments3Fixed},
		{"match_with_variable", "users:alice:checking", segments3Var},
		{"match_with_regex", "banks:FR76300060000112:main", segmentsRegex},
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

func TestValidatePattern(t *testing.T) {
	t.Parallel()

	assert.NoError(t, ValidatePattern("users:{id}:checking"))
	assert.Error(t, ValidatePattern(""))
	assert.Error(t, ValidatePattern("users:{id}:{id}"))
}
