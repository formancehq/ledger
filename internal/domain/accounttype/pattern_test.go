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
		wantBindings map[string]string
	}{
		{
			name:         "fixed match",
			pattern:      "platform:fees",
			address:      "platform:fees",
			wantMatch:    true,
			wantBindings: map[string]string{},
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
			name:         "single fixed",
			pattern:      "world",
			address:      "world",
			wantMatch:    true,
			wantBindings: map[string]string{},
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
				assert.Equal(t, tt.wantBindings, bindings)
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

func TestRewriteAddress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		bindings map[string]string
		target   string
		want     string
		wantErr  string
	}{
		{
			name:     "simple rewrite",
			bindings: map[string]string{"id": "alice"},
			target:   "clients:{id}:courant",
			want:     "clients:alice:courant",
		},
		{
			name:     "multiple variables",
			bindings: map[string]string{"orgId": "acme", "deptId": "eng"},
			target:   "companies:{orgId}:teams:{deptId}",
			want:     "companies:acme:teams:eng",
		},
		{
			name:     "pure fixed target",
			bindings: map[string]string{"id": "alice"},
			target:   "platform:fees",
			want:     "platform:fees",
		},
		{
			name:     "missing binding",
			bindings: map[string]string{"id": "alice"},
			target:   "clients:{name}:courant",
			wantErr:  "missing binding",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			target, err := ParsePattern(tt.target)
			require.NoError(t, err)

			got, err := RewriteAddress(tt.bindings, target)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestPatternString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern string
	}{
		{"platform:fees"},
		{"users:{id}:checking"},
		{"banks:{iban:^[A-Z]{2}[0-9]{14}$}:main"},
		{"org:{orgId}:departments:{deptId:^[0-9]+$}:main"},
	}

	for _, tt := range tests {
		t.Run(tt.pattern, func(t *testing.T) {
			t.Parallel()
			segments, err := ParsePattern(tt.pattern)
			require.NoError(t, err)
			assert.Equal(t, tt.pattern, PatternString(segments))
		})
	}
}

func TestVariableNames(t *testing.T) {
	t.Parallel()

	segments, err := ParsePattern("org:{orgId}:departments:{deptId}:main")
	require.NoError(t, err)
	assert.Equal(t, []string{"orgId", "deptId"}, VariableNames(segments))
}

func TestSortBySpecificity(t *testing.T) {
	t.Parallel()

	// fees:card (specificity 2) should come before fees:{type} (specificity 1)
	feesCard, err := ParsePattern("fees:card")
	require.NoError(t, err)
	feesType, err := ParsePattern("fees:{type}")
	require.NoError(t, err)

	patterns := [][]PatternSegment{feesType, feesCard}
	SortBySpecificity(patterns)

	assert.Equal(t, "fees:card", PatternString(patterns[0]))
	assert.Equal(t, "fees:{type}", PatternString(patterns[1]))
}

func TestValidatePattern(t *testing.T) {
	t.Parallel()

	assert.NoError(t, ValidatePattern("users:{id}:checking"))
	assert.Error(t, ValidatePattern(""))
	assert.Error(t, ValidatePattern("users:{id}:{id}"))
}

func TestDetectOverlaps(t *testing.T) {
	t.Parallel()

	feesCard, err := ParsePattern("fees:card")
	require.NoError(t, err)
	feesType, err := ParsePattern("fees:{type}")
	require.NoError(t, err)
	usersChecking, err := ParsePattern("users:{id}:checking")
	require.NoError(t, err)

	existing := map[string][]PatternSegment{
		"fees-card":      feesCard,
		"fees-type":      feesType,
		"users-checking": usersChecking,
	}

	// fees:{category} overlaps with both fees:card and fees:{type}
	newPattern, err := ParsePattern("fees:{category}")
	require.NoError(t, err)
	overlaps := DetectOverlaps(newPattern, existing)
	assert.Equal(t, []string{"fees-card", "fees-type"}, overlaps)

	// platform:fees overlaps with nothing (different segment count)
	platformFees, err := ParsePattern("platform:fees:main")
	require.NoError(t, err)
	overlaps = DetectOverlaps(platformFees, existing)
	assert.Empty(t, overlaps)
}
