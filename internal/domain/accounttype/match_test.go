package accounttype

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestPatternsConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a    string
		b    string
		want bool
	}{
		{
			name: "same fixed segments with overlapping variables",
			a:    "users:{id}:checking",
			b:    "users:{name}:checking",
			want: true,
		},
		{
			name: "different fixed segment at position 2",
			a:    "users:{id}:checking",
			b:    "users:{id}:savings",
			want: false,
		},
		{
			name: "different fixed segment at position 0",
			a:    "users:{id}:checking",
			b:    "admin:{id}:checking",
			want: false,
		},
		{
			name: "different length",
			a:    "users:{id}:checking",
			b:    "users:{id}",
			want: false,
		},
		{
			name: "same specificity and compatible two segments",
			a:    "users:{id}",
			b:    "users:{name}",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			segsA, err := ParsePattern(tt.a)
			require.NoError(t, err)

			segsB, err := ParsePattern(tt.b)
			require.NoError(t, err)

			assert.Equal(t, tt.want, PatternsConflict(segsA, segsB))
		})
	}
}

func TestCompileTypesSorted(t *testing.T) {
	t.Parallel()

	// Insert keys in reverse alphabetical order to ensure the output is
	// sorted regardless of map iteration order.
	types := map[string]*commonpb.AccountType{
		"zebra": {
			Name:    "zebra",
			Pattern: "zebra:{id}",
		},
		"middle": {
			Name:    "middle",
			Pattern: "middle:{id}",
		},
		"alpha": {
			Name:    "alpha",
			Pattern: "alpha:{id}",
		},
	}

	compiled, err := CompileTypes(types)
	require.NoError(t, err)
	require.Len(t, compiled, 3)

	assert.Equal(t, "alpha", compiled[0].Original.GetName())
	assert.Equal(t, "middle", compiled[1].Original.GetName())
	assert.Equal(t, "zebra", compiled[2].Original.GetName())
}

// TestCompileTypesFailsOnInvalidType pins invariant #7 for the compiled chart:
// a stored type that does not compile is an error, never a silently dropped
// entry. Dropping it would leave a STRICT ledger with a partial (or empty)
// chart and disable enforcement for the addresses the type covers.
func TestCompileTypesFailsOnInvalidType(t *testing.T) {
	t.Parallel()

	valid := &commonpb.AccountType{Name: "alpha", Pattern: "alpha:{id}"}

	tests := []struct {
		name    string
		invalid *commonpb.AccountType
		errText string
	}{
		{
			name: "unknown segment type variable",
			invalid: &commonpb.AccountType{Name: "users", Pattern: "users:{id}", SegmentTypes: map[string]*commonpb.SegmentType{
				"nope": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
			}},
			errText: `segment_types references unknown variable "nope"`,
		},
		{
			name: "invalid segment regex",
			invalid: &commonpb.AccountType{Name: "users", Pattern: "users:{id}", SegmentTypes: map[string]*commonpb.SegmentType{
				"id": {Constraint: &commonpb.SegmentType_Regex{Regex: "("}},
			}},
			errText: "invalid regex",
		},
		{
			name:    "invalid pattern",
			invalid: &commonpb.AccountType{Name: "users", Pattern: "users::x"},
			errText: `account type "users"`,
		},
		{
			name:    "nil definition",
			invalid: nil,
			errText: `account type "users" has no definition`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			types := map[string]*commonpb.AccountType{"alpha": valid, "users": tt.invalid}

			compiled, err := CompileTypes(types)
			require.ErrorContains(t, err, tt.errText)
			require.Nil(t, compiled)

			info := &commonpb.LedgerInfo{AccountTypes: types}
			compiled, err = CompileTypesReader(info.AsReader().GetAccountTypes())
			require.ErrorContains(t, err, tt.errText)
			require.Nil(t, compiled)
		})
	}
}

// TestFindConflictSelectsFirstInChartOrder pins the deterministic conflict
// selection the FSM reports in ACCOUNT_TYPE_CONFLICT (invariant #2).
func TestFindConflictSelectsFirstInChartOrder(t *testing.T) {
	t.Parallel()

	chart, err := CompileTypes(map[string]*commonpb.AccountType{
		"zzz":    {Name: "zzz", Pattern: "users:{y}"},
		"aaa":    {Name: "aaa", Pattern: "users:{x}"},
		"orders": {Name: "orders", Pattern: "orders:{id}"},
	})
	require.NoError(t, err)

	segments, err := ParsePattern("users:{z}")
	require.NoError(t, err)

	existing, conflict := FindConflict(segments, chart)
	require.True(t, conflict)
	require.Equal(t, "aaa", existing.Name)

	segments, err = ParsePattern("merchants:{id}")
	require.NoError(t, err)

	_, conflict = FindConflict(segments, chart)
	require.False(t, conflict)
}
