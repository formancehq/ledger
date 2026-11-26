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

	compiled := CompileTypes(types)
	require.Len(t, compiled, 3)

	assert.Equal(t, "alpha", compiled[0].Original.GetName())
	assert.Equal(t, "middle", compiled[1].Original.GetName())
	assert.Equal(t, "zebra", compiled[2].Original.GetName())
}
