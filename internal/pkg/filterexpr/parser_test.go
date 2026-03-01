package filterexpr

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("metadata string equality", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[category] == premium")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.Field.GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "premium", sc.GetHardcoded())
	})

	t.Run("metadata quoted value", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`metadata[name] == "hello world"`)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "name", fc.Field.GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "hello world", sc.GetHardcoded())
	})

	t.Run("metadata single-quoted value", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[name] == 'hello world'")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "hello world", sc.GetHardcoded())
	})

	t.Run("metadata boolean true", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[active] == true")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		bc := fc.GetBoolCond()
		require.NotNil(t, bc)
		assert.True(t, bc.GetHardcoded())
	})

	t.Run("metadata boolean false", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[active] == false")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		bc := fc.GetBoolCond()
		require.NotNil(t, bc)
		// GetHardcoded returns false for both "not set" and "hardcoded=false",
		// so check the oneof variant directly.
		_, ok := bc.Value.(*commonpb.BoolCondition_Hardcoded)
		assert.True(t, ok)
	})

	t.Run("metadata integer", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] == 42")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(42), *ic.Min)
		assert.Equal(t, int64(42), *ic.Max)
	})

	t.Run("metadata negative integer", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[score] == -5")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(-5), *ic.Min)
	})

	t.Run("metadata exists", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[category] exists")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.Field.GetMetadata())
		assert.NotNil(t, fc.GetExistsCond())
	})

	t.Run("metadata not equal desugars to not", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[category] != premium")
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		fc := notF.Filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.Field.GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "premium", sc.GetHardcoded())
	})

	t.Run("address exact", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`address == "users:alice"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:alice", am.GetHardcodedExact())
	})

	t.Run("address prefix", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`address ^= "users:"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:", am.GetHardcodedPrefix())
	})

	t.Run("address bare word", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("address ^= users:")
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:", am.GetHardcodedPrefix())
	})

	t.Run("AND", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[a] == x and metadata[b] == y")
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 2)

		fc0 := andF.Filters[0].GetField()
		require.NotNil(t, fc0)
		assert.Equal(t, "a", fc0.Field.GetMetadata())

		fc1 := andF.Filters[1].GetField()
		require.NotNil(t, fc1)
		assert.Equal(t, "b", fc1.Field.GetMetadata())
	})

	t.Run("OR", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[a] == x or metadata[b] == y")
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.Filters, 2)
	})

	t.Run("precedence: and binds tighter than or", func(t *testing.T) {
		t.Parallel()
		// "a or b and c" should parse as "a or (b and c)"
		filter, err := Parse("metadata[a] == x or metadata[b] == y and metadata[c] == z")
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.Filters, 2)

		// First operand is a simple condition
		assert.NotNil(t, orF.Filters[0].GetField())

		// Second operand is an AND
		andF := orF.Filters[1].GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 2)
	})

	t.Run("grouping overrides precedence", func(t *testing.T) {
		t.Parallel()
		// "(a or b) and c" should have AND at top level
		filter, err := Parse("(metadata[a] == x or metadata[b] == y) and metadata[c] == z")
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 2)

		// First operand is an OR
		orF := andF.Filters[0].GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.Filters, 2)
	})

	t.Run("NOT", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("not metadata[a] == x")
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		assert.NotNil(t, notF.Filter.GetField())
	})

	t.Run("NOT with grouping", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("not (metadata[a] == x or metadata[b] == y)")
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		orF := notF.Filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.Filters, 2)
	})

	t.Run("multiple AND operands", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[a] == x and metadata[b] == y and metadata[c] == z")
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 3)
	})

	t.Run("error: empty expression", func(t *testing.T) {
		t.Parallel()
		_, err := Parse("")
		require.Error(t, err)
	})

	t.Run("error: missing operator", func(t *testing.T) {
		t.Parallel()
		_, err := Parse("metadata[key]")
		require.Error(t, err)
	})

	t.Run("error: missing value", func(t *testing.T) {
		t.Parallel()
		_, err := Parse("metadata[key] ==")
		require.Error(t, err)
	})

	t.Run("error: unknown keyword", func(t *testing.T) {
		t.Parallel()
		_, err := Parse("foobar == 42")
		require.Error(t, err)
	})

	t.Run("error: trailing tokens", func(t *testing.T) {
		t.Parallel()
		_, err := Parse("metadata[key] == val extra")
		require.Error(t, err)
	})

	t.Run("error: unclosed parenthesis", func(t *testing.T) {
		t.Parallel()
		_, err := Parse("(metadata[key] == val")
		require.Error(t, err)
	})
}
