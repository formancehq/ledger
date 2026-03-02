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

	t.Run("source exact", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`source == "merchants:alice"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "merchants:alice", am.GetHardcodedExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am.GetRole())
	})

	t.Run("source prefix", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`source ^= "merchants:"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "merchants:", am.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am.GetRole())
	})

	t.Run("destination exact", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`destination == "users:bob"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:bob", am.GetHardcodedExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, am.GetRole())
	})

	t.Run("destination prefix", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`destination ^= "users:"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:", am.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, am.GetRole())
	})

	t.Run("source and destination combined", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`source ^= "a:" and destination ^= "b:"`)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 2)

		srcAm := andF.Filters[0].GetAddress()
		require.NotNil(t, srcAm)
		assert.Equal(t, "a:", srcAm.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, srcAm.GetRole())

		dstAm := andF.Filters[1].GetAddress()
		require.NotNil(t, dstAm)
		assert.Equal(t, "b:", dstAm.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, dstAm.GetRole())
	})

	t.Run("address has ANY role by default", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse(`address == "users:alice"`)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_ANY, am.GetRole())
	})

	t.Run("metadata greater than", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] > 18")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.Field.GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(18), *ic.Min)
		assert.True(t, ic.MinExclusive)
		assert.Nil(t, ic.Max)
	})

	t.Run("metadata greater than or equal", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] >= 18")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.Field.GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(18), *ic.Min)
		assert.False(t, ic.MinExclusive)
		assert.Nil(t, ic.Max)
	})

	t.Run("metadata less than", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] < 65")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.Field.GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Nil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(65), *ic.Max)
		assert.True(t, ic.MaxExclusive)
	})

	t.Run("metadata less than or equal", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] <= 65")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.Field.GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Nil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(65), *ic.Max)
		assert.False(t, ic.MaxExclusive)
	})

	t.Run("metadata range combined with AND", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] >= 18 and metadata[age] < 65")
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 2)

		fc0 := andF.Filters[0].GetField()
		require.NotNil(t, fc0)
		ic0 := fc0.GetIntCond()
		require.NotNil(t, ic0)
		require.NotNil(t, ic0.Min)
		assert.Equal(t, int64(18), *ic0.Min)
		assert.False(t, ic0.MinExclusive)

		fc1 := andF.Filters[1].GetField()
		require.NotNil(t, fc1)
		ic1 := fc1.GetIntCond()
		require.NotNil(t, ic1)
		require.NotNil(t, ic1.Max)
		assert.Equal(t, int64(65), *ic1.Max)
		assert.True(t, ic1.MaxExclusive)
	})

	t.Run("metadata negative integer range", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[score] > -10")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(-10), *ic.Min)
		assert.True(t, ic.MinExclusive)
	})

	t.Run("error: string range not supported", func(t *testing.T) {
		t.Parallel()
		_, err := Parse(`metadata[name] > "alice"`)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "range operators only support integer values")
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

	// --- Parameter tests ---

	t.Run("param: metadata string equality", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[category] == $val")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.Field.GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "val", sc.GetParam())
	})

	t.Run("param: metadata != desugars to not param", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[category] != $val")
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		fc := notF.Filter.GetField()
		require.NotNil(t, fc)
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "val", sc.GetParam())
	})

	t.Run("param: metadata greater than", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] > $min")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "min", ic.ParamMin)
		assert.True(t, ic.MinExclusive)
		assert.Empty(t, ic.ParamMax)
	})

	t.Run("param: metadata greater than or equal", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] >= $min")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "min", ic.ParamMin)
		assert.False(t, ic.MinExclusive)
	})

	t.Run("param: metadata less than", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] < $max")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "max", ic.ParamMax)
		assert.True(t, ic.MaxExclusive)
		assert.Empty(t, ic.ParamMin)
	})

	t.Run("param: metadata less than or equal", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[age] <= $max")
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "max", ic.ParamMax)
		assert.False(t, ic.MaxExclusive)
	})

	t.Run("param: address exact", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("address == $addr")
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "addr", am.GetParamExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_ANY, am.GetRole())
	})

	t.Run("param: address prefix", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("address ^= $prefix")
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "prefix", am.GetParamPrefix())
	})

	t.Run("param: source exact", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("source == $src")
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "src", am.GetParamExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am.GetRole())
	})

	t.Run("param: destination prefix", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("destination ^= $dst")
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "dst", am.GetParamPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, am.GetRole())
	})

	t.Run("param: combined with hardcoded in AND", func(t *testing.T) {
		t.Parallel()
		filter, err := Parse("metadata[tier] == $tier and metadata[age] >= $min_age")
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.Filters, 2)

		fc0 := andF.Filters[0].GetField()
		require.NotNil(t, fc0)
		sc := fc0.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "tier", sc.GetParam())

		fc1 := andF.Filters[1].GetField()
		require.NotNil(t, fc1)
		ic := fc1.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "min_age", ic.ParamMin)
		assert.False(t, ic.MinExclusive)
	})
}
