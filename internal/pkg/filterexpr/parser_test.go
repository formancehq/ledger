package filterexpr

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestParse(t *testing.T) {
	t.Parallel()

	t.Run("metadata string equality", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] == premium", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.GetField().GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "premium", sc.GetHardcoded())
	})

	t.Run("metadata quoted value", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`metadata[name] == "hello world"`, tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "name", fc.GetField().GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "hello world", sc.GetHardcoded())
	})

	t.Run("metadata single-quoted value", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[name] == 'hello world'", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "hello world", sc.GetHardcoded())
	})

	t.Run("metadata boolean true", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[active] == true", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		bc := fc.GetBoolCond()
		require.NotNil(t, bc)
		assert.True(t, bc.GetHardcoded())
	})

	t.Run("metadata boolean false", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[active] == false", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		bc := fc.GetBoolCond()
		require.NotNil(t, bc)
		// GetHardcoded returns false for both "not set" and "hardcoded=false",
		// so check the oneof variant directly.
		_, ok := bc.GetValue().(*commonpb.BoolCondition_Hardcoded)
		assert.True(t, ok)
	})

	t.Run("metadata integer", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] == 42", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(42), ic.GetMin())
		assert.Equal(t, int64(42), ic.GetMax())
	})

	t.Run("metadata negative integer", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[score] == -5", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(-5), ic.GetMin())
	})

	t.Run("metadata exists", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] exists", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.GetField().GetMetadata())
		assert.NotNil(t, fc.GetExistsCond())
	})

	t.Run("metadata not equal desugars to not", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] != premium", tx)
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		fc := notF.GetFilter().GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.GetField().GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "premium", sc.GetHardcoded())
	})

	t.Run("address exact", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`address == "users:alice"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:alice", am.GetHardcodedExact())
	})

	t.Run("address prefix", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`address ^= "users:"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:", am.GetHardcodedPrefix())
	})

	t.Run("address prefix with special char must be quoted", func(t *testing.T) {
		t.Parallel()

		// A `:` is not a bare-Ident char (EN-1547): the prefix must be quoted.
		filter, err := Parse(`address ^= "users:"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:", am.GetHardcodedPrefix())

		// The bare (unquoted) punctuated form is rejected.
		_, err = Parse("address ^= users:", tx)
		require.Error(t, err)
	})

	t.Run("AND", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[a] == x and metadata[b] == y", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		fc0 := andF.GetFilters()[0].GetField()
		require.NotNil(t, fc0)
		assert.Equal(t, "a", fc0.GetField().GetMetadata())

		fc1 := andF.GetFilters()[1].GetField()
		require.NotNil(t, fc1)
		assert.Equal(t, "b", fc1.GetField().GetMetadata())
	})

	t.Run("OR", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[a] == x or metadata[b] == y", tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)
	})

	t.Run("precedence: and binds tighter than or", func(t *testing.T) {
		t.Parallel()
		// "a or b and c" should parse as "a or (b and c)"
		filter, err := Parse("metadata[a] == x or metadata[b] == y and metadata[c] == z", tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)

		// First operand is a simple condition
		assert.NotNil(t, orF.GetFilters()[0].GetField())

		// Second operand is an AND
		andF := orF.GetFilters()[1].GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)
	})

	t.Run("grouping overrides precedence", func(t *testing.T) {
		t.Parallel()
		// "(a or b) and c" should have AND at top level
		filter, err := Parse("(metadata[a] == x or metadata[b] == y) and metadata[c] == z", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		// First operand is an OR
		orF := andF.GetFilters()[0].GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)
	})

	t.Run("NOT", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("not metadata[a] == x", tx)
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		assert.NotNil(t, notF.GetFilter().GetField())
	})

	t.Run("NOT with grouping", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("not (metadata[a] == x or metadata[b] == y)", tx)
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		orF := notF.GetFilter().GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)
	})

	t.Run("multiple AND operands", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[a] == x and metadata[b] == y and metadata[c] == z", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 3)
	})

	t.Run("error: empty expression", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("", tx)
		require.Error(t, err)
	})

	t.Run("error: missing operator", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("metadata[key]", tx)
		require.Error(t, err)
	})

	t.Run("error: missing value", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("metadata[key] ==", tx)
		require.Error(t, err)
	})

	t.Run("source exact", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`source == "merchants:alice"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "merchants:alice", am.GetHardcodedExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am.GetRole())
	})

	t.Run("source prefix", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`source ^= "merchants:"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "merchants:", am.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am.GetRole())
	})

	t.Run("destination exact", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`destination == "users:bob"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:bob", am.GetHardcodedExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, am.GetRole())
	})

	t.Run("destination prefix", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`destination ^= "users:"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "users:", am.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, am.GetRole())
	})

	t.Run("source and destination combined", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`source ^= "a:" and destination ^= "b:"`, tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		srcAm := andF.GetFilters()[0].GetAddress()
		require.NotNil(t, srcAm)
		assert.Equal(t, "a:", srcAm.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, srcAm.GetRole())

		dstAm := andF.GetFilters()[1].GetAddress()
		require.NotNil(t, dstAm)
		assert.Equal(t, "b:", dstAm.GetHardcodedPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, dstAm.GetRole())
	})

	t.Run("address has ANY role by default", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`address == "users:alice"`, tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_ANY, am.GetRole())
	})

	t.Run("metadata greater than", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] > 18", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.GetField().GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(18), ic.GetMin())
		assert.True(t, ic.GetMinExclusive())
		assert.Nil(t, ic.Max)
	})

	t.Run("metadata greater than or equal", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] >= 18", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.GetField().GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(18), ic.GetMin())
		assert.False(t, ic.GetMinExclusive())
		assert.Nil(t, ic.Max)
	})

	t.Run("metadata less than", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] < 65", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.GetField().GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Nil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(65), ic.GetMax())
		assert.True(t, ic.GetMaxExclusive())
	})

	t.Run("metadata less than or equal", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] <= 65", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "age", fc.GetField().GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Nil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(65), ic.GetMax())
		assert.False(t, ic.GetMaxExclusive())
	})

	t.Run("metadata range combined with AND", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] >= 18 and metadata[age] < 65", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		fc0 := andF.GetFilters()[0].GetField()
		require.NotNil(t, fc0)
		ic0 := fc0.GetIntCond()
		require.NotNil(t, ic0)
		require.NotNil(t, ic0.Min)
		assert.Equal(t, int64(18), ic0.GetMin())
		assert.False(t, ic0.GetMinExclusive())

		fc1 := andF.GetFilters()[1].GetField()
		require.NotNil(t, fc1)
		ic1 := fc1.GetIntCond()
		require.NotNil(t, ic1)
		require.NotNil(t, ic1.Max)
		assert.Equal(t, int64(65), ic1.GetMax())
		assert.True(t, ic1.GetMaxExclusive())
	})

	t.Run("metadata negative integer range", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[score] > -10", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(-10), ic.GetMin())
		assert.True(t, ic.GetMinExclusive())
	})

	t.Run("error: string range not supported", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(`metadata[name] > "alice"`, tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "range operators only support integer values")
	})

	t.Run("metadata between inclusive range", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[block_height] between 800000 and 800099", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "block_height", fc.GetField().GetMetadata())
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(800000), ic.GetMin())
		assert.Equal(t, int64(800099), ic.GetMax())
		assert.False(t, ic.GetMinExclusive())
		assert.False(t, ic.GetMaxExclusive())
	})

	t.Run("metadata between collapses to equality when bounds match", func(t *testing.T) {
		t.Parallel()

		// `between X and X` compiles to a single value — same shape as `== X`,
		// so the downstream PrefixIterator fast path applies.
		filter, err := Parse("metadata[age] between 42 and 42", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, int64(42), ic.GetMin())
		assert.Equal(t, int64(42), ic.GetMax())
	})

	t.Run("metadata between with parameters", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] between $low and $high", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "low", ic.GetParamMin())
		assert.Equal(t, "high", ic.GetParamMax())
		assert.Nil(t, ic.Min)
		assert.Nil(t, ic.Max)
	})

	t.Run("metadata between mixed param and literal", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] between 18 and $max", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(18), ic.GetMin())
		assert.Equal(t, "max", ic.GetParamMax())
	})

	t.Run("metadata between does not consume outer AND", func(t *testing.T) {
		t.Parallel()

		// The inner `and` belongs to BetweenRange; the outer `and` to AndExpr.
		filter, err := Parse("metadata[a] between 1 and 10 and metadata[b] == foo", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		ic := andF.GetFilters()[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, int64(1), ic.GetMin())
		assert.Equal(t, int64(10), ic.GetMax())

		sc := andF.GetFilters()[1].GetField().GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "foo", sc.GetHardcoded())
	})

	t.Run("error: between with reversed bounds", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("metadata[age] between 100 and 10", tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of order")
	})

	t.Run("error: between with non-integer value", func(t *testing.T) {
		t.Parallel()

		_, err := Parse(`metadata[name] between "alice" and "bob"`, tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "range operators only support integer values")
	})

	t.Run("error: between mixes param low with non-integer high", func(t *testing.T) {
		t.Parallel()

		// Exercises the param-branch high-side parseIntValue error path.
		_, err := Parse(`metadata[name] between $lo and "bob"`, tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "range operators only support integer values")
	})

	t.Run("error: between mixes non-integer low with param high", func(t *testing.T) {
		t.Parallel()

		// Exercises the param-branch low-side parseIntValue error path.
		_, err := Parse(`metadata[name] between "alice" and $hi`, tx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "range operators only support integer values")
	})

	t.Run("error: unknown keyword", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("foobar == 42", tx)
		require.Error(t, err)
	})

	t.Run("error: trailing tokens", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("metadata[key] == val extra", tx)
		require.Error(t, err)
	})

	t.Run("error: unclosed parenthesis", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("(metadata[key] == val", tx)
		require.Error(t, err)
	})

	// --- Parameter tests ---

	t.Run("param: metadata string equality", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] == $val", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.GetField().GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "val", sc.GetParam())
	})

	t.Run("param: metadata != desugars to not param", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] != $val", tx)
		require.NoError(t, err)

		notF := filter.GetNot()
		require.NotNil(t, notF)
		fc := notF.GetFilter().GetField()
		require.NotNil(t, fc)
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "val", sc.GetParam())
	})

	t.Run("param: metadata greater than", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] > $min", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "min", ic.GetParamMin())
		assert.True(t, ic.GetMinExclusive())
		assert.Empty(t, ic.GetParamMax())
	})

	t.Run("param: metadata greater than or equal", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] >= $min", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "min", ic.GetParamMin())
		assert.False(t, ic.GetMinExclusive())
	})

	t.Run("param: metadata less than", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] < $max", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "max", ic.GetParamMax())
		assert.True(t, ic.GetMaxExclusive())
		assert.Empty(t, ic.GetParamMin())
	})

	t.Run("param: metadata less than or equal", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] <= $max", tx)
		require.NoError(t, err)

		fc := filter.GetField()
		require.NotNil(t, fc)
		ic := fc.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "max", ic.GetParamMax())
		assert.False(t, ic.GetMaxExclusive())
	})

	t.Run("param: address exact", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("address == $addr", tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "addr", am.GetParamExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_ANY, am.GetRole())
	})

	t.Run("param: address prefix", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("address ^= $prefix", tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "prefix", am.GetParamPrefix())
	})

	t.Run("param: source exact", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("source == $src", tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "src", am.GetParamExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am.GetRole())
	})

	t.Run("param: destination prefix", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("destination ^= $dst", tx)
		require.NoError(t, err)

		am := filter.GetAddress()
		require.NotNil(t, am)
		assert.Equal(t, "dst", am.GetParamPrefix())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION, am.GetRole())
	})

	t.Run("param: combined with hardcoded in AND", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[tier] == $tier and metadata[age] >= $min_age", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		fc0 := andF.GetFilters()[0].GetField()
		require.NotNil(t, fc0)
		sc := fc0.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "tier", sc.GetParam())

		fc1 := andF.GetFilters()[1].GetField()
		require.NotNil(t, fc1)
		ic := fc1.GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, "min_age", ic.GetParamMin())
		assert.False(t, ic.GetMinExclusive())
	})

	// --- In operator tests ---

	t.Run("metadata in with strings", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`metadata[category] in (premium, gold, silver)`, tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 3)

		for i, expected := range []string{"premium", "gold", "silver"} {
			fc := orF.GetFilters()[i].GetField()
			require.NotNil(t, fc)
			assert.Equal(t, "category", fc.GetField().GetMetadata())
			sc := fc.GetStringCond()
			require.NotNil(t, sc)
			assert.Equal(t, expected, sc.GetHardcoded())
		}
	})

	t.Run("metadata in with quoted strings", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`metadata[name] in ("hello world", "foo bar")`, tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)

		sc0 := orF.GetFilters()[0].GetField().GetStringCond()
		require.NotNil(t, sc0)
		assert.Equal(t, "hello world", sc0.GetHardcoded())

		sc1 := orF.GetFilters()[1].GetField().GetStringCond()
		require.NotNil(t, sc1)
		assert.Equal(t, "foo bar", sc1.GetHardcoded())
	})

	t.Run("metadata in with integers", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[age] in (18, 25, 30)", tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 3)

		for i, expected := range []int64{18, 25, 30} {
			ic := orF.GetFilters()[i].GetField().GetIntCond()
			require.NotNil(t, ic)
			assert.Equal(t, expected, ic.GetMin())
			assert.Equal(t, expected, ic.GetMax())
		}
	})

	t.Run("metadata in with single value collapses", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] in (premium)", tx)
		require.NoError(t, err)

		// Single value: no OrFilter wrapper, direct field condition.
		fc := filter.GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "category", fc.GetField().GetMetadata())
		sc := fc.GetStringCond()
		require.NotNil(t, sc)
		assert.Equal(t, "premium", sc.GetHardcoded())
	})

	t.Run("metadata in with params", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] in ($a, $b)", tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)

		assert.Equal(t, "a", orF.GetFilters()[0].GetField().GetStringCond().GetParam())
		assert.Equal(t, "b", orF.GetFilters()[1].GetField().GetStringCond().GetParam())
	})

	t.Run("address in", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`address in ("users:alice", "users:bob")`, tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)

		am0 := orF.GetFilters()[0].GetAddress()
		require.NotNil(t, am0)
		assert.Equal(t, "users:alice", am0.GetHardcodedExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_ANY, am0.GetRole())

		am1 := orF.GetFilters()[1].GetAddress()
		require.NotNil(t, am1)
		assert.Equal(t, "users:bob", am1.GetHardcodedExact())
	})

	t.Run("source in", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse(`source in ("bank:main", "bank:secondary")`, tx)
		require.NoError(t, err)

		orF := filter.GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)

		am0 := orF.GetFilters()[0].GetAddress()
		require.NotNil(t, am0)
		assert.Equal(t, "bank:main", am0.GetHardcodedExact())
		assert.Equal(t, commonpb.AddressRole_ADDRESS_ROLE_SOURCE, am0.GetRole())
	})

	t.Run("metadata in combined with AND", func(t *testing.T) {
		t.Parallel()

		filter, err := Parse("metadata[category] in (a, b) and metadata[status] == active", tx)
		require.NoError(t, err)

		andF := filter.GetAnd()
		require.NotNil(t, andF)
		require.Len(t, andF.GetFilters(), 2)

		// First operand is the desugared OR from "in"
		orF := andF.GetFilters()[0].GetOr()
		require.NotNil(t, orF)
		require.Len(t, orF.GetFilters(), 2)

		// Second operand is a simple field condition
		fc := andF.GetFilters()[1].GetField()
		require.NotNil(t, fc)
		assert.Equal(t, "status", fc.GetField().GetMetadata())
	})

	t.Run("error: metadata in empty list", func(t *testing.T) {
		t.Parallel()

		_, err := Parse("metadata[category] in ()", tx)
		require.Error(t, err)
	})

	// Regression for #341 / Review-2 L-19. Without the depth guard,
	// participle's recursive-descent parser stack-overflows the
	// process on inputs with many `not ` or `(` repetitions. Reject
	// such inputs upfront.
	t.Run("error: deeply nested not", func(t *testing.T) {
		t.Parallel()

		input := strings.Repeat("not ", MaxParseDepth+5) + "metadata[x] == y"

		_, err := Parse(input, tx)
		require.ErrorIs(t, err, ErrFilterTooDeep,
			"deeply-nested `not` chain must trip the depth guard before participle (#341)")
	})

	t.Run("error: deeply nested parens", func(t *testing.T) {
		t.Parallel()

		opens := strings.Repeat("(", MaxParseDepth+5)
		closes := strings.Repeat(")", MaxParseDepth+5)

		_, err := Parse(opens+"metadata[x] == y"+closes, tx)
		require.ErrorIs(t, err, ErrFilterTooDeep,
			"deeply-nested parens must trip the depth guard before participle (#341)")
	})
}

func TestParse_HasAsset(t *testing.T) {
	t.Parallel()

	t.Run("bare asset is precision 0", func(t *testing.T) {
		t.Parallel()

		f, err := Parse("has asset USD", tx)
		require.NoError(t, err)
		c := f.GetAccountHasAsset()
		require.NotNil(t, c)
		assert.Equal(t, "USD", c.GetAssetBase())
		assert.Equal(t, uint32(0), c.GetPrecision())
	})

	t.Run("asset with precision", func(t *testing.T) {
		t.Parallel()

		f, err := Parse("has asset USD/4", tx)
		require.NoError(t, err)
		c := f.GetAccountHasAsset()
		require.NotNil(t, c)
		assert.Equal(t, "USD", c.GetAssetBase())
		assert.Equal(t, uint32(4), c.GetPrecision())
	})

	t.Run("combined with metadata", func(t *testing.T) {
		t.Parallel()

		f, err := Parse("has asset USD and metadata[type] == premium", tx)
		require.NoError(t, err)
		require.NotNil(t, f.GetAnd())
		require.Len(t, f.GetAnd().GetFilters(), 2)
	})

	// A "/" present but with a non-canonical precision must be rejected, not
	// silently coerced to a different cell (a wrong result set). Covers
	// unparseable suffixes plus the leading-zero / zero-precision forms that
	// domain.ValidateAsset also rejects ("USD/02" aliasing "USD/2",
	// "USD/0"/"USD/00" aliasing the bare-"USD" precision-0 cell).
	t.Run("malformed precision is rejected", func(t *testing.T) {
		t.Parallel()

		for _, in := range []string{
			"has asset USD/abc", "has asset USD/256", "has asset USD/2/3", "has asset USD/",
			"has asset USD/0", "has asset USD/02", "has asset USD/00",
		} {
			_, err := Parse(in, tx)
			require.Error(t, err, "Parse(%q, tx) should reject the non-canonical precision", in)
		}
	})
}

// TestParse_HasAssetKeywordsNotReserved guards that `has` and `asset` stay
// usable as bare account/metadata values — they must not be promoted to global
// lexer keywords just to drive the `has asset` production.
func TestParse_HasAssetKeywordsNotReserved(t *testing.T) {
	t.Parallel()

	t.Run("has is a usable bare address value", func(t *testing.T) {
		t.Parallel()

		f, err := Parse("address == has", tx)
		require.NoError(t, err)
		assert.Equal(t, "has", f.GetAddress().GetHardcodedExact())
	})

	t.Run("asset is a usable bare metadata value", func(t *testing.T) {
		t.Parallel()

		f, err := Parse("metadata[k] == asset", tx)
		require.NoError(t, err)
		assert.Equal(t, "asset", f.GetField().GetStringCond().GetHardcoded())
	})

	t.Run("has-prefixed address segment must be quoted", func(t *testing.T) {
		t.Parallel()

		// The `:` makes this not a bare Ident (EN-1547): quote it.
		f, err := Parse(`address ^= "has:wallet"`, tx)
		require.NoError(t, err)
		assert.Equal(t, "has:wallet", f.GetAddress().GetHardcodedPrefix())

		_, err = Parse("address ^= has:wallet", tx)
		require.Error(t, err)
	})
}
