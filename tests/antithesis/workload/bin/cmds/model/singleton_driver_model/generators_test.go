package main

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Every shape the wire allows on a numeric field leaf must come out of the
// generator: open sides, exclusive sides, and the type's extrema.
func TestGenFieldLeafCoversEveryNumericShape(t *testing.T) {
	t.Parallel()

	seeds := []fieldSeed{
		{key: "u", declaredType: commonpb.MetadataType_METADATA_TYPE_UINT64, sample: commonpb.NewUintValue(7)},
		{key: "i", declaredType: commonpb.MetadataType_METADATA_TYPE_INT64, sample: commonpb.NewIntValue(-7)},
	}

	var (
		uintOpenMin, uintOpenMax, uintExclMin, uintExclMax, uintZero, uintMax bool
		intOpenMin, intOpenMax, intExclMin, intExclMax, intMin, intMax        bool
	)

	var splitRange, wrapShape bool

	for range 20_000 {
		f := genFieldLeaf(seeds)

		// A split range is And(key >= lo, key <= hi) on one key; with an
		// exclusive extremum on one side it is a contradiction the compiler must
		// keep empty, e.g. And(x >= a, x < 0) on an unsigned key.
		if and := f.GetAnd(); and != nil {
			require.Len(t, and.GetFilters(), 2)
			a, b := and.GetFilters()[0].GetField(), and.GetFilters()[1].GetField()
			require.Equal(t, a.GetField().GetMetadata(), b.GetField().GetMetadata())
			splitRange = true

			if uc := b.GetUintCond(); uc != nil && uc.Max != nil && uc.GetMax() == 0 && uc.GetMaxExclusive() {
				wrapShape = true
			}

			continue
		}

		switch c := f.GetField().GetCondition().(type) {
		case *commonpb.FieldCondition_UintCond:
			uc := c.UintCond
			uintOpenMin = uintOpenMin || uc.Min == nil
			uintOpenMax = uintOpenMax || uc.Max == nil
			uintExclMin = uintExclMin || uc.GetMinExclusive()
			uintExclMax = uintExclMax || uc.GetMaxExclusive()
			uintZero = uintZero || (uc.Max != nil && uc.GetMax() == 0)
			uintMax = uintMax || (uc.Min != nil && uc.GetMin() == math.MaxUint64)
			require.False(t, uc.Min == nil && uc.GetMinExclusive(), "an open side is never exclusive")
			require.False(t, uc.Max == nil && uc.GetMaxExclusive(), "an open side is never exclusive")
		case *commonpb.FieldCondition_IntCond:
			ic := c.IntCond
			intOpenMin = intOpenMin || ic.Min == nil
			intOpenMax = intOpenMax || ic.Max == nil
			intExclMin = intExclMin || ic.GetMinExclusive()
			intExclMax = intExclMax || ic.GetMaxExclusive()
			intMin = intMin || (ic.Max != nil && ic.GetMax() == math.MinInt64)
			intMax = intMax || (ic.Min != nil && ic.GetMin() == math.MaxInt64)
		}
	}

	for name, seen := range map[string]bool{
		"uint open min": uintOpenMin, "uint open max": uintOpenMax, "uint exclusive min": uintExclMin, "uint exclusive max": uintExclMax,
		"uint max bound 0": uintZero, "uint min bound MaxUint64": uintMax,
		"int open min": intOpenMin, "int open max": intOpenMax, "int exclusive min": intExclMin, "int exclusive max": intExclMax,
		"int max bound MinInt64": intMin, "int min bound MaxInt64": intMax,
		"split range": splitRange, "And(x >= a, x < 0) on unsigned": wrapShape,
	} {
		require.True(t, seen, name)
	}
}

// The transaction id range comes out two-sided, half-open and exclusive.
func TestGenTransactionFilterFreeIDRangeShapes(t *testing.T) {
	t.Parallel()

	var openMin, openMax, exclMin, exclMax, twoSided bool
	for range 20_000 {
		bu := genTransactionFilterFree(maxQueryGenDepth).GetBuiltinUint()
		if bu == nil {
			continue
		}

		c := bu.GetCond()
		openMin = openMin || c.Min == nil
		openMax = openMax || c.Max == nil
		exclMin = exclMin || c.GetMinExclusive()
		exclMax = exclMax || c.GetMaxExclusive()
		twoSided = twoSided || (c.Min != nil && c.Max != nil)
	}

	require.True(t, openMin && openMax && exclMin && exclMax && twoSided)
}

// And/Or carry zero to three operands.
func TestGenBooleanArity(t *testing.T) {
	t.Parallel()

	leaf := func(int) *commonpb.QueryFilter { return filterReverted(true) }
	seen := map[int]bool{}
	for range 2_000 {
		f := genBoolean(0, leaf)
		if and := f.GetAnd(); and != nil {
			seen[len(and.GetFilters())] = true
		}
		if or := f.GetOr(); or != nil {
			seen[len(or.GetFilters())] = true
		}
	}

	require.Equal(t, map[int]bool{0: true, 1: true, 2: true, 3: true}, seen)
}
