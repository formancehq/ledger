package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestMergeFieldRanges(t *testing.T) {
	t.Parallel()

	t.Run("single filter passes through", func(t *testing.T) {
		t.Parallel()

		in := []*commonpb.QueryFilter{intRange("a", new(int64(10)), false, nil, false)}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		assert.Same(t, in[0], got[0])
	})

	t.Run("complementary ranges on same field merge to bounded", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND a < 20  -> a in [10, 20)
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			intRange("a", nil, false, new(int64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		ic := got[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Equal(t, int64(10), ic.GetMin())
		assert.Equal(t, int64(19), ic.GetMax())
		assert.False(t, ic.GetMinExclusive())
		assert.False(t, ic.GetMaxExclusive())
	})

	t.Run("inclusive max normalizes the same way", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND a <= 19  -> a in [10, 19]
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			intRange("a", nil, false, new(int64(19)), false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		ic := got[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, int64(10), ic.GetMin())
		assert.Equal(t, int64(19), ic.GetMax())
	})

	t.Run("exclusive bounds round inward", func(t *testing.T) {
		t.Parallel()

		// a > 10 AND a < 20  -> a in [11, 19]
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), true, nil, false),
			intRange("a", nil, false, new(int64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		ic := got[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, int64(11), ic.GetMin())
		assert.Equal(t, int64(19), ic.GetMax())
	})

	t.Run("stricter lower bound wins", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND a >= 20  -> a >= 20
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			intRange("a", new(int64(20)), false, nil, false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		ic := got[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		assert.Equal(t, int64(20), ic.GetMin())
		assert.Nil(t, ic.Max)
	})

	t.Run("contradiction returns empty range", func(t *testing.T) {
		t.Parallel()

		// a >= 20 AND a < 10  -> impossible interval
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(20)), false, nil, false),
			intRange("a", nil, false, new(int64(10)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		ic := got[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		require.NotNil(t, ic.Min)
		require.NotNil(t, ic.Max)
		assert.Greater(t, ic.GetMin(), ic.GetMax(),
			"contradictory bounds should survive — downstream range scan returns no rows")
	})

	t.Run("different fields unchanged", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND b < 20  -> two filters, unchanged
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			intRange("b", nil, false, new(int64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, in[0], got[0])
		assert.Same(t, in[1], got[1])
	})

	t.Run("merge preserves order and foreign filters", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND c == "x" AND a < 20  -> [a in [10,20), c == "x"]
		strFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "c"},
					Condition: &commonpb.FieldCondition_StringCond{
						StringCond: &commonpb.StringCondition{
							Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "x"},
						},
					},
				},
			},
		}

		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			strFilter,
			intRange("a", nil, false, new(int64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)

		// Merged `a` lands at index 0 (replaces first occurrence).
		aIC := got[0].GetField().GetIntCond()
		require.NotNil(t, aIC)
		assert.Equal(t, int64(10), aIC.GetMin())
		assert.Equal(t, int64(19), aIC.GetMax())

		// String filter keeps its relative position.
		assert.Same(t, strFilter, got[1])
	})

	t.Run("equality is not merged with range", func(t *testing.T) {
		t.Parallel()

		// a == 5 AND a >= 3  -> unchanged (equality already optimal)
		five := int64(5)
		eqFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "a"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Min: &five, Max: &five},
					},
				},
			},
		}
		in := []*commonpb.QueryFilter{
			eqFilter,
			intRange("a", new(int64(3)), false, nil, false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, in[0], got[0])
		assert.Same(t, in[1], got[1])
	})

	t.Run("parameterized bounds are not merged", func(t *testing.T) {
		t.Parallel()

		paramFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "a"},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{ParamMin: "lo"},
					},
				},
			},
		}
		in := []*commonpb.QueryFilter{
			paramFilter,
			intRange("a", nil, false, new(int64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, in[0], got[0])
		assert.Same(t, in[1], got[1])
	})

	t.Run("three ranges on the same field all collapse", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND a < 100 AND a >= 20  -> [20, 100)
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			intRange("a", nil, false, new(int64(100)), true),
			intRange("a", new(int64(20)), false, nil, false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		ic := got[0].GetField().GetIntCond()
		require.NotNil(t, ic)
		assert.Equal(t, int64(20), ic.GetMin())
		assert.Equal(t, int64(99), ic.GetMax())
	})
}

// intRange builds a metadata IntCondition filter on `field`. Pass nil for an
// absent bound; (value, exclusive) for a present one.
func intRange(field string, low *int64, lowExcl bool, high *int64, highExcl bool) *commonpb.QueryFilter {
	ic := &commonpb.IntCondition{}
	if low != nil {
		ic.Min = low
		ic.MinExclusive = lowExcl
	}
	if high != nil {
		ic.Max = high
		ic.MaxExclusive = highExcl
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: field},
				Condition: &commonpb.FieldCondition_IntCond{IntCond: ic},
			},
		},
	}
}

func TestMergeFieldRanges_Uint(t *testing.T) {
	t.Parallel()

	t.Run("complementary uint ranges merge to bounded", func(t *testing.T) {
		t.Parallel()

		// a >= 10 AND a < 20  -> a in [10, 19]
		in := []*commonpb.QueryFilter{
			uintRange("a", new(uint64(10)), false, nil, false),
			uintRange("a", nil, false, new(uint64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		uc := got[0].GetField().GetUintCond()
		require.NotNil(t, uc)
		require.NotNil(t, uc.Min)
		require.NotNil(t, uc.Max)
		assert.Equal(t, uint64(10), uc.GetMin())
		assert.Equal(t, uint64(19), uc.GetMax())
		assert.False(t, uc.GetMinExclusive())
		assert.False(t, uc.GetMaxExclusive())
	})

	t.Run("uint exclusive bounds round inward", func(t *testing.T) {
		t.Parallel()

		// a > 10 AND a < 20  -> a in [11, 19]
		in := []*commonpb.QueryFilter{
			uintRange("a", new(uint64(10)), true, nil, false),
			uintRange("a", nil, false, new(uint64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		uc := got[0].GetField().GetUintCond()
		require.NotNil(t, uc)
		assert.Equal(t, uint64(11), uc.GetMin())
		assert.Equal(t, uint64(19), uc.GetMax())
	})

	t.Run("stricter uint upper bound wins", func(t *testing.T) {
		t.Parallel()

		// a <= 50 AND a <= 20  -> a <= 20
		in := []*commonpb.QueryFilter{
			uintRange("a", nil, false, new(uint64(50)), false),
			uintRange("a", nil, false, new(uint64(20)), false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		uc := got[0].GetField().GetUintCond()
		require.NotNil(t, uc)
		assert.Nil(t, uc.Min)
		require.NotNil(t, uc.Max)
		assert.Equal(t, uint64(20), uc.GetMax())
	})

	t.Run("uint contradiction returns empty range", func(t *testing.T) {
		t.Parallel()

		// a >= 20 AND a < 10
		in := []*commonpb.QueryFilter{
			uintRange("a", new(uint64(20)), false, nil, false),
			uintRange("a", nil, false, new(uint64(10)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 1)
		uc := got[0].GetField().GetUintCond()
		require.NotNil(t, uc)
		assert.Greater(t, uc.GetMin(), uc.GetMax())
	})

	t.Run("uint with both bounds already set is not merged", func(t *testing.T) {
		t.Parallel()

		// Fully bounded (a in [10, 20]) AND additional `a >= 5` — neither side
		// is a pure half-range, so the merger leaves them alone (intersection
		// is correct but the input isn't a merge candidate).
		boundedFilter := uintRange("a", new(uint64(10)), false, new(uint64(20)), false)
		in := []*commonpb.QueryFilter{
			boundedFilter,
			uintRange("a", new(uint64(5)), false, nil, false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, in[0], got[0])
		assert.Same(t, in[1], got[1])
	})

	t.Run("uint parameterized bounds are not merged", func(t *testing.T) {
		t.Parallel()

		paramFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{Metadata: "a"},
					Condition: &commonpb.FieldCondition_UintCond{
						UintCond: &commonpb.UintCondition{ParamMin: "lo"},
					},
				},
			},
		}
		in := []*commonpb.QueryFilter{
			paramFilter,
			uintRange("a", nil, false, new(uint64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, in[0], got[0])
		assert.Same(t, in[1], got[1])
	})

	t.Run("int and uint on same metadata key do not merge", func(t *testing.T) {
		t.Parallel()

		// IntCond and UintCond would never coexist for the same field in
		// practice (schema fixes the type), but the merger keys by Int/Uint
		// kind so the two pass through unchanged if they ever did.
		in := []*commonpb.QueryFilter{
			intRange("a", new(int64(10)), false, nil, false),
			uintRange("a", nil, false, new(uint64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
	})

	t.Run("non-field filter passes through unchanged", func(t *testing.T) {
		t.Parallel()

		// AddressMatch isn't a FieldCondition, so the merger lets it through
		// without inspection — that's the address-filter pass-through path.
		addr := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: "users:"},
				},
			},
		}
		in := []*commonpb.QueryFilter{
			addr,
			intRange("a", new(int64(10)), false, nil, false),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, addr, got[0])
	})

	t.Run("metadata key without name is not merged", func(t *testing.T) {
		t.Parallel()

		// Builtin-like FieldCondition with empty Metadata key — defensive
		// branch ensuring mergeableFieldKey rejects entries without a key.
		fc := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field: &commonpb.FieldRef{},
					Condition: &commonpb.FieldCondition_IntCond{
						IntCond: &commonpb.IntCondition{Min: new(int64(10))},
					},
				},
			},
		}
		in := []*commonpb.QueryFilter{
			fc,
			intRange("a", nil, false, new(int64(20)), true),
		}
		got := mergeFieldRanges(in)

		require.Len(t, got, 2)
		assert.Same(t, fc, got[0])
	})

	t.Run("nil filter list returns nil", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, mergeFieldRanges(nil))
	})
}

// uintRange builds a metadata UintCondition filter on `field`. Mirror of
// intRange for the unsigned path.
func uintRange(field string, low *uint64, lowExcl bool, high *uint64, highExcl bool) *commonpb.QueryFilter {
	uc := &commonpb.UintCondition{}
	if low != nil {
		uc.Min = low
		uc.MinExclusive = lowExcl
	}
	if high != nil {
		uc.Max = high
		uc.MaxExclusive = highExcl
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: field},
				Condition: &commonpb.FieldCondition_UintCond{UintCond: uc},
			},
		},
	}
}
