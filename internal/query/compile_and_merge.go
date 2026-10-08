package query

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// mergeFieldRanges coalesces multiple IntCondition / UintCondition predicates
// on the same metadata field within an AND into a single bounded condition.
// Conditions on unique fields and non-range filters pass through unchanged.
// Order is preserved for unchanged entries; merged entries replace the first
// occurrence of the field and later duplicates are dropped.
//
// This is a safety-net for the `field >= X AND field < Y` idiom, which would
// otherwise compile to two unbounded half-range scans that each materialize
// the matching half of the index before intersection. After merging the same
// query becomes a single bounded range scan in compileIntCondition, and when
// bounds collapse to one value the equality fast path (PrefixIterator) kicks
// in for free.
//
// Conditions with parameterized bounds (ParamMin/ParamMax) are not merged —
// runtime resolution can move the bounds in ways the planner can't predict
// at compile time. Mixed equality + range on the same field is also passed
// through (equality is already optimal and won't benefit from intersection
// with a wider range).
func mergeFieldRanges(filters []*ledgerpb.QueryFilter) []*ledgerpb.QueryFilter {
	if len(filters) < 2 {
		return filters
	}

	// Index of the entry holding the merged condition for each field key.
	// Negative means "no mergeable condition seen yet for this key".
	firstIdx := make(map[string]int, len(filters))
	merged := make([]*ledgerpb.QueryFilter, 0, len(filters))

	for _, f := range filters {
		key, kind, ok := mergeableFieldKey(f)
		if !ok {
			merged = append(merged, f)

			continue
		}

		prev, seen := firstIdx[key]
		if !seen {
			firstIdx[key] = len(merged)
			merged = append(merged, f)

			continue
		}

		combined, didMerge := mergeTwo(merged[prev], f, kind)
		if !didMerge {
			merged = append(merged, f)

			continue
		}

		merged[prev] = combined
	}

	return merged
}

// fieldKind tags which numeric branch of FieldCondition a filter occupies, so
// we don't try to intersect an IntCondition with a UintCondition (different
// proto types, never set on the same field in practice).
type fieldKind int

const (
	kindInt fieldKind = iota + 1
	kindUint
)

// mergeableFieldKey returns the dedup key for a filter when it is a
// metadata FieldCondition with a numeric condition that the planner can fold.
// Equality + range mixes are intentionally treated as non-mergeable: equality
// already takes the streaming PrefixIterator fast path, intersecting with a
// wider range would only add work.
func mergeableFieldKey(f *ledgerpb.QueryFilter) (string, fieldKind, bool) {
	fc := f.GetField()
	if fc == nil {
		return "", 0, false
	}

	metaKey := fc.GetField().GetMetadata()
	if metaKey == "" {
		return "", 0, false
	}

	switch cond := fc.GetCondition().(type) {
	case *ledgerpb.FieldCondition_IntCond:
		if !isPureRange(cond.IntCond) {
			return "", 0, false
		}

		return "int:" + metaKey, kindInt, true
	case *ledgerpb.FieldCondition_UintCond:
		if !isUintPureRange(cond.UintCond) {
			return "", 0, false
		}

		return "uint:" + metaKey, kindUint, true
	default:
		return "", 0, false
	}
}

// isPureRange returns true when the IntCondition has at least one bound, no
// equality form (min == max), and no parameterized bound. These are exactly
// the conditions the merger will combine.
func isPureRange(ic *ledgerpb.IntCondition) bool {
	if ic == nil {
		return false
	}
	if ic.GetParamMin() != "" || ic.GetParamMax() != "" {
		return false
	}
	if ic.Min != nil && ic.Max != nil {
		// Already bounded on both sides — either equality (== X) or already a
		// `between`. Equality stays untouched; a fully-formed range needs no
		// further merging within the same AND clause.
		return false
	}

	return ic.Min != nil || ic.Max != nil
}

func isUintPureRange(uc *ledgerpb.UintCondition) bool {
	if uc == nil {
		return false
	}
	if uc.GetParamMin() != "" || uc.GetParamMax() != "" {
		return false
	}
	if uc.Min != nil && uc.Max != nil {
		return false
	}

	return uc.Min != nil || uc.Max != nil
}

// mergeTwo intersects two pure-range field conditions on the same metadata
// field. Returns (combined, true) on success, or (nil, false) when the inputs
// can't be merged (different proto shapes). Bound values and exclusivity are
// preserved so the overflow-aware downstream resolver can detect empty ranges
// at the integer extrema.
func mergeTwo(a, b *ledgerpb.QueryFilter, kind fieldKind) (*ledgerpb.QueryFilter, bool) {
	field := a.GetField().GetField()

	switch kind {
	case kindInt:
		ac := a.GetField().GetIntCond()
		bc := b.GetField().GetIntCond()
		if ac == nil || bc == nil {
			return nil, false
		}

		return wrapIntFieldCondition(field, intersectInt(ac, bc)), true
	case kindUint:
		ac := a.GetField().GetUintCond()
		bc := b.GetField().GetUintCond()
		if ac == nil || bc == nil {
			return nil, false
		}

		return wrapUintFieldCondition(field, intersectUint(ac, bc)), true
	}

	return nil, false
}

// intersectInt picks the stricter lower and upper bound from a and b. At least
// one of the inputs always has a Min, and at least one always has a Max, but
// neither is required on both sides — that's the whole point of merging
// half-ranges together.
func intersectInt(a, b *ledgerpb.IntCondition) *ledgerpb.IntCondition {
	out := &ledgerpb.IntCondition{}

	lowA, hasLowA := intMin(a)
	lowB, hasLowB := intMin(b)
	switch {
	case hasLowA && hasLowB:
		if lowA > lowB || lowA == lowB && a.GetMinExclusive() {
			out.Min = &lowA
			out.MinExclusive = a.GetMinExclusive()
		} else {
			out.Min = &lowB
			out.MinExclusive = b.GetMinExclusive()
		}
	case hasLowA:
		out.Min = &lowA
		out.MinExclusive = a.GetMinExclusive()
	case hasLowB:
		out.Min = &lowB
		out.MinExclusive = b.GetMinExclusive()
	}

	highA, hasHighA := intMax(a)
	highB, hasHighB := intMax(b)
	switch {
	case hasHighA && hasHighB:
		if highA < highB || highA == highB && a.GetMaxExclusive() {
			out.Max = &highA
			out.MaxExclusive = a.GetMaxExclusive()
		} else {
			out.Max = &highB
			out.MaxExclusive = b.GetMaxExclusive()
		}
	case hasHighA:
		out.Max = &highA
		out.MaxExclusive = a.GetMaxExclusive()
	case hasHighB:
		out.Max = &highB
		out.MaxExclusive = b.GetMaxExclusive()
	}

	return out
}

func intMin(ic *ledgerpb.IntCondition) (int64, bool) {
	if ic.Min == nil {
		return 0, false
	}

	return ic.GetMin(), true
}

func intMax(ic *ledgerpb.IntCondition) (int64, bool) {
	if ic.Max == nil {
		return 0, false
	}

	return ic.GetMax(), true
}

func intersectUint(a, b *ledgerpb.UintCondition) *ledgerpb.UintCondition {
	out := &ledgerpb.UintCondition{}

	lowA, hasLowA := uintMin(a)
	lowB, hasLowB := uintMin(b)
	switch {
	case hasLowA && hasLowB:
		if lowA > lowB || lowA == lowB && a.GetMinExclusive() {
			out.Min = &lowA
			out.MinExclusive = a.GetMinExclusive()
		} else {
			out.Min = &lowB
			out.MinExclusive = b.GetMinExclusive()
		}
	case hasLowA:
		out.Min = &lowA
		out.MinExclusive = a.GetMinExclusive()
	case hasLowB:
		out.Min = &lowB
		out.MinExclusive = b.GetMinExclusive()
	}

	highA, hasHighA := uintMax(a)
	highB, hasHighB := uintMax(b)
	switch {
	case hasHighA && hasHighB:
		if highA < highB || highA == highB && a.GetMaxExclusive() {
			out.Max = &highA
			out.MaxExclusive = a.GetMaxExclusive()
		} else {
			out.Max = &highB
			out.MaxExclusive = b.GetMaxExclusive()
		}
	case hasHighA:
		out.Max = &highA
		out.MaxExclusive = a.GetMaxExclusive()
	case hasHighB:
		out.Max = &highB
		out.MaxExclusive = b.GetMaxExclusive()
	}

	return out
}

func uintMin(uc *ledgerpb.UintCondition) (uint64, bool) {
	if uc.Min == nil {
		return 0, false
	}

	return uc.GetMin(), true
}

func uintMax(uc *ledgerpb.UintCondition) (uint64, bool) {
	if uc.Max == nil {
		return 0, false
	}

	return uc.GetMax(), true
}

func wrapIntFieldCondition(field *ledgerpb.FieldRef, ic *ledgerpb.IntCondition) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field:     field,
				Condition: &ledgerpb.FieldCondition_IntCond{IntCond: ic},
			},
		},
	}
}

func wrapUintFieldCondition(field *ledgerpb.FieldRef, uc *ledgerpb.UintCondition) *ledgerpb.QueryFilter {
	return &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Field{
			Field: &ledgerpb.FieldCondition{
				Field:     field,
				Condition: &ledgerpb.FieldCondition_UintCond{UintCond: uc},
			},
		},
	}
}
