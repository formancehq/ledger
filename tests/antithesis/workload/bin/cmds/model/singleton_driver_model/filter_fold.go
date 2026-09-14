package main

import (
	"cmp"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// filterFold reduces a QueryFilter tree bottom-up. foldFilter owns the
// And/Or/Not traversal, so every walk over a filter — index needs, validity,
// matching, rendering — shares one combinator step and differs only in how it
// judges a leaf. leaf receives every non-combinator node, the nil filter
// included, so each walk decides in one place what "no condition" means.
type filterFold[T any] struct {
	and  func(children []T) T
	or   func(children []T) T
	not  func(child T) T
	leaf func(f *commonpb.QueryFilter) T
}

func foldFilter[T any](f *commonpb.QueryFilter, fold filterFold[T]) T {
	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		return fold.and(foldChildren(x.And.GetFilters(), fold))
	case *commonpb.QueryFilter_Or:
		return fold.or(foldChildren(x.Or.GetFilters(), fold))
	case *commonpb.QueryFilter_Not:
		return fold.not(foldFilter(x.Not.GetFilter(), fold))
	default:
		return fold.leaf(f)
	}
}

func foldChildren[T any](children []*commonpb.QueryFilter, fold filterFold[T]) []T {
	out := make([]T, 0, len(children))
	for _, child := range children {
		out = append(out, foldFilter(child, fold))
	}

	return out
}

// anyLeaf reports whether pred holds for some leaf of f; combinators only
// propagate their children's verdicts.
func anyLeaf(f *commonpb.QueryFilter, pred func(*commonpb.QueryFilter) bool) bool {
	return foldFilter(f, filterFold[bool]{and: anyOf, or: anyOf, not: identity[bool], leaf: pred})
}

// visitLeaves calls visit on every leaf of f in tree order.
func visitLeaves(f *commonpb.QueryFilter, visit func(*commonpb.QueryFilter)) {
	foldFilter(f, filterFold[struct{}]{
		and: func([]struct{}) struct{} { return struct{}{} },
		or:  func([]struct{}) struct{} { return struct{}{} },
		not: identity[struct{}],
		leaf: func(leaf *commonpb.QueryFilter) struct{} {
			visit(leaf)

			return struct{}{}
		},
	})
}

func anyOf(vs []bool) bool {
	for _, v := range vs {
		if v {
			return true
		}
	}

	return false
}

func allOf(vs []bool) bool {
	for _, v := range vs {
		if !v {
			return false
		}
	}

	return true
}

func negate(v bool) bool { return !v }

func identity[T any](v T) T { return v }

// kleene is a three-valued verdict: known=false means the answer hinges on a
// server stamp the model has not learned yet. Booleans propagate unknowns
// Kleene-style — a decided AND/OR short-circuits, an undecided one stays
// unknown.
type kleene struct {
	match, known bool
}

func kleeneAnd(children []kleene) kleene {
	known := true
	for _, c := range children {
		if c.known && !c.match {
			return kleene{match: false, known: true}
		}

		known = known && c.known
	}

	return kleene{match: true, known: known}
}

func kleeneOr(children []kleene) kleene {
	known := true
	for _, c := range children {
		if c.known && c.match {
			return kleene{match: true, known: true}
		}

		known = known && c.known
	}

	return kleene{match: false, known: known}
}

func kleeneNot(c kleene) kleene { return kleene{match: !c.match, known: c.known} }

// withinBounds reports whether v lies within the bounds an Int/UintCondition
// carries — mirroring resolveUintBounds: each side honors its exclusive flag
// and an absent bound is open on that side.
func withinBounds[T cmp.Ordered](v T, lo *T, loExclusive bool, hi *T, hiExclusive bool) bool {
	if lo != nil && (v < *lo || (loExclusive && v == *lo)) {
		return false
	}

	if hi != nil && (v > *hi || (hiExclusive && v == *hi)) {
		return false
	}

	return true
}

func matchUintBounds(cond *commonpb.UintCondition, v uint64) bool {
	return withinBounds(v, cond.Min, cond.GetMinExclusive(), cond.Max, cond.GetMaxExclusive())
}

func matchIntBounds(cond *commonpb.IntCondition, v int64) bool {
	return withinBounds(v, cond.Min, cond.GetMinExclusive(), cond.Max, cond.GetMaxExclusive())
}

// renderBound renders one side of a bound for filter descriptions: "_" when
// open, the value otherwise, an exclusive bound parenthesized on its outer
// side — "(3" for a lower bound, "7)" for an upper one.
func renderBound[T cmp.Ordered](v *T, exclusive, upper bool) string {
	if v == nil {
		return "_"
	}

	s := fmt.Sprint(*v)

	switch {
	case exclusive && upper:
		return s + ")"
	case exclusive:
		return "(" + s
	default:
		return s
	}
}
