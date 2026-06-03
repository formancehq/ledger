package accounttype

import (
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// CompiledType holds an account type with its pre-parsed pattern segments,
// avoiding repeated ParsePattern calls in the hot path.
type CompiledType struct {
	Segments    []PatternSegment
	Specificity int
	Original    *commonpb.AccountType
}

// CompileTypes pre-parses all account types into CompiledType entries.
// Types with invalid patterns are silently skipped.
// Variable segments are annotated with constraints from the proto segment_types map.
// The output is sorted by name for deterministic ordering across nodes.
func CompileTypes(types map[string]*commonpb.AccountType) []CompiledType {
	// Sort keys for deterministic iteration order.
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}

	slices.Sort(names)

	compiled := make([]CompiledType, 0, len(types))

	for _, name := range names {
		at := types[name]
		segments, err := ParsePattern(at.GetPattern())
		if err != nil {
			continue
		}

		if err := ValidateSegmentTypes(segments, at.GetSegmentTypes()); err != nil {
			continue
		}

		compiled = append(compiled, CompiledType{
			Segments:    segments,
			Specificity: Specificity(segments),
			Original:    at,
		})
	}

	return compiled
}

// PatternsConflict returns true if two parsed patterns can match the same
// address with the same specificity. Two patterns conflict when they have the
// same number of segments, the same specificity, and at every position the
// segments are compatible (both fixed with the same value, or at least one is
// variable).
func PatternsConflict(a, b []PatternSegment) bool {
	if len(a) != len(b) {
		return false
	}

	if Specificity(a) != Specificity(b) {
		return false
	}

	for i := range a {
		// Two fixed segments must match literally to overlap.
		if a[i].Kind == SegmentFixed && b[i].Kind == SegmentFixed {
			if a[i].Value != b[i].Value {
				return false
			}
		}
		// If at least one is variable, any value could match — compatible.
	}

	return true
}

// FindMatchingType finds the best matching account type for an address using
// longest-match (highest specificity). Returns nil if no type matches.
func FindMatchingType(
	address string,
	compiled []CompiledType,
) *commonpb.AccountType {
	var (
		best     *commonpb.AccountType
		bestSpec = -1
		bestLen  = 0
	)

	for i := range compiled {
		ct := &compiled[i]

		if _, ok := MatchAddress(address, ct.Segments); !ok {
			continue
		}

		if ct.Specificity > bestSpec || (ct.Specificity == bestSpec && len(ct.Segments) < bestLen) {
			best = ct.Original
			bestSpec = ct.Specificity
			bestLen = len(ct.Segments)
		}
	}

	return best
}
