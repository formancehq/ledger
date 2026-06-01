package accounttype

import (
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
func CompileTypes(types map[string]*commonpb.AccountType) []CompiledType {
	compiled := make([]CompiledType, 0, len(types))

	for _, at := range types {
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
