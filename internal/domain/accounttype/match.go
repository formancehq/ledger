package accounttype

import (
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// CompiledType holds an account type with its pre-parsed pattern segments,
// avoiding repeated ParsePattern calls in the hot path. Name is the
// authoritative chart key. Original is the read-only account type view (never
// the cached mutable pointer), so a compiled result cannot mutate the source
// ledger configuration through an alias.
type CompiledType struct {
	Name        string
	Segments    []PatternSegment
	Specificity int
	Original    commonpb.AccountTypeReader
}

// CompileTypes pre-parses all account types into CompiledType entries.
// Variable segments are annotated with constraints from the proto segment_types map.
// The output is sorted by name for deterministic ordering across nodes.
//
// The FSM accepts a type only when ParseType succeeds, so a type that does not
// compile is an impossible state (invariant #7): CompileTypes returns an error
// for the first such type in name order instead of dropping it, which would
// silently disable enforcement for the addresses the type was meant to cover.
func CompileTypes(types map[string]*commonpb.AccountType) ([]CompiledType, error) {
	// Sort keys for deterministic iteration order.
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}

	slices.Sort(names)

	return compileTypes(names, func(name string) (commonpb.AccountTypeReader, bool) {
		at := types[name]
		if at == nil {
			return nil, false
		}

		return at.AsReader(), true
	})
}

// CompileTypesReader pre-parses account types without cloning the owning
// ledger. It consumes the immutable LedgerInfo account-types reader directly,
// so read-only validation paths never materialise a mutable AccountTypes map.
// It fails on an invalid stored type exactly like CompileTypes.
func CompileTypesReader(types commonpb.LedgerInfo_AccountTypesMapReader) ([]CompiledType, error) {
	if types == nil {
		return nil, nil
	}

	names := make([]string, 0, types.Len())
	types.Range(func(name string, _ commonpb.AccountTypeReader) bool {
		names = append(names, name)

		return true
	})

	slices.Sort(names)

	return compileTypes(names, types.Get)
}

func compileTypes(names []string, get func(name string) (commonpb.AccountTypeReader, bool)) ([]CompiledType, error) {
	compiled := make([]CompiledType, 0, len(names))

	for _, name := range names {
		at, ok := get(name)
		if !ok || at == nil {
			return nil, fmt.Errorf("account type %q has no definition", name)
		}

		segments, err := ParseType(at)
		if err != nil {
			return nil, fmt.Errorf("account type %q (pattern %q): %w", name, at.GetPattern(), err)
		}

		compiled = append(compiled, CompiledType{
			Name:        name,
			Segments:    segments,
			Specificity: Specificity(segments),
			Original:    at,
		})
	}

	return compiled, nil
}

// FindConflict returns the first entry of chart whose pattern conflicts with
// segments (see PatternsConflict). chart is scanned in slice order; callers pass
// a name-sorted chart (CompileTypes output) so the selected conflict is
// identical on every replica (invariant #2).
func FindConflict(segments []PatternSegment, chart []CompiledType) (CompiledType, bool) {
	for _, existing := range chart {
		if PatternsConflict(segments, existing.Segments) {
			return existing, true
		}
	}

	return CompiledType{}, false
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
) commonpb.AccountTypeReader {
	var (
		best     commonpb.AccountTypeReader
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
