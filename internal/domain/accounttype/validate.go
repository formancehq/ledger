package accounttype

import (
	"fmt"
	"maps"
	"regexp"
	"slices"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ValidatePattern parses and validates a pattern string.
// Returns an error if the pattern is syntactically invalid.
func ValidatePattern(pattern string) error {
	_, err := ParsePattern(pattern)

	return err
}

// ValidateSegmentTypes checks that segment_types references valid variable names
// from the pattern and applies the constraint to each segment.
func ValidateSegmentTypes(segments []PatternSegment, segTypes map[string]*commonpb.SegmentType) error {
	return applySegmentTypes(segments, slices.Sorted(maps.Keys(segTypes)), func(name string) any {
		return segTypes[name].GetConstraint()
	})
}

// validateSegmentTypesReader is the read-only twin of ValidateSegmentTypes:
// it consumes the immutable segment-types map reader so compiled account
// types never alias the cached ledger configuration.
func validateSegmentTypesReader(segments []PatternSegment, segTypes commonpb.AccountType_SegmentTypesMapReader) error {
	names := make([]string, 0, segTypes.Len())
	segTypes.Range(func(name string, _ commonpb.SegmentTypeReader) bool {
		names = append(names, name)

		return true
	})

	slices.Sort(names)

	return applySegmentTypes(segments, names, func(name string) any {
		st, _ := segTypes.Get(name)
		if st == nil {
			return nil
		}

		return st.GetConstraint()
	})
}

// applySegmentTypes validates declared segment types against the pattern's
// variable names and installs the corresponding per-segment matchers.
// Iteration order is caller-provided (sorted) so error selection is
// deterministic across replicas (invariant #2). get returns the constraint
// for a declared variable name, or nil when the stored type carries none.
func applySegmentTypes(segments []PatternSegment, names []string, get func(name string) any) error {
	vars := make(map[string]int, len(segments))

	for i := range segments {
		if segments[i].Kind == SegmentVariable {
			vars[segments[i].Value] = i
		}
	}

	for _, name := range names {
		idx, ok := vars[name]
		if !ok {
			return fmt.Errorf("segment_types references unknown variable %q", name)
		}

		matcher, err := buildMatcher(get(name))
		if err != nil {
			return fmt.Errorf("segment_types variable %q: %w", name, err)
		}

		if matcher != nil {
			segments[idx].Matcher = matcher
		}
	}

	return nil
}

func buildMatcher(constraint any) (SegmentMatcher, error) {
	if constraint == nil {
		return nil, nil
	}

	switch c := constraint.(type) {
	case *commonpb.SegmentType_Regex:
		compiled, err := regexp.Compile("^(?:" + c.Regex + ")$")
		if err != nil {
			return nil, fmt.Errorf("invalid regex %q: %w", c.Regex, err)
		}

		return compiled.MatchString, nil

	case *commonpb.SegmentType_Uuid:
		return matchUUID, nil

	case *commonpb.SegmentType_Uint64:
		return matchUint64, nil

	case *commonpb.SegmentType_Bytes:
		return matchHexBytes, nil

	default:
		return nil, nil
	}
}

func isHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}

// matchUUID validates RFC 4122 format: 8-4-4-4-12 lowercase hex with dashes.
func matchUUID(s string) bool {
	if len(s) != 36 {
		return false
	}

	for i := range 36 {
		c := s[i]

		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !isHexDigit(c) {
				return false
			}
		}
	}

	return true
}

// matchUint64 validates a non-empty decimal digit string.
func matchUint64(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

// matchHexBytes validates even-length lowercase hex.
func matchHexBytes(s string) bool {
	if len(s) == 0 || len(s)%2 != 0 {
		return false
	}

	for i := range len(s) {
		if !isHexDigit(s[i]) {
			return false
		}
	}

	return true
}
