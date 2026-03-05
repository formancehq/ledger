package accounttype

import (
	"fmt"
	"sort"
)

// ValidatePattern parses and validates a pattern string.
// Returns an error if the pattern is syntactically invalid.
func ValidatePattern(pattern string) error {
	_, err := ParsePattern(pattern)
	return err
}

// ValidateMigration checks that a migration from source to target pattern is valid.
// All variables in the target pattern must appear in the source pattern (subset).
func ValidateMigration(source, target []PatternSegment) error {
	sourceVars := make(map[string]struct{})
	for _, seg := range source {
		if seg.Kind == SegmentVariable {
			sourceVars[seg.Value] = struct{}{}
		}
	}

	for _, seg := range target {
		if seg.Kind == SegmentVariable {
			if _, ok := sourceVars[seg.Value]; !ok {
				return fmt.Errorf("target variable %q not found in source pattern", seg.Value)
			}
		}
	}

	return nil
}

// DetectOverlaps checks if a new pattern could match the same addresses as
// any existing pattern. Returns the names of overlapping patterns.
// This is advisory — longest-match handles overlaps correctly at runtime.
func DetectOverlaps(newSegments []PatternSegment, existing map[string][]PatternSegment) []string {
	var overlaps []string
	for name, segs := range existing {
		if patternsOverlap(newSegments, segs) {
			overlaps = append(overlaps, name)
		}
	}
	sort.Strings(overlaps)
	return overlaps
}

// patternsOverlap returns true if two patterns could match the same address.
// Two patterns overlap if they have the same number of segments and each
// position is either both fixed with the same value, or at least one is variable.
func patternsOverlap(a, b []PatternSegment) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Kind == SegmentFixed && b[i].Kind == SegmentFixed {
			if a[i].Value != b[i].Value {
				return false
			}
		}
		// If either is variable, there exists some value that could match both.
	}
	return true
}
