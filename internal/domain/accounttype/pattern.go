package accounttype

import (
	"errors"
	"fmt"
	"strings"
)

// SegmentKind distinguishes fixed (literal) from variable (capture) segments.
type SegmentKind int

const (
	SegmentFixed SegmentKind = iota
	SegmentVariable
)

// SegmentMatcher validates a variable segment value.
type SegmentMatcher func(value string) bool

// PatternSegment represents one colon-separated segment in an account type pattern.
type PatternSegment struct {
	Kind    SegmentKind
	Value   string         // literal for Fixed, variable name for Variable
	Matcher SegmentMatcher // constraint for Variable segments (nil = match any non-empty string)
}

// ParsePattern parses a pattern string like "users:{id}:checking" into segments.
// Variable segments use the syntax {name}. Constraints are declared separately
// via segment_types on the AccountType proto.
func ParsePattern(pattern string) ([]PatternSegment, error) {
	if pattern == "" {
		return nil, errors.New("pattern must not be empty")
	}

	parts := strings.Split(pattern, ":")
	segments := make([]PatternSegment, 0, len(parts))
	seenVars := make(map[string]struct{})

	for _, part := range parts {
		if part == "" {
			return nil, errors.New("pattern contains empty segment")
		}

		seg, err := parseSegment(part)
		if err != nil {
			return nil, err
		}

		if seg.Kind == SegmentVariable {
			if _, exists := seenVars[seg.Value]; exists {
				return nil, fmt.Errorf("duplicate variable name %q in pattern", seg.Value)
			}
			seenVars[seg.Value] = struct{}{}

			if len(seenVars) > maxBindings {
				return nil, fmt.Errorf("pattern has more than %d variables", maxBindings)
			}
		}

		segments = append(segments, seg)
	}

	return segments, nil
}

// parseSegment parses a single segment: either a literal or {name}.
func parseSegment(s string) (PatternSegment, error) {
	if !strings.HasPrefix(s, "{") {
		if !isValidSegmentName(s) {
			return PatternSegment{}, fmt.Errorf("invalid fixed segment %q: must match [a-zA-Z0-9_-]+", s)
		}

		return PatternSegment{Kind: SegmentFixed, Value: s}, nil
	}

	if !strings.HasSuffix(s, "}") {
		return PatternSegment{}, fmt.Errorf("unclosed variable in segment %q", s)
	}

	name := s[1 : len(s)-1]
	if name == "" {
		return PatternSegment{}, fmt.Errorf("empty variable name in segment %q", s)
	}

	if !isValidVariableName(name) {
		return PatternSegment{}, fmt.Errorf("invalid variable name %q: must match [a-zA-Z_][a-zA-Z0-9_]*", name)
	}

	return PatternSegment{Kind: SegmentVariable, Value: name}, nil
}

func isValidSegmentName(s string) bool {
	if len(s) == 0 {
		return false
	}

	for i := range len(s) {
		c := s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
			return false
		}
	}

	return true
}

func isValidVariableName(s string) bool {
	if len(s) == 0 {
		return false
	}

	c := s[0]
	if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
		return false
	}

	for i := 1; i < len(s); i++ {
		c = s[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}

	return true
}

// MatchAddress matches an account address against parsed pattern segments.
// Returns variable bindings and whether the address matched. Fully zero-allocation.
func MatchAddress(address string, segments []PatternSegment) (Bindings, bool) {
	var bindings Bindings
	remaining := address
	last := len(segments) - 1

	for i := range segments {
		seg := &segments[i]

		var part string
		colonIdx := strings.IndexByte(remaining, ':')

		if i < last {
			if colonIdx < 0 {
				return bindings, false
			}
			part = remaining[:colonIdx]
			remaining = remaining[colonIdx+1:]
		} else {
			if colonIdx >= 0 {
				return bindings, false
			}
			part = remaining
		}

		switch seg.Kind {
		case SegmentFixed:
			if part != seg.Value {
				return bindings, false
			}
		case SegmentVariable:
			if len(part) == 0 {
				return bindings, false
			}
			if seg.Matcher != nil && !seg.Matcher(part) {
				return bindings, false
			}
			bindings.set(seg.Value, part)
		}
	}

	return bindings, true
}

// Specificity returns the number of fixed segments in a pattern.
// Higher specificity means a more specific match.
func Specificity(segments []PatternSegment) int {
	count := 0
	for _, seg := range segments {
		if seg.Kind == SegmentFixed {
			count++
		}
	}

	return count
}
