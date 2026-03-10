package accounttype

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// SegmentKind distinguishes fixed (literal) from variable (capture) segments.
type SegmentKind int

const (
	SegmentFixed SegmentKind = iota
	SegmentVariable
)

// PatternSegment represents one colon-separated segment in an account type pattern.
type PatternSegment struct {
	Kind    SegmentKind
	Value   string // literal for Fixed, variable name for Variable
	Pattern string // regex constraint for Variable (empty = match any non-empty)
}

// segmentNameRe validates fixed segment literals.
var segmentNameRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// variableNameRe validates variable names inside {name} or {name:regex}.
var variableNameRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// ParsePattern parses a pattern string like "users:{id}:checking" into segments.
// Colons inside braces are part of the variable regex, not segment delimiters.
func ParsePattern(pattern string) ([]PatternSegment, error) {
	if pattern == "" {
		return nil, errors.New("pattern must not be empty")
	}

	parts := splitPatternSegments(pattern)
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
		}

		segments = append(segments, seg)
	}

	return segments, nil
}

// splitPatternSegments splits a pattern on ':' while respecting '{...}' braces.
// Colons inside braces are treated as part of the variable regex.
func splitPatternSegments(pattern string) []string {
	var parts []string
	depth := 0
	start := 0
	for i, ch := range pattern {
		switch ch {
		case '{':
			depth++
		case '}':
			if depth > 0 {
				depth--
			}
		case ':':
			if depth == 0 {
				parts = append(parts, pattern[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, pattern[start:])

	return parts
}

// parseSegment parses a single segment: either a literal or {name} or {name:regex}.
func parseSegment(s string) (PatternSegment, error) {
	if !strings.HasPrefix(s, "{") {
		if !segmentNameRe.MatchString(s) {
			return PatternSegment{}, fmt.Errorf("invalid fixed segment %q: must match [a-zA-Z0-9_-]+", s)
		}

		return PatternSegment{Kind: SegmentFixed, Value: s}, nil
	}

	if !strings.HasSuffix(s, "}") {
		return PatternSegment{}, fmt.Errorf("unclosed variable in segment %q", s)
	}

	inner := s[1 : len(s)-1]
	if inner == "" {
		return PatternSegment{}, fmt.Errorf("empty variable name in segment %q", s)
	}

	// Split on first colon to separate name from optional regex.
	name, regex, _ := strings.Cut(inner, ":")

	if !variableNameRe.MatchString(name) {
		return PatternSegment{}, fmt.Errorf("invalid variable name %q: must match [a-zA-Z_][a-zA-Z0-9_]*", name)
	}

	if regex != "" {
		if _, err := regexp.Compile(regex); err != nil {
			return PatternSegment{}, fmt.Errorf("invalid regex in variable %q: %w", name, err)
		}
	}

	return PatternSegment{Kind: SegmentVariable, Value: name, Pattern: regex}, nil
}

// MatchAddress matches an account address against parsed pattern segments.
// Returns variable bindings and whether the address matched.
func MatchAddress(address string, segments []PatternSegment) (map[string]string, bool) {
	parts := strings.Split(address, ":")
	if len(parts) != len(segments) {
		return nil, false
	}

	bindings := make(map[string]string, len(segments))
	for i, seg := range segments {
		part := parts[i]
		switch seg.Kind {
		case SegmentFixed:
			if part != seg.Value {
				return nil, false
			}
		case SegmentVariable:
			if part == "" {
				return nil, false
			}
			if seg.Pattern != "" {
				matched, err := regexp.MatchString("^(?:"+seg.Pattern+")$", part)
				if err != nil || !matched {
					return nil, false
				}
			}
			bindings[seg.Value] = part
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

// RewriteAddress applies captured variable bindings to a target pattern,
// producing a new account address.
func RewriteAddress(bindings map[string]string, target []PatternSegment) (string, error) {
	parts := make([]string, len(target))
	for i, seg := range target {
		switch seg.Kind {
		case SegmentFixed:
			parts[i] = seg.Value
		case SegmentVariable:
			val, ok := bindings[seg.Value]
			if !ok {
				return "", fmt.Errorf("missing binding for variable %q", seg.Value)
			}
			parts[i] = val
		}
	}

	return strings.Join(parts, ":"), nil
}

// PatternString reconstructs the pattern string from parsed segments.
func PatternString(segments []PatternSegment) string {
	parts := make([]string, len(segments))
	for i, seg := range segments {
		switch seg.Kind {
		case SegmentFixed:
			parts[i] = seg.Value
		case SegmentVariable:
			if seg.Pattern != "" {
				parts[i] = "{" + seg.Value + ":" + seg.Pattern + "}"
			} else {
				parts[i] = "{" + seg.Value + "}"
			}
		}
	}

	return strings.Join(parts, ":")
}

// VariableNames returns the variable names in a pattern in order.
func VariableNames(segments []PatternSegment) []string {
	var names []string
	for _, seg := range segments {
		if seg.Kind == SegmentVariable {
			names = append(names, seg.Value)
		}
	}

	return names
}

// SortBySpecificity sorts pattern segment slices by specificity (most specific first).
// Ties are broken by fewer total segments (more constrained), then lexicographic on pattern string.
func SortBySpecificity(patterns [][]PatternSegment) {
	sort.SliceStable(patterns, func(i, j int) bool {
		si, sj := Specificity(patterns[i]), Specificity(patterns[j])
		if si != sj {
			return si > sj
		}
		li, lj := len(patterns[i]), len(patterns[j])
		if li != lj {
			return li < lj
		}

		return PatternString(patterns[i]) < PatternString(patterns[j])
	})
}
