package flagbind

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ParseSetValues parses a list of Helm-style "key=value" strings into a nested map.
// Keys use dotted paths with optional array indices:
//
//	"ingress.tls[0].secretName=my-secret"
//	"ingress.tls[0].hosts[0]=example.com"
//	"raft.electionTick=10"
//
// Values are coerced: "true"/"false" -> bool, valid integers -> float64, else string.
func ParseSetValues(sets []string) (map[string]any, error) {
	result := make(map[string]any)

	for _, s := range sets {
		before, after, ok := strings.Cut(s, "=")
		if !ok {
			return nil, fmt.Errorf("invalid --set value %q: missing '='", s)
		}
		key := before
		val := after

		if err := setNestedPath(result, key, coerceValue(val)); err != nil {
			return nil, fmt.Errorf("--set %q: %w", s, err)
		}
	}

	return result, nil
}

// setNestedPath sets a value at a dotted path with array index support.
// It creates intermediate maps and slices as needed.
func setNestedPath(root map[string]any, path string, val any) error {
	segments := parseSegments(path)

	return setSegments(root, segments, val)
}

// segment represents a key or an array index in a path.
type segment struct {
	key   string
	index int
	isIdx bool
}

// parseSegments splits "a.b[0].c[1]" into: key(a), key(b), idx(0), key(c), idx(1)
// Escaped dots (\.) are treated as literal dots in key names, e.g.:
//
//	"annotations.service\.beta\.kubernetes\.io/foo" → key(annotations), key(service.beta.kubernetes.io/foo)
func parseSegments(path string) []segment {
	var segs []segment
	for _, part := range splitEscaped(path, '.') {
		bracketIdx := strings.IndexByte(part, '[')
		if bracketIdx < 0 {
			segs = append(segs, segment{key: part})

			continue
		}

		name := part[:bracketIdx]
		if name != "" {
			segs = append(segs, segment{key: name})
		}

		rest := part[bracketIdx:]
		for rest != "" {
			if rest[0] != '[' {
				break
			}
			end := strings.IndexByte(rest, ']')
			if end < 0 {
				break
			}
			n, err := strconv.Atoi(rest[1:end])
			if err == nil {
				segs = append(segs, segment{index: n, isIdx: true})
			}
			rest = rest[end+1:]
		}
	}

	return segs
}

// splitEscaped splits a string on sep, respecting:
//   - \. escaped dots (Helm-style)
//   - "quoted.segments" where dots are literal
func splitEscaped(s string, sep byte) []string {
	var parts []string
	var buf strings.Builder
	inQuote := false

	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case ch == '"':
			inQuote = !inQuote
		case ch == '\\' && i+1 < len(s) && s[i+1] == sep:
			buf.WriteByte(sep)
			i++
		case ch == sep && !inQuote:
			parts = append(parts, buf.String())
			buf.Reset()
		default:
			buf.WriteByte(ch)
		}
	}
	parts = append(parts, buf.String())

	return parts
}

// setSegments recursively navigates/creates the structure and sets the value.
func setSegments(current any, segs []segment, val any) error {
	if len(segs) == 0 {
		return nil
	}

	seg := segs[0]
	isLast := len(segs) == 1

	if seg.isIdx {
		arr, ok := current.([]any)
		if !ok {
			return fmt.Errorf("expected array for index [%d]", seg.index)
		}
		// This shouldn't happen at top level — arrays are always behind a key.
		_ = arr

		return errors.New("unexpected array index at root")
	}

	m, ok := current.(map[string]any)
	if !ok {
		return fmt.Errorf("expected map for key %q", seg.key)
	}

	if isLast {
		m[seg.key] = val

		return nil
	}

	next := segs[1]

	if next.isIdx {
		// Next segment is an array index — ensure this key holds a slice.
		arr, _ := m[seg.key].([]any)
		if arr == nil {
			arr = make([]any, 0)
		}

		// Grow the array if needed.
		for len(arr) <= next.index {
			arr = append(arr, nil)
		}

		if len(segs) == 2 {
			// The array element IS the value.
			arr[next.index] = val
		} else {
			// More segments after the index — ensure the element is a map or array.
			elem := arr[next.index]
			thirdSeg := segs[2]
			if thirdSeg.isIdx {
				if elem == nil {
					elem = make([]any, 0)
				}
				innerArr, _ := elem.([]any)
				for len(innerArr) <= thirdSeg.index {
					innerArr = append(innerArr, nil)
				}
				if len(segs) == 3 {
					innerArr[thirdSeg.index] = val
				} else {
					if innerArr[thirdSeg.index] == nil {
						innerArr[thirdSeg.index] = make(map[string]any)
					}
					if err := setSegments(innerArr[thirdSeg.index], segs[3:], val); err != nil {
						return err
					}
				}
				elem = innerArr
			} else {
				if elem == nil {
					elem = make(map[string]any)
				}
				if err := setSegments(elem, segs[2:], val); err != nil {
					return err
				}
			}
			arr[next.index] = elem
		}

		m[seg.key] = arr

		return nil
	}

	// Next segment is a key — ensure this key holds a map.
	sub, _ := m[seg.key].(map[string]any)
	if sub == nil {
		sub = make(map[string]any)
		m[seg.key] = sub
	}

	return setSegments(sub, segs[1:], val)
}

// coerceValue keeps all values as strings.
// ApplyToStruct handles type conversion via a lenient JSON round-trip.
func coerceValue(s string) any {
	return s
}
