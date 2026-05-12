package accounttype

import (
	"fmt"
	"regexp"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// ValidatePattern parses and validates a pattern string.
// Returns an error if the pattern is syntactically invalid.
func ValidatePattern(pattern string) error {
	_, err := ParsePattern(pattern)

	return err
}

// Implicit regex constraints for typed segments.
var (
	uuidRe   = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	uint64Re = regexp.MustCompile(`^[0-9]+$`)
	bytesRe  = regexp.MustCompile(`^([0-9a-f]{2})+$`)
)

// ValidateSegmentTypes checks that segment_types references valid variable names
// from the pattern and that existing regex constraints are compatible with the
// declared types.
func ValidateSegmentTypes(segments []PatternSegment, segTypes map[string]commonpb.SegmentValueType) error {
	vars := make(map[string]int, len(segments))

	for i := range segments {
		if segments[i].Kind == SegmentVariable {
			vars[segments[i].Value] = i
		}
	}

	for name, svt := range segTypes {
		idx, ok := vars[name]
		if !ok {
			return fmt.Errorf("segment_types references unknown variable %q", name)
		}

		if svt == commonpb.SegmentValueType_SEGMENT_VALUE_STRING {
			continue
		}

		implicitRe := implicitRegexp(svt)
		if implicitRe == nil {
			return fmt.Errorf("segment_types has unsupported type %d for variable %q", svt, name)
		}

		// The type's implicit regex replaces any explicit regex.
		// The type constraint is authoritative for key encoding.
		segments[idx].CompiledRegexp = implicitRe
	}

	return nil
}

func implicitRegexp(svt commonpb.SegmentValueType) *regexp.Regexp {
	switch svt {
	case commonpb.SegmentValueType_SEGMENT_VALUE_UUID:
		return uuidRe
	case commonpb.SegmentValueType_SEGMENT_VALUE_UINT64:
		return uint64Re
	case commonpb.SegmentValueType_SEGMENT_VALUE_BYTES:
		return bytesRe
	default:
		return nil
	}
}
