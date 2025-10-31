package ledger

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"

	"github.com/formancehq/go-libs/v3/pointer"
)

type AccountSchema struct {
	Metadata map[string]string
	Rules    string
}

type SegmentSchema struct {
	Fixed    *string
	Pattern  *string
	Label    *string
	Segments []SegmentSchema
	Account  *AccountSchema
}

type ChartOfAccounts []SegmentSchema

const SegmentRegex = "^\\$?[a-zA-Z0-9_-]+$"

var Regexp = regexp.MustCompile(SegmentRegex)

func ValidateSegment(addr string) bool {
	return Regexp.Match([]byte(addr))
}

func (s *ChartOfAccounts) UnmarshalJSON(data []byte) error {
	var rootSegment SegmentSchema
	err := rootSegment.UnmarshalJSON(data)
	if err != nil {
		return err
	}
	*s = rootSegment.Segments
	return nil
}
func (s *SegmentSchema) UnmarshalJSON(data []byte) error {
	var segment map[string]json.RawMessage
	if err := json.Unmarshal(data, &segment); err != nil {
		return err
	}
	isLeaf := true
	var isAccount bool
	var account AccountSchema
	var pattern *string
	hasVariableSubsegment := false
	var segments []SegmentSchema
	keys := []string{}
	for key := range segment {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := segment[key]
		isSubsegment := key[0] != '_'

		if isSubsegment {
			if !ValidateSegment(key) {
				return fmt.Errorf("invalid address segment: %v", key)
			}
			segment := SegmentSchema{}
			err := segment.UnmarshalJSON(value)
			if err != nil {
				return fmt.Errorf("invalid subsegment: %v", err)
			}
			if segment.Pattern != nil {
				if hasVariableSubsegment {
					return fmt.Errorf("invalid subsegments: cannot have two variable segments with the same prefix")
				}
				hasVariableSubsegment = true
				segment.Label = pointer.For(key[1:])
			} else {
				segment.Fixed = &key
			}
			if key[0] == '$' && segment.Pattern == nil {
				return fmt.Errorf("cannot have a variable segment without a pattern") // TODO: Should this actually be an error?
			}
			if segments == nil {
				segments = []SegmentSchema{}
			}
			segments = append(segments, segment)
			isLeaf = false
		} else if key == "_pattern" {
			var pat string
			err := json.Unmarshal(value, &pat)
			if err != nil {
				return nil
			}
			pattern = &pat
		} else if key == "_self" {
			isAccount = true
		} else if key == "_metadata" {
			err := json.Unmarshal(value, &account.Metadata)
			if err != nil {
				return err
			}
		} else if key == "_rules" {
			err := json.Unmarshal(value, &account.Rules)
			if err != nil {
				return err
			}
		}
	}
	isAccount = isAccount || isLeaf
	s.Segments = segments
	s.Pattern = pattern
	if isAccount {
		s.Account = &account
	}

	return nil
}

func (s *ChartOfAccounts) MarshalJSON() ([]byte, error) {
	out := make(map[string]any)
	for _, value := range []SegmentSchema(*s) {
		serialized, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if value.Fixed != nil {
			out[*value.Fixed] = json.RawMessage(serialized)
		} else if value.Label != nil {
			key := "$" + *value.Label
			out[key] = json.RawMessage(serialized)
		}
	}
	return json.Marshal(out)
}
func (s *SegmentSchema) MarshalJSON() ([]byte, error) {
	out := make(map[string]any)
	for _, value := range s.Segments {
		serialized, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		if value.Fixed != nil {
			out[*value.Fixed] = json.RawMessage(serialized)
		} else if value.Label != nil {
			key := "$" + *value.Label
			out[key] = json.RawMessage(serialized)
		}
	}
	if s.Pattern != nil {
		out["_pattern"] = s.Pattern
	}
	if s.Account != nil {
		if len(s.Segments) > 0 {
			out["_self"] = map[string]any{}
		}
		out["_metadata"] = s.Account.Metadata
	}
	return json.Marshal(out)
}
