package ledger

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

type ChartAccountRules struct{}

type ChartAccountMetadata struct {
	Default *string `json:"default,omitempty"`
}

type ChartAccount struct {
	Metadata map[string]ChartAccountMetadata
	Rules    ChartAccountRules
}

type ChartSegment struct {
	VariableSegment *ChartVariableSegment
	FixedSegments   map[string]ChartSegment
	Account         *ChartAccount
}

type ChartVariableSegment struct {
	ChartSegment

	Pattern *string
	Label   string
}

const PROPERTY_PREFIX = "."
const PATTERN_KEY = PROPERTY_PREFIX + "pattern"
const SELF_KEY = PROPERTY_PREFIX + "self"
const RULES_KEY = PROPERTY_PREFIX + "rules"
const METADATA_KEY = PROPERTY_PREFIX + "metadata"

type ChartOfAccounts map[string]ChartSegment

var ChartSegmentRegexp = regexp.MustCompile(`^(\$|\.)?[a-zA-Z0-9_-]+$`)

func ValidateSegment(addr string) bool {
	return ChartSegmentRegexp.Match([]byte(addr))
}

func (s *ChartOfAccounts) UnmarshalJSON(data []byte) error {
	var segment map[string]json.RawMessage
	if err := json.Unmarshal(data, &segment); err != nil {
		return err
	}
	out := make(map[string]ChartSegment)
	for key, value := range segment {
		if !ValidateSegment(key) {
			return fmt.Errorf("invalid segment name: %v", key)
		}
		if strings.HasPrefix(key, "$") {
			return fmt.Errorf("invalid key %v: root cannot have a variable segment", key)
		}
		if strings.HasPrefix(key, PROPERTY_PREFIX) {
			return fmt.Errorf("invalid key %v: the root cannot be an account", key)
		}

		// prevent .pattern on root segments
		{
			var segment map[string]any
			err := json.Unmarshal(value, &segment)
			if err != nil {
				return fmt.Errorf("invalid segment: %v", err)
			}
			if _, ok := segment[PATTERN_KEY]; ok {
				return fmt.Errorf("cannot have a pattern on a fixed segment")
			}
		}

		var seg ChartSegment
		err := seg.UnmarshalJSON(value)
		if err != nil {
			return fmt.Errorf("invalid segment `%v`: %v", key, err)
		}
		out[key] = seg
	}
	*s = out
	return nil
}
func (s *ChartSegment) UnmarshalJSON(data []byte) error {
	var segment map[string]json.RawMessage
	if err := json.Unmarshal(data, &segment); err != nil {
		return err
	}
	var (
		isLeaf          = true
		isAccount       bool
		account         ChartAccount
		fixedSegments   map[string]ChartSegment
		variableSegment *ChartVariableSegment
	)
	for key, value := range segment {
		isSubsegment := !strings.HasPrefix(key, PROPERTY_PREFIX)

		if isSubsegment {
			if !ValidateSegment(key) {
				return fmt.Errorf("invalid address segment: %v", key)
			}
			var pattern *string
			{
				var segment map[string]any
				err := json.Unmarshal(value, &segment)
				if err != nil {
					return fmt.Errorf("invalid segment: %v", err)
				}
				if pat, ok := segment[PATTERN_KEY]; ok {
					if pat, ok := pat.(string); ok {
						_, err := regexp.Compile(pat)
						if err != nil {
							return fmt.Errorf("invalid pattern regex: %v", err)
						}
						pattern = &pat
					} else {
						return fmt.Errorf("pattern must be a string")
					}
				}
			}
			segment := ChartSegment{}
			err := segment.UnmarshalJSON(value)
			if err != nil {
				return fmt.Errorf("invalid segment: %v", err)
			}
			if strings.HasPrefix(key, "$") {
				if variableSegment != nil {
					return fmt.Errorf("cannot have two variable segments with the same prefix")
				}
				variableSegment = &ChartVariableSegment{
					ChartSegment: segment,
					Pattern:      pattern,
					Label:        key[1:],
				}
			} else if pattern != nil {
				return fmt.Errorf("cannot have a pattern on a fixed segment")
			} else {
				if fixedSegments == nil {
					fixedSegments = map[string]ChartSegment{}
				}
				fixedSegments[key] = segment
			}
			isLeaf = false
		} else if key == SELF_KEY {
			var obj map[string]json.RawMessage
			if err := json.Unmarshal(value, &obj); err != nil {
				return fmt.Errorf("%v must be an empty object", SELF_KEY)
			}
			if len(obj) != 0 {
				return fmt.Errorf("%v must be an empty object", SELF_KEY)
			}
			isAccount = true
		} else if key == METADATA_KEY {
			err := json.Unmarshal(value, &account.Metadata)
			if err != nil {
				return fmt.Errorf("invalid default metadata: %v", err)
			}
		} else if key == RULES_KEY {
			err := json.Unmarshal(value, &account.Rules)
			if err != nil {
				return fmt.Errorf("invalid account rules: %v", err)
			}
		}
	}
	isAccount = isAccount || isLeaf
	if isAccount {
		s.Account = &account
	}
	s.FixedSegments = fixedSegments
	s.VariableSegment = variableSegment

	return nil
}

func (s ChartOfAccounts) MarshalJSON() ([]byte, error) {
	out := make(map[string]any)
	for key, value := range map[string]ChartSegment(s) {
		serialized, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		out[key] = json.RawMessage(serialized)
	}
	return json.Marshal(out)
}

func (s ChartSegment) marshalJsonObject() (map[string]any, error) {
	out := make(map[string]any)
	for key, value := range s.FixedSegments {
		serialized, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		out[key] = json.RawMessage(serialized)
	}
	if s.VariableSegment != nil {
		key := fmt.Sprintf("$%v", s.VariableSegment.Label)
		serialized, err := s.VariableSegment.MarshalJSON()
		if err != nil {
			return nil, err
		}
		out[key] = json.RawMessage(serialized)
	}
	if s.Account != nil {
		if s.Account.Metadata != nil {
			out[METADATA_KEY] = s.Account.Metadata
		}
		// Never emitted for now
		if s.Account.Rules != (ChartAccountRules{}) {
			out[RULES_KEY] = s.Account.Rules
		}
		if len(s.FixedSegments) > 0 || s.VariableSegment != nil {
			out[SELF_KEY] = map[string]interface{}{}
		}
	}
	return out, nil
}

func (s ChartSegment) MarshalJSON() ([]byte, error) {
	out, err := s.marshalJsonObject()
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func (s ChartVariableSegment) MarshalJSON() ([]byte, error) {
	out, err := s.marshalJsonObject()
	if err != nil {
		return nil, err
	}
	if s.Pattern != nil {
		out[PATTERN_KEY] = *s.Pattern
	}
	return json.Marshal(out)
}

func findAccountSchema(path []string, fixedSegments map[string]ChartSegment, variableSegment *ChartVariableSegment, account []string) (*ChartAccount, error) {
	nextSegment := account[0]
	if segment, ok := fixedSegments[nextSegment]; ok {
		if len(account) > 1 {
			return findAccountSchema(append(path, nextSegment), segment.FixedSegments, segment.VariableSegment, account[1:])
		} else if segment.Account != nil {
			return segment.Account, nil
		} else {
			return nil, ErrInvalidAccount{
				path:            path,
				segment:         nextSegment,
				patternMismatch: false,
				hasSubsegments:  len(account) > 1,
			}
		}
	}
	if variableSegment != nil {
		matches := true
		if variableSegment.Pattern != nil {
			var err error
			matches, err = regexp.Match(*variableSegment.Pattern, []byte(nextSegment))
			if err != nil {
				return nil, fmt.Errorf("invalid pattern regex: %v", err)
			}
		}
		if matches {
			if len(account) > 1 {
				return findAccountSchema(append(path, nextSegment), variableSegment.FixedSegments, variableSegment.VariableSegment, account[1:])
			} else if variableSegment.Account != nil {
				return variableSegment.Account, nil
			} else {
				return nil, ErrInvalidAccount{
					path:            path,
					segment:         nextSegment,
					patternMismatch: false,
					hasSubsegments:  len(account) > 1,
				}
			}
		}
	}
	return nil, ErrInvalidAccount{
		path:            path,
		segment:         nextSegment,
		patternMismatch: variableSegment != nil,
		hasSubsegments:  len(account) > 1,
	}
}
func (c *ChartOfAccounts) FindAccountSchema(account string) (*ChartAccount, error) {
	schema, err := findAccountSchema([]string{}, map[string]ChartSegment(*c), nil, strings.Split(account, ":"))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

func (c *ChartOfAccounts) ValidatePosting(posting Posting) error {
	_, err := c.FindAccountSchema(posting.Source)
	if err != nil {
		return err
	}
	_, err = c.FindAccountSchema(posting.Destination)
	if err != nil {
		return err
	}
	return nil
}
