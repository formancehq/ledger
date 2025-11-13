package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type ChartAccountRules struct{}

type ChartAccount struct {
	Metadata map[string]string
	Rules    ChartAccountRules
}

type ChartSegment struct {
	VariableSegment *ChartVariableSegment
	FixedSegments   map[string]ChartSegment
	Account         *ChartAccount
}

type ChartVariableSegment struct {
	ChartSegment

	Pattern string
	Label   string
}

type ChartOfAccounts map[string]ChartSegment

const SegmentRegex = "^\\$?[a-zA-Z0-9_-]+$"

var Regexp = regexp.MustCompile(SegmentRegex)

func ValidateSegment(addr string) bool {
	return Regexp.Match([]byte(addr))
}

func (s *ChartOfAccounts) UnmarshalJSON(data []byte) error {
	var segment map[string]json.RawMessage
	if err := json.Unmarshal(data, &segment); err != nil {
		return err
	}
	out := make(map[string]ChartSegment)
	for key, value := range segment {
		if !ValidateSegment(key) || key[0] == '$' || key[0] == '_' {
			return fmt.Errorf("invalid segment name: %v", key)
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
	isLeaf := true
	var isAccount bool
	var account ChartAccount
	var fixedSegments map[string]ChartSegment
	var variableSegment *ChartVariableSegment
	keys := []string{}
	for key := range segment {
		keys = append(keys, key)
	}
	for _, key := range keys {
		value := segment[key]
		isSubsegment := key[0] != '_'

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
				if pat, ok := segment["_pattern"]; ok {
					if pat, ok := pat.(string); ok {
						pattern = &pat
					}
				}
			}
			segment := ChartSegment{}
			err := segment.UnmarshalJSON(value)
			if err != nil {
				return fmt.Errorf("invalid segment: %v", err)
			}
			if pattern != nil {
				if key[0] != '$' {
					return fmt.Errorf("cannot have a pattern on a fixed segment")
				}
				if variableSegment != nil {
					return fmt.Errorf("cannot have two variable segments with the same prefix")
				}
				variableSegment = &ChartVariableSegment{
					ChartSegment: segment,
					Pattern:      *pattern,
					Label:        key[1:],
				}
			} else {
				if key[0] == '$' {
					return fmt.Errorf("cannot have a variable segment without a pattern")
				}
				if fixedSegments == nil {
					fixedSegments = map[string]ChartSegment{}
				}
				fixedSegments[key] = segment
			}
			isLeaf = false
		} else if key == "_self" {
			if string(value) != "{}" {
				return fmt.Errorf("_self must be an empty object")
			}
			isAccount = true
		} else if key == "_metadata" {
			err := json.Unmarshal(value, &account.Metadata)
			if err != nil {
				return fmt.Errorf("invalid default metadata: %v", err)
			}
		} else if key == "_rules" {
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
			out["_metadata"] = s.Account.Metadata
		}
		out["_rules"] = s.Account.Rules
		if len(s.FixedSegments) > 0 || s.VariableSegment != nil {
			out["_self"] = map[string]interface{}{}
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
	out["_pattern"] = s.Pattern
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
			return nil, ErrInvalidAccount{path, nextSegment}
		}
	}
	if variableSegment != nil {
		matches, err := regexp.Match(variableSegment.Pattern, []byte(nextSegment))
		if err != nil {
			return nil, errors.New("invalid regex")
		}
		if matches {
			if len(account) > 1 {
				return findAccountSchema(append(path, nextSegment), variableSegment.FixedSegments, variableSegment.VariableSegment, account[1:])
			} else if variableSegment.Account != nil {
				return variableSegment.Account, nil
			} else {
				return nil, ErrInvalidAccount{path, nextSegment}
			}
		}
	}
	return nil, ErrInvalidAccount{path, nextSegment}
}
func (c *ChartOfAccounts) FindAccountSchema(account string) (*ChartAccount, error) {
	schema, err := findAccountSchema([]string{}, map[string]ChartSegment(*c), nil, strings.Split(account, ":"))
	if err != nil {
		if account == "world" {
			return &ChartAccount{}, nil
		}
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
