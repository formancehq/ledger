package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type AccountRules struct {
	AllowedSources      map[string]interface{} `json:"allowedSources,omitempty"`
	AllowedDestinations map[string]interface{} `json:"allowedDestinations,omitempty"`
}

type AccountSchema struct {
	Metadata map[string]string
	Rules    AccountRules
}

type SegmentSchema struct {
	VariableSegment *VariableSegment
	FixedSegments   map[string]SegmentSchema
	Account         *AccountSchema
}

type VariableSegment struct {
	SegmentSchema

	Pattern string
	Label   string
}

type ChartOfAccounts map[string]SegmentSchema

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
	*s = rootSegment.FixedSegments
	if rootSegment.VariableSegment != nil {
		return errors.New("variable segments are not allowed at the root")
	}
	if rootSegment.Account != nil {
		return errors.New("the chart root is not a valid account")
	}
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
	var fixedSegments map[string]SegmentSchema
	var variableSegment *VariableSegment
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
			var pattern *string
			{
				var segment map[string]any
				err := json.Unmarshal(value, &segment)
				if err != nil {
					return fmt.Errorf("invalid subsegment: %v", err)
				}
				if pat, ok := segment["_pattern"]; ok {
					if pat, ok := pat.(string); ok {
						pattern = &pat
					}
				}
			}
			segment := SegmentSchema{}
			err := segment.UnmarshalJSON(value)
			if err != nil {
				return fmt.Errorf("invalid subsegment: %v", err)
			}
			if pattern != nil {
				if key[0] != '$' {
					return fmt.Errorf("cannot have a pattern on a fixed segment") // TODO: Should this actually be an error?
				}
				if variableSegment != nil {
					return fmt.Errorf("invalid subsegments: cannot have two variable segments with the same prefix")
				}
				variableSegment = &VariableSegment{
					SegmentSchema: segment,
					Pattern:       *pattern,
					Label:         key[1:],
				}
			} else {
				if key[0] == '$' {
					return fmt.Errorf("cannot have a variable segment without a pattern") // TODO: Should this actually be an error?
				}
				if fixedSegments == nil {
					fixedSegments = map[string]SegmentSchema{}
				}
				fixedSegments[key] = segment
			}
			isLeaf = false
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
	if isAccount {
		s.Account = &account
	}
	s.FixedSegments = fixedSegments
	s.VariableSegment = variableSegment

	return nil
}

func (s *ChartOfAccounts) MarshalJSON() ([]byte, error) {
	out := make(map[string]any)
	for key, value := range map[string]SegmentSchema(*s) {
		serialized, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		out[key] = json.RawMessage(serialized)
	}
	return json.Marshal(out)
}

func (s *SegmentSchema) marshalJsonObject() (map[string]any, error) {
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
		if s.Account.Rules.AllowedDestinations != nil || s.Account.Rules.AllowedSources != nil {
			out["_rules"] = s.Account.Rules
		}
		if len(s.FixedSegments) > 0 || s.VariableSegment != nil {
			out["_self"] = map[string]interface{}{}
		}
	}
	return out, nil
}

func (s *SegmentSchema) MarshalJSON() ([]byte, error) {
	out, err := s.marshalJsonObject()
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func (s *VariableSegment) MarshalJSON() ([]byte, error) {
	out, err := s.marshalJsonObject()
	if err != nil {
		return nil, err
	}
	out["_pattern"] = s.Pattern
	return json.Marshal(out)
}

func findAccountSchema(fixedSegments map[string]SegmentSchema, variableSegment *VariableSegment, account []string) (*AccountSchema, error) {
	nextSegment := account[0]
	if segment, ok := fixedSegments[nextSegment]; ok {
		if len(account) > 1 {
			return findAccountSchema(segment.FixedSegments, segment.VariableSegment, account)
		} else if segment.Account != nil {
			return segment.Account, nil
		} else {
			return nil, errors.New("account is not allowed by the chart of accounts")
		}
	}
	if variableSegment != nil {
		matches, err := regexp.Match(variableSegment.Pattern, []byte(nextSegment))
		if err != nil {
			return nil, errors.New("invalid regex")
		}
		if matches {
			return nil, errors.New("account is not allowed by the chart of accounts")
		}
	}
	return nil, errors.New("account is not allowed by the chart of accounts")
}
func (c *ChartOfAccounts) FindAccountSchema(account string) (*AccountSchema, error) {
	schema, err := findAccountSchema(map[string]SegmentSchema(*c), nil, strings.Split(account, ":"))
	if err != nil {
		if account == "world" {
			return &AccountSchema{}, nil
		}
		return nil, err
	}
	return schema, nil
}

func (c *ChartOfAccounts) ValidatePosting(posting Posting) error {
	source, err := c.FindAccountSchema(posting.Source)
	if err != nil {
		return err
	}
	destination, err := c.FindAccountSchema(posting.Destination)
	if err != nil {
		return err
	}
	if source.Rules.AllowedDestinations != nil && source.Rules.AllowedDestinations[posting.Destination] == nil {
		return errors.New("destination is not allowed")
	}
	if destination.Rules.AllowedSources != nil && destination.Rules.AllowedSources[posting.Source] == nil {
		return errors.New("source is not allowed")
	}
	return nil
}
