package ledger

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
)

type AccountRules struct {
	AllowedSources      []string `json:"allowedSources,omitempty"`
	AllowedDestinations []string `json:"allowedDestinations,omitempty"`
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
	var segment map[string]json.RawMessage
	if err := json.Unmarshal(data, &segment); err != nil {
		return err
	}
	out := make(map[string]SegmentSchema)
	for key, value := range segment {
		if !ValidateSegment(key) || key[0] == '$' || key[0] == '_' {
			return fmt.Errorf("invalid segment name: %v", key)
		}
		var seg SegmentSchema
		err := seg.UnmarshalJSON(value)
		if err != nil {
			return fmt.Errorf("invalid segment `%v`: %v", key, err)
		}
		out[key] = seg
	}
	*s = out
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
			segment := SegmentSchema{}
			err := segment.UnmarshalJSON(value)
			if err != nil {
				return fmt.Errorf("invalid segment: %v", err)
			}
			if pattern != nil {
				if key[0] != '$' {
					return fmt.Errorf("cannot have a pattern on a fixed segment") // TODO: Should this actually be an error?
				}
				if variableSegment != nil {
					return fmt.Errorf("cannot have two variable segments with the same prefix")
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
	for key, value := range map[string]SegmentSchema(s) {
		serialized, err := value.MarshalJSON()
		if err != nil {
			return nil, err
		}
		out[key] = json.RawMessage(serialized)
	}
	return json.Marshal(out)
}

func (s SegmentSchema) marshalJsonObject() (map[string]any, error) {
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

func (s SegmentSchema) MarshalJSON() ([]byte, error) {
	out, err := s.marshalJsonObject()
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func (s VariableSegment) MarshalJSON() ([]byte, error) {
	out, err := s.marshalJsonObject()
	if err != nil {
		return nil, err
	}
	out["_pattern"] = s.Pattern
	return json.Marshal(out)
}

func findAccountSchema(path []string, fixedSegments map[string]SegmentSchema, variableSegment *VariableSegment, account []string) (*AccountSchema, error) {
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
func (c *ChartOfAccounts) FindAccountSchema(account string) (*AccountSchema, error) {
	schema, err := findAccountSchema([]string{}, map[string]SegmentSchema(*c), nil, strings.Split(account, ":"))
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
	if source.Rules.AllowedDestinations != nil && !slices.Contains(source.Rules.AllowedDestinations, posting.Destination) {
		return ErrDestinationNotAllowed{
			source:              posting.Source,
			destination:         posting.Destination,
			allowedDestinations: source.Rules.AllowedDestinations,
		}
	}
	if destination.Rules.AllowedSources != nil && !slices.Contains(destination.Rules.AllowedSources, posting.Source) {
		return ErrSourceNotAllowed{
			source:         posting.Source,
			destination:    posting.Destination,
			allowedSources: destination.Rules.AllowedSources,
		}
	}
	return nil
}
