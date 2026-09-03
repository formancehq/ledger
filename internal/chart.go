package ledger

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
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
	// VariableSegments holds every `$variable` child declared at this level,
	// ordered by label. A level may declare several of them as long as each one
	// carries a `.pattern`, which is how a single level fans out into subtrees
	// of different shapes depending on the value of the segment.
	VariableSegments []ChartVariableSegment
	FixedSegments    map[string]ChartSegment
	Account          *ChartAccount
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
		isLeaf           = true
		isAccount        bool
		account          ChartAccount
		fixedSegments    map[string]ChartSegment
		variableSegments []ChartVariableSegment
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
				variableSegments = append(variableSegments, ChartVariableSegment{
					ChartSegment: segment,
					Pattern:      pattern,
					Label:        key[1:],
				})
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
	// A pattern is what keeps sibling variable segments apart: an ungated one
	// matches every value, so it would make its siblings ambiguous.
	if len(variableSegments) > 1 {
		for _, variableSegment := range variableSegments {
			if variableSegment.Pattern == nil {
				return fmt.Errorf(
					"variable segment $%v needs a %v: a level declaring several variable segments requires one on each of them",
					variableSegment.Label, PATTERN_KEY,
				)
			}
		}
	}
	// Subsegments come out of a map, so order by label to keep resolution and
	// serialization deterministic.
	slices.SortFunc(variableSegments, func(a, b ChartVariableSegment) int {
		return strings.Compare(a.Label, b.Label)
	})

	isAccount = isAccount || isLeaf
	if isAccount {
		s.Account = &account
	}
	s.FixedSegments = fixedSegments
	s.VariableSegments = variableSegments

	if _, ok := segment[METADATA_KEY]; ok && !isAccount {
		return fmt.Errorf("cannot have %v on a non-account segment", METADATA_KEY)
	}
	if _, ok := segment[RULES_KEY]; ok && !isAccount {
		return fmt.Errorf("cannot have %v on a non-account segment", RULES_KEY)
	}

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
	for _, variableSegment := range s.VariableSegments {
		key := fmt.Sprintf("$%v", variableSegment.Label)
		serialized, err := variableSegment.MarshalJSON()
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
		if len(s.FixedSegments) > 0 || len(s.VariableSegments) > 0 {
			out[SELF_KEY] = map[string]any{}
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

// descendInto resolves the remainder of an address against the segment
// `account[0]` was matched by, either by recursing into its children or, when
// the address stops here, by returning the account the segment carries.
func descendInto(path []string, segment ChartSegment, account []string) (*ChartAccount, error) {
	nextSegment := account[0]
	if len(account) > 1 {
		// Cap the slice so append allocates instead of writing into the
		// caller's backing array: sibling variable segments are tried in turn
		// and each one's errors keep a reference to the path they were built
		// from.
		childPath := append(path[:len(path):len(path)], nextSegment)
		return findAccountSchema(childPath, segment.FixedSegments, segment.VariableSegments, account[1:])
	}
	if segment.Account != nil {
		return segment.Account, nil
	}
	return nil, ErrInvalidAccount{
		path:            path,
		segment:         nextSegment,
		patternMismatch: false,
		hasSubsegments:  false,
	}
}

func findAccountSchema(path []string, fixedSegments map[string]ChartSegment, variableSegments []ChartVariableSegment, account []string) (*ChartAccount, error) {
	nextSegment := account[0]
	if segment, ok := fixedSegments[nextSegment]; ok {
		return descendInto(path, segment, account)
	}
	// A level can declare several variable segments, each gated by its own
	// pattern, so the same level can hold subtrees of different shapes. They are
	// tried in label order and the first one resolving the rest of the address
	// wins; a candidate whose pattern matches but whose subtree rejects the
	// remainder does not hide the others.
	var firstMatchErr error
	for _, variableSegment := range variableSegments {
		if variableSegment.Pattern != nil {
			matches, err := regexp.Match(*variableSegment.Pattern, []byte(nextSegment))
			if err != nil {
				return nil, fmt.Errorf("invalid pattern regex: %v", err)
			}
			if !matches {
				continue
			}
		}
		schema, err := descendInto(path, variableSegment.ChartSegment, account)
		if err == nil {
			return schema, nil
		}
		if firstMatchErr == nil {
			firstMatchErr = err
		}
	}
	// Some pattern admitted the segment, the address was rejected further down.
	if firstMatchErr != nil {
		return nil, firstMatchErr
	}
	return nil, ErrInvalidAccount{
		path:            path,
		segment:         nextSegment,
		patternMismatch: len(variableSegments) > 0,
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

func (c *ChartAccount) DefaultMetadata() metadata.Metadata {
	defaultMetadata := metadata.Metadata{}
	for key, value := range c.Metadata {
		if value.Default != nil {
			defaultMetadata[key] = *value.Default
		}
	}
	return defaultMetadata
}
