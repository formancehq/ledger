package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
)

type ResourceKind string

const (
	ResourceKindTransactions ResourceKind = "transactions"
	ResourceKindAccounts     ResourceKind = "accounts"
	ResourceKindLogs         ResourceKind = "logs"
	ResourceKindVolumes      ResourceKind = "volumes"
)

type QueryTemplates map[string]QueryTemplate

func (t QueryTemplates) Validate() error {
	for _, t := range t {
		if err := t.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type VarSpec struct {
	Type    string  `json:"type,omitempty"`
	Default *string `json:"default"`
}

func (p *VarSpec) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		p.Type = s
		return nil
	}
	type alias VarSpec
	var a alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*p = VarSpec(a)
	return nil
}

type RunQueryTemplateParams struct {
	json.RawMessage
	Cursor string `json:"cursor"`
}

type QueryTemplateParams[Opts any] struct {
	PIT        *time.Time
	OOT        *time.Time
	Expand     []string
	Opts       Opts
	SortColumn string
	SortOrder  *bunpaginate.Order
	PageSize   uint
}

func (q QueryTemplateParams[Opts]) Overwrite(others ...json.RawMessage) (*QueryTemplateParams[Opts], error) {
	for _, other := range others {
		if len(other) != 0 {
			err := json.Unmarshal(other, &q)
			if err != nil {
				return nil, err
			}
			err = json.Unmarshal(other, &q.Opts)
			if err != nil {
				return nil, err
			}
		}
	}
	return &q, nil
}

type QueryTemplate struct {
	Name     string             `json:"name,omitempty"`
	Resource ResourceKind       `json:"resource"`
	Params   json.RawMessage    `json:"params"`
	Vars     map[string]VarSpec `json:"vars"`
	Body     json.RawMessage    `json:"body"`
}

// Validate a query template
func (q QueryTemplate) Validate() error {
	if len(q.Params) == 0 {
		return nil
	}
	switch q.Resource {
	case ResourceKindAccounts:
		return nil
	case ResourceKindLogs:
		return nil
	case ResourceKindTransactions:
		return nil
	case ResourceKindVolumes:
		var x GetVolumesOptions
		if err := json.Unmarshal(q.Params, &x); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown resource kind: %v", q.Resource)
	}
	return nil
}

// Resolve filter template using the provided vars
func ResolveFilterTemplate(body json.RawMessage, vars map[string]string) (query.Builder, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var filter map[string]any
	if err := dec.Decode(&filter); err != nil {
		return nil, err
	}
	resolveFilterTemplate(filter, vars)

	s, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}
	return query.ParseJSON(string(s))
}

func resolveFilterTemplate(m any, vars map[string]string) any {
	switch v := m.(type) {
	case string:
		for k, s := range vars {
			v = strings.ReplaceAll(v, fmt.Sprintf("<%s>", k), s)
		}
		return v
	case []any:
		for idx, s := range v {
			v[idx] = resolveFilterTemplate(s, vars)
		}
	case map[string]any:
		for key, value := range v {
			v[key] = resolveFilterTemplate(value, vars)
		}
	default:
		panic(fmt.Sprintf("unexpected filter shape: %v", v))
	}
	return m
}
