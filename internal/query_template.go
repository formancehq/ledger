package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iancoleman/strcase"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
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

type QueryMode string

const (
	Sync QueryMode = "sync"
)

type QueryTemplates map[string]QueryTemplate

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
	QueryTemplateParams
	Cursor string `json:"cursor"`
}

type QueryTemplateParams struct {
	PIT        *time.Time
	OOT        *time.Time
	Expand     []string
	Opts       json.RawMessage
	SortColumn string
	SortOrder  *bunpaginate.Order
	PageSize   uint
}

func (q *QueryTemplateParams) UnmarshalJSON(data []byte) error {
	type X struct {
		PIT      *time.Time      `json:"endTime"`
		OOT      *time.Time      `json:"startTime"`
		Expand   []string        `json:"expand,omitempty"`
		Opts     json.RawMessage `json:"opts"`
		Sort     string          `json:"sort"`
		PageSize uint            `json:"pageSize"`
	}
	var x X
	err := json.Unmarshal(data, &x)
	if err != nil {
		return err
	}
	q.PIT = x.PIT
	q.OOT = x.OOT
	q.Expand = x.Expand
	q.Opts = x.Opts
	q.PageSize = x.PageSize

	if x.Sort != "" {
		parts := strings.SplitN(x.Sort, ":", 2)
		q.SortColumn = strcase.ToSnake(parts[0])
		if len(parts) > 1 {
			switch {
			case strings.ToLower(parts[1]) == "desc":
				q.SortOrder = pointer.For(bunpaginate.Order(bunpaginate.OrderDesc))
			case strings.ToLower(parts[1]) == "asc":
				q.SortOrder = pointer.For(bunpaginate.Order(bunpaginate.OrderAsc))
			default:
				return fmt.Errorf("invalid order: %s", parts[1])
			}
		}
	}
	return nil
}

func (q *QueryTemplateParams) Overwrite(other QueryTemplateParams) {
	if other.PIT != nil {
		q.PIT = other.PIT
	}
	if other.OOT != nil {
		q.OOT = other.OOT
	}
	if len(other.Expand) > 0 {
		q.Expand = other.Expand
	}
	if other.Opts != nil {
		q.Opts = other.Opts
	}
	if other.SortColumn != "" {
		q.SortColumn = other.SortColumn
	}
	if other.SortOrder != nil {
		q.SortOrder = other.SortOrder
	}
	if other.PageSize != 0 {
		q.PageSize = other.PageSize
	}
}

type QueryTemplate struct {
	Name     string              `json:"name,omitempty"`
	Resource ResourceKind        `json:"resource"`
	Mode     QueryMode           `json:"mode"`
	Params   QueryTemplateParams `json:"params"`
	Vars     map[string]VarSpec  `json:"vars"`
	Body     json.RawMessage     `json:"body"`
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
