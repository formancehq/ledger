package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/iancoleman/strcase"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/internal/queries"
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

type QueryTemplateParams[Opts any] struct {
	PIT        *time.Time
	OOT        *time.Time
	Expand     []string
	Opts       Opts
	SortColumn string
	SortOrder  *bunpaginate.Order
	PageSize   uint
}

func (p *QueryTemplateParams[Opts]) UnmarshalJSON(b []byte) error {
	var x struct {
		PIT      *time.Time `json:"endTime"`
		OOT      *time.Time `json:"startTime"`
		Expand   []string   `json:"expand,omitempty"`
		Sort     string     `json:"sort"`
		PageSize uint       `json:"pageSize"`
	}
	err := json.Unmarshal(b, &x)
	if err != nil {
		return err
	}
	p.PIT = x.PIT
	p.OOT = x.OOT
	p.Expand = x.Expand
	p.PageSize = x.PageSize

	if x.Sort != "" {
		parts := strings.SplitN(x.Sort, ":", 2)
		p.SortColumn = strcase.ToSnake(parts[0])
		if strings.TrimSpace(parts[0]) == "" {
			return fmt.Errorf("invalid sort column: %q", x.Sort)
		}
		if len(parts) > 1 {
			switch {
			case strings.ToLower(parts[1]) == "desc":
				p.SortOrder = pointer.For(bunpaginate.Order(bunpaginate.OrderDesc))
			case strings.ToLower(parts[1]) == "asc":
				p.SortOrder = pointer.For(bunpaginate.Order(bunpaginate.OrderAsc))
			default:
				return fmt.Errorf("invalid order: %s", parts[1])
			}
		}
	}
	return nil
}

func unmarshalWithNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

func (q QueryTemplateParams[Opts]) Overwrite(others ...json.RawMessage) (*QueryTemplateParams[Opts], error) {
	for _, other := range others {
		if len(other) != 0 && !bytes.Equal(bytes.TrimSpace(other), []byte("null")) {
			err := unmarshalWithNumber(other, &q)
			if err != nil {
				return nil, err
			}
			err = unmarshalWithNumber(other, &q.Opts)
			if err != nil {
				return nil, err
			}
		}
	}
	return &q, nil
}

type QueryTemplate struct {
	Description string                     `json:"description,omitempty"`
	Resource    queries.ResourceKind       `json:"resource"`
	Params      json.RawMessage            `json:"params,omitempty"`
	Vars        map[string]queries.VarDecl `json:"vars,omitempty"`
	Body        json.RawMessage            `json:"body,omitempty"`
}

// Validate a query template
func (q QueryTemplate) Validate() error {
	// check resource validity
	schema, err := queries.GetResourceSchema(q.Resource)
	if err != nil {
		return fmt.Errorf("unknown resource kind: %v", q.Resource)
	}
	// check if the params matches the resource
	if len(q.Params) > 0 {
		var params QueryTemplateParams[any]
		err := validateParam(q.Params, &params)
		if err != nil {
			return fmt.Errorf("invalid params: %w", err)
		}
		if params.SortColumn != "" {
			_, field := schema.GetFieldByNameOrAlias(params.SortColumn)
			if field == nil || !field.IsPaginated {
				return fmt.Errorf("invalid sort column `%s`", params.SortColumn)
			}
		}
		switch q.Resource {
		case queries.ResourceKindVolume:
			var opts GetVolumesOptions
			err = validateParam(q.Params, &opts)
		}
		if err != nil {
			return fmt.Errorf("invalid params: %w", err)
		}
	}
	// validate variable declarations
	err = queries.ValidateVarDeclarations(q.Vars)
	if err != nil {
		return fmt.Errorf("failed to validate variable declarations: %w", err)
	}
	// validate body
	if len(q.Body) > 0 {
		err = queries.ValidateFilterBody(q.Resource, q.Body, q.Vars)
		if err != nil {
			return fmt.Errorf("failed to validate filter body: %w", err)
		}
	}
	return nil
}

func validateParam[Opts any](params json.RawMessage, pointer *Opts) error {
	if params == nil {
		return nil
	}
	if err := unmarshalWithNumber(params, pointer); err != nil {
		return err
	}
	return nil
}
