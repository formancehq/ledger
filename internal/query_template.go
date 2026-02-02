package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
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

func unmarshalWithNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

func (q QueryTemplateParams[Opts]) Overwrite(others ...json.RawMessage) (*QueryTemplateParams[Opts], error) {
	for _, other := range others {
		if len(other) != 0 {
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
	Params      json.RawMessage            `json:"params"`
	Vars        map[string]queries.VarSpec `json:"vars"`
	Body        json.RawMessage            `json:"body"`
}

// Validate a query template
func (q QueryTemplate) Validate() error {
	// check resource validity
	if !slices.Contains(queries.Resources, q.Resource) {
		return fmt.Errorf("unknown resource kind: %v", q.Resource)
	}
	// check if the params matches the resource
	if len(q.Params) > 0 {
		var err error
		// TODO: missing commmon param unmarshal?
		// err = validateParam[QueryTemplateParams](q.Params)
		// if err != nil {
		// 	return fmt.Errorf("invalid params: %w", err)
		// }
		switch q.Resource {
		case queries.ResourceKindVolume:
			err = validateParam[GetVolumesOptions](q.Params)
		}
		if err != nil {
			return fmt.Errorf("invalid params: %w", err)
		}
	}
	err := queries.ValidateVars(q.Vars)
	if err != nil {
		return err
	}
	// validate body
	return queries.ValidateFilterBody(q.Resource, q.Body, q.Vars)
}

func validateParam[Opts any](params json.RawMessage) error {
	if params == nil {
		return nil
	}
	var x GetVolumesOptions
	if err := unmarshalWithNumber(params, &x); err != nil {
		return err
	}
	return nil
}
