package ledger

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/formancehq/go-libs/v3/query"
)

type QueryMode string

const (
	Sync QueryMode = "sync"
)

type QueryTemplates map[string]QueryTemplate

type ParamSpec struct {
	Type    string  `json:"type,omitempty"`
	Default *string `json:"default"`
}

func (p *ParamSpec) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		p.Type = s
		return nil
	}
	type alias ParamSpec
	var a alias
	if err := json.Unmarshal(b, &a); err == nil {
		return err
	}
	*p = ParamSpec(a)
	return nil
}

type QueryTemplate struct {
	Name        string               `json:"name,omitempty"`
	OperationId string               `json:"operationId"`
	Mode        QueryMode            `json:"mode"`
	Params      map[string]ParamSpec `json:"params"`
	Body        json.RawMessage      `json:"body"`
}

func ResolveFilter(body json.RawMessage, params map[string]string) (query.Builder, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var filter map[string]any
	if err := dec.Decode(&filter); err != nil {
		return nil, err
	}
	if len(filter) == 0 {
		return nil, fmt.Errorf("empty filter")
	}
	resolve(filter, params)

	s, err := json.Marshal(filter)
	if err != nil {
		return nil, err
	}
	return query.ParseJSON(string(s))
}

func resolve(m any, params map[string]string) any {
	switch v := m.(type) {
	case string:
		for k, s := range params {
			v = strings.ReplaceAll(v, fmt.Sprintf("<%s>", k), s)
		}
		return v
	case []any:
		for idx, s := range v {
			v[idx] = resolve(s, params)
		}
	case map[string]any:
		for key, value := range v {
			v[key] = resolve(value, params)
		}
	default:
		panic(fmt.Sprintf("unexpected filter shape: %v", v))
	}
	return m
}
