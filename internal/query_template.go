package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"strings"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/internal/resources"
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
	Type    string `json:"type,omitempty"`
	Default any    `json:"default"`
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
	Name     string                 `json:"name,omitempty"`
	Resource resources.ResourceKind `json:"resource"`
	Params   json.RawMessage        `json:"params"`
	Vars     map[string]VarSpec     `json:"vars"`
	Body     json.RawMessage        `json:"body"`
}

// Validate a query template
func (q QueryTemplate) Validate() error {
	// check params
	if len(q.Params) == 0 {
		return nil
	}
	switch q.Resource {
	case resources.ResourceKindAccount:
		return nil
	case resources.ResourceKindLog:
		return nil
	case resources.ResourceKindTransaction:
		return nil
	case resources.ResourceKindVolume:
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
func ResolveFilterTemplate(resourceKind resources.ResourceKind, body json.RawMessage, varDeclarations map[string]VarSpec, callVars map[string]any) (query.Builder, error) {
	vars := map[string]any{}
	for k, v := range varDeclarations {
		if v.Default != nil {
			vars[k] = v.Default
		}
	}
	maps.Copy(vars, callVars)

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	schema := resources.GetResourceSchema(resourceKind)

	var filter map[string]any
	if err := dec.Decode(&filter); err != nil {
		return nil, err
	}
	result, err := resolveFilterTemplate(schema, filter, vars)
	if err != nil {
		return nil, err
	}
	if filter, ok := result.(map[string]any); ok {
		s, err := json.Marshal(filter)
		if err != nil {
			return nil, err
		}
		return query.ParseJSON(string(s))
	} else {
		return nil, fmt.Errorf("unexpected type")
	}
}

func resolveFilterTemplate(schema resources.EntitySchema, m any, vars map[string]any) (any, error) {
	var err error
	switch v := m.(type) {
	case []any:
		for idx, s := range v {
			v[idx], err = resolveFilterTemplate(schema, s, vars)
			if err != nil {
				return nil, err
			}
		}
	case map[string]any:
		for key, value := range v {
			if !strings.HasPrefix(key, "$") {
				// if value is a string, it may contain variable placeholders that we need to resolve
				if value, ok := value.(string); ok {
					v[key], err = resolveFilter(schema, key, value, vars)
					if err != nil {
						return nil, err
					}
				}
			} else {
				v[key], err = resolveFilterTemplate(schema, value, vars)
				if err != nil {
					return nil, err
				}
			}
		}
	default:
		return nil, fmt.Errorf("unexpected filter shape: %v", v)
	}
	return m, nil
}

func resolveFilter(schema resources.EntitySchema, key string, value string, vars map[string]any) (any, error) {
	key, _, err := parseAccess(key)
	if err != nil {
		return nil, err
	}
	_, field := schema.GetFieldByNameOrAlias(key)
	if field == nil {
		return nil, fmt.Errorf("unknown field: %s", key)
	}
	valueType := field.Type.ValueType()
	switch valueType.(type) {
	case resources.TypeString:
		return resources.ReplaceVariables(value, vars)
	case resources.TypeBoolean:
		value, err := extractVariable[bool](value, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case resources.TypeDate:
		value, err := extractVariable[string](value, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case resources.TypeNumeric:
		v, err := extractVariable[json.Number](value, vars)
		if err != nil {
			// fallback to float64 for now
			v, err2 := extractVariable[float64](value, vars)
			if err2 != nil {
				return nil, err
			}
			return v, nil
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected resources.FieldType: %#v", valueType)
	}
}

var varRegex = regexp.MustCompile(`^<([a-z_]+)>$`)

func extractVariable[T any](s string, vars map[string]any) (*T, error) {
	matches := varRegex.FindStringSubmatch(s)

	if len(matches) == 0 {
		return nil, fmt.Errorf("expected a \"<variable>\" string or a plain value")
	}
	name := matches[1]
	if value, ok := vars[name]; ok {
		if v, ok := value.(T); ok {
			return &v, nil
		} else {
			return nil, fmt.Errorf("cannot use variable `%s` as type `%s`", name, reflect.TypeOf((*T)(nil)).Elem().Name())
		}
	} else {
		return nil, fmt.Errorf("missing variable: %v", name)
	}
}

var accessRegex = regexp.MustCompile(`^([a-z_]+)(?:\[([a-zA-Z0-9_/]+)\])?$`)

func parseAccess(input string) (string, string, error) {
	m := accessRegex.FindStringSubmatch(input)
	if m == nil {
		return "", "", errors.New("invalid field name")
	}
	return m[1], m[2], nil
}
