package ledger

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
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
	Type    resources.ValueType `json:"type,omitempty"`
	Default any                 `json:"default"`
}

func (p *VarSpec) UnmarshalJSON(b []byte) error {
	// handle plain string as type
	var s string
	if err := unmarshalWithNumber(b, &s); err == nil {
		p.Type = resources.ValueType(s)
		return nil
	}
	// handle full object case
	type alias VarSpec
	var a alias
	if err := unmarshalWithNumber(b, &a); err != nil {
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
	Description string                 `json:"description,omitempty"`
	Resource    resources.ResourceKind `json:"resource"`
	Params      json.RawMessage        `json:"params"`
	Vars        map[string]VarSpec     `json:"vars"`
	Body        json.RawMessage        `json:"body"`
}

// Validate a query template
func (q QueryTemplate) Validate() error {
	// check resource validity
	if !slices.Contains(resources.Resources, q.Resource) {
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
		case resources.ResourceKindVolume:
			err = validateParam[GetVolumesOptions](q.Params)
		}
		if err != nil {
			return fmt.Errorf("invalid params: %w", err)
		}
	}
	// check vars
	for name, spec := range q.Vars {
		// validate type
		if !slices.Contains(resources.ValueTypes, spec.Type) {
			return fmt.Errorf("variable `%s` has invalid type `%s`, expected one of `%v`", name, spec, resources.ValueTypes)
		}
		// validate default
		var err error
		switch spec.Type {
		case resources.ValueTypeBoolean:
			err = validateVariableDefault[bool](spec.Default, nil)
		case resources.ValueTypeDate:
			err = validateVariableDefault(spec.Default, func(dateString string) error {
				_, err := time.ParseTime(dateString)
				if err != nil {
					return err
				}
				return nil
			})
		case resources.ValueTypeInt:
			err = validateVariableDefault[json.Number](spec.Default, nil)
			if err != nil {
				err = validateVariableDefault[float64](spec.Default, nil)
			}
		case resources.ValueTypeString:
			err = validateVariableDefault[string](spec.Default, nil)
		}
		if err != nil {
			return fmt.Errorf("invalid default for variable %s: %w", name, err)
		}
	}
	// validate body
	return validateFilterBody(q.Resource, q.Body, q.Vars)
}

func validateFilterBody(resource resources.ResourceKind, body json.RawMessage, vars map[string]VarSpec) error {
	var filter map[string]any
	if err := unmarshalWithNumber(body, &filter); err != nil {
		return err
	}
	return validateFilterTemplate(resource, filter, vars)
}

func validateFilterTemplate(resource resources.ResourceKind, m any, vars map[string]VarSpec) error {
	var err error
	switch v := m.(type) {
	case []any:
		for _, s := range v {
			err = validateFilterTemplate(resource, s, vars)
			if err != nil {
				return err
			}
		}
	case map[string]any:
		for key, value := range v {
			if !strings.HasPrefix(key, "$") {
				schema := resources.GetResourceSchema(resource)
				valueType, err := getFieldType(schema, key)
				if err != nil {
					return err
				}
				// if value is a string, it may contain variable placeholders that we need to validate
				if value, ok := value.(string); ok && *valueType != resources.ValueTypeString {
					err = validateVarRef(*valueType, value, vars)
					if err != nil {
						return err
					}
				} else if true { // check any type matches valuetype
					switch *valueType {
					case resources.ValueTypeBoolean:

					case resources.ValueTypeDate:
					case resources.ValueTypeInt:
					case resources.ValueTypeString:
					default:
						panic("unexpected resources.ValueType")
					}
				}
			} else {
				err = validateFilterTemplate(resource, value, vars)
				if err != nil {
					return err
				}
			}
		}
	default:
		return fmt.Errorf("unexpected filter shape: %v", v)
	}
	return nil
}

func validateVarRef(expectedType resources.ValueType, s string, vars map[string]VarSpec) error {
	name, err := extractVariableName(s)
	if err != nil {
		return err
	}
	if spec, ok := vars[name]; ok {
		if spec.Type != expectedType {
			return fmt.Errorf("cannot use variable `%s` as type `%s`", name, expectedType)
		}
	} else {
		return fmt.Errorf("variable `%v` is not declared", name)
	}
	return nil
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

func validateVariableDefault[T any](value any, validate func(T) error) error {
	if value == nil {
		return nil
	}
	if v, ok := value.(T); ok {
		if validate != nil {
			return validate(v)
		} else {
			return nil
		}
	} else {
		return fmt.Errorf("default value doesn't match declared type `%s`", reflect.TypeOf((*T)(nil)).Elem().Name())
	}
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

	schema := resources.GetResourceSchema(resourceKind)

	var filter map[string]any
	if err := unmarshalWithNumber(body, &filter); err != nil {
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

func getFieldType(schema resources.EntitySchema, access string) (*resources.ValueType, error) {
	key, _, err := parseAccess(access)
	if err != nil {
		return nil, err
	}
	_, field := schema.GetFieldByNameOrAlias(key)
	if field == nil {
		return nil, fmt.Errorf("unknown field: %s", key)
	}
	return pointer.For(field.Type.ValueType()), nil
}

func resolveFilter(schema resources.EntitySchema, key string, value string, vars map[string]any) (any, error) {
	valueType, err := getFieldType(schema, key)
	if err != nil {
		return nil, err
	}
	switch *valueType {
	case resources.ValueTypeString:
		return resources.ReplaceVariables(value, vars)
	case resources.ValueTypeBoolean:
		value, err := extractVariable[bool](value, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case resources.ValueTypeDate:
		value, err := extractVariable[string](value, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case resources.ValueTypeInt:
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

func extractVariableName(s string) (string, error) {
	matches := varRegex.FindStringSubmatch(s)
	if len(matches) == 0 {
		return "", fmt.Errorf("expected a \"<variable>\" string or a plain value")
	}
	return matches[1], nil
}

func extractVariable[T any](s string, vars map[string]any) (*T, error) {
	name, err := extractVariableName(s)
	if err != nil {
		return nil, err
	}
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
