package queries

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"regexp"
	"strings"

	"github.com/formancehq/go-libs/v3/query"
)

func unmarshalWithNumber(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	return dec.Decode(v)
}

func ValidateFilterBody(resource ResourceKind, body json.RawMessage, vars map[string]VarSpec) error {
	var filter map[string]any
	if err := unmarshalWithNumber(body, &filter); err != nil {
		return err
	}
	schema := GetResourceSchema(resource)
	return validateFilterTemplate(schema, filter, vars)
}

func validateFilterTemplate(schema EntitySchema, m map[string]any, vars map[string]VarSpec) error {
	operator, value, err := singleKey(m)
	if err != nil {
		return err
	}
	switch operator {
	case "$and", "$or":
		if set, ok := value.([]any); ok {
			for _, v := range set {
				if v, ok := v.(map[string]any); ok {
					err = validateFilterTemplate(schema, v, vars)
					if err != nil {
						return err
					}
				} else {
					return fmt.Errorf("unexpected type: %T", v)
				}
			}
		} else {
			return fmt.Errorf("unexpected type: %T", value)
		}
	case "$match", "$gte", "$lte", "$gt", "$lt", "$exists", "$like", "$in":
		if mp, ok := value.(map[string]any); ok {
			fieldKey, operand, err := singleKey(mp)
			if err != nil {
				return err
			}
			valueType, err := schema.GetFieldType(fieldKey)
			if err != nil {
				return err
			}
			if operator == "$in" {
				if set, ok := operand.([]any); ok {
					for _, v := range set {
						err := validateValue(*valueType, v, vars)
						if err != nil {
							return err
						}
					}
				} else {
					return fmt.Errorf("unexpected type: %T", value)
				}
			} else {
				err := validateValue(*valueType, operand, vars)
				if err != nil {
					return err
				}
			}
		} else {
			return fmt.Errorf("unexpected type: %T", value)
		}
	default:
		return fmt.Errorf("unexpected operator: %T", operator)
	}
	return nil
}

func validateValue(expectedType ValueType, value any, vars map[string]VarSpec) error {
	// if value is a string and we don't expect a string,
	// it must be a variable placeholders that we need to validate
	if value, ok := value.(string); ok && expectedType != ValueTypeString {
		err := validateVarRef(expectedType, value, vars)
		if err != nil {
			return err
		}
	} else {
		// otherwise check that the value's type matches
		err := validateValueType(expectedType, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func validateVarRef(expectedType ValueType, s string, vars map[string]VarSpec) error {
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

func singleKey(m map[string]any) (string, any, error) {
	switch {
	case len(m) == 0:
		return "", nil, fmt.Errorf("expected single key, found none")
	case len(m) > 1:
		return "", nil, fmt.Errorf("expected single key, found more than one")
	default:
		var (
			key   string
			value any
		)
		for key, value = range m {
		}
		return key, value, nil
	}
}

// Resolve filter template using the provided vars
func ResolveFilterTemplate(resourceKind ResourceKind, body json.RawMessage, varDeclarations map[string]VarSpec, callVars map[string]any) (query.Builder, error) {
	vars := map[string]any{}
	for k, v := range varDeclarations {
		if v.Default != nil {
			vars[k] = v.Default
		}
	}
	maps.Copy(vars, callVars)

	schema := GetResourceSchema(resourceKind)

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
		return nil, fmt.Errorf("unexpected type: %T", result)
	}
}

func resolveFilterTemplate(schema EntitySchema, m any, vars map[string]any) (any, error) {
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
				v[key], err = resolveNestedFilter(schema, key, value, vars)
				if err != nil {
					return nil, err
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

func resolveNestedFilter(schema EntitySchema, key string, value any, vars map[string]any) (any, error) {
	var err error
	switch v := value.(type) {
	case string:
		value, err = resolveFilter(schema, key, v, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case []any:
		for idx := range v {
			v[idx], err = resolveNestedFilter(schema, key, v[idx], vars)
			if err != nil {
				return nil, err
			}
		}
		return v, nil
	default:
		return nil, fmt.Errorf("unexpected filter shape: %v", v)
	}
}

func resolveFilter(schema EntitySchema, key string, value string, vars map[string]any) (any, error) {
	valueType, err := schema.GetFieldType(key)
	if err != nil {
		return nil, err
	}
	switch *valueType {
	case ValueTypeString:
		return ReplaceVariables(value, vars)
	case ValueTypeBoolean:
		value, err := extractVariable[bool](value, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case ValueTypeDate:
		value, err := extractVariable[string](value, vars)
		if err != nil {
			return nil, err
		}
		return value, nil
	case ValueTypeInt:
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
		return nil, fmt.Errorf("unexpected FieldType: %#v", valueType)
	}
}

var varRegex = regexp.MustCompile(`^\${([a-z_]+)}$`)

func extractVariableName(s string) (string, error) {
	matches := varRegex.FindStringSubmatch(s)
	if len(matches) == 0 {
		return "", fmt.Errorf("expected a \"${variable}\" string or a plain value")
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
