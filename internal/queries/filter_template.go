package queries

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"reflect"
	"regexp"

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

	builder, err := query.ParseJSON(string(body))
	if err != nil {
		return err
	}
	if builder == nil {
		return nil
	}

	return builder.Walk(func(operator string, key string, value *any) error {
		ty, err := schema.GetFieldType(key)
		if err != nil {
			return err
		}
		switch operator {
		case "$in":
			if set, ok := (*value).([]any); ok {
				for _, v := range set {
					err := validateValue(*ty, v, vars)
					if err != nil {
						return err
					}
				}
			} else {
				return fmt.Errorf("unexpected type: %T", *value)
			}
		default:
			err := validateValue(*ty, *value, vars)
			if err != nil {
				return err
			}
		}
		return nil
	})
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
		return fmt.Errorf("default value doesn't match declared type `%s`", reflect.TypeFor[T]().Name())
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

	builder, err := query.ParseJSON(string(body))
	if err != nil {
		return nil, err
	}

	err = builder.Walk(func(operator string, key string, value *any) error {
		var err error
		*value, err = resolveNestedFilter(schema, key, *value, vars)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return builder, nil
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
		return nil, fmt.Errorf("unexpected filter shape: %T", v)
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
		return *value, nil
	case ValueTypeDate:
		value, err := extractVariable[string](value, vars)
		if err != nil {
			return nil, err
		}
		return *value, nil
	case ValueTypeInt:
		v, err := extractVariable[json.Number](value, vars)
		if err != nil {
			// fallback to float64 for now
			v, err2 := extractVariable[float64](value, vars)
			if err2 != nil {
				return nil, err
			}
			bigFloat := new(big.Float).SetFloat64(*v)
			bigInt, acc := bigFloat.Int(nil)
			if acc != big.Exact {
				return nil, fmt.Errorf("provided number should be an integer: %#v", valueType)
			}
			return bigInt, nil
		}
		if x, ok := new(big.Int).SetString(string(*v), 10); ok {
			return x, nil
		} else {
			return nil, fmt.Errorf("provided number should be an integer: %#v", valueType)
		}
	default:
		return nil, fmt.Errorf("unexpected FieldType: %#v", valueType)
	}
}

var varRegex = regexp.MustCompile(`^\${([a-z_]+)}$`)

func extractVariableName(s string) (string, error) {
	matches := varRegex.FindStringSubmatch(s)
	if len(matches) == 0 {
		return "", fmt.Errorf("expected a \"${variable}\" string or a plain value, got `%s`", s)
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
