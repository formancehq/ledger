package queries

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func ValidateFilterBody(resource ResourceKind, body json.RawMessage, varDecls map[string]VarDecl) error {
	var (
		vars = map[string]FieldType{}
		err  error
	)
	for k, v := range varDecls {
		vars[k] = v.Type
	}

	var filter map[string]any
	if err := unmarshalWithNumber(body, &filter); err != nil {
		return err
	}
	schema, err := GetResourceSchema(resource)
	if err != nil {
		return err
	}

	builder, err := query.ParseJSON(string(body))
	if err != nil {
		return err
	}
	if builder == nil {
		return nil
	}

	return builder.Walk(func(operator string, key string, value *any) error {
		fieldType, err := schema.GetFieldType(key)
		if err != nil {
			return err
		}
		switch operator {
		case OperatorIn:
			// we expect the value to be a slice of the same type as fieldType
			if values, ok := (*value).([]any); ok {
				for _, v := range values {
					err := validateValue(fieldType, v, vars)
					if err != nil {
						return err
					}
				}
			} else {
				return fmt.Errorf("expected array, got `%T`", *value)
			}
		case OperatorExists:
			// we expect the field to be a map, and the value to match its underlying type
			if m, ok := fieldType.(TypeMap); ok {
				return validateValue(m.underlyingType, *value, vars)
			} else {
				return fmt.Errorf("$exists can only be called on a map field, got: %T", fieldType)
			}
		case OperatorMatch, OperatorLike, OperatorLT, OperatorGT, OperatorLTE, OperatorGTE:
			return validateValue(fieldType, *value, vars)
		default:
			return fmt.Errorf("unexpected operator: %s", operator)
		}
		return nil
	})
}

func validateValue(expectedType FieldType, value any, vars map[string]FieldType) error {
	// if value is a string and we don't expect a string,
	// it must be a variable placeholders that we need to validate
	if valueStr, ok := value.(string); ok {
		if expectedType != (TypeString{}) {
			err := validateVarRef(expectedType, valueStr, vars)
			if err != nil {
				return err
			}
		} else {
			err := ValidateStringTemplate(valueStr, vars)
			if err != nil {
				return err
			}
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

func validateVarRef(expectedType FieldType, s string, vars map[string]FieldType) error {
	name, err := extractVariableName(s)
	if err != nil {
		return err
	}
	if varType, ok := vars[name]; ok {
		if varType != expectedType {
			return fmt.Errorf("cannot use variable `%s` as type `%s`", name, FieldTypeToString(expectedType))
		}
	} else {
		return fmt.Errorf("variable `%v` is not declared", name)
	}
	return nil
}

// Resolve filter template using the provided vars
func ResolveFilterTemplate(resourceKind ResourceKind, body json.RawMessage, varDecls map[string]VarDecl, callVars map[string]any) (query.Builder, error) {
	vars := map[string]any{}
	for k, v := range varDecls {
		if v.Default != nil {
			vars[k] = v.Default
		}
	}
	for k, v := range callVars {
		if decl, ok := varDecls[k]; ok {
			if err := validateValueType(decl.Type, v); err != nil {
				return nil, err
			} else {
				vars[k] = v
			}
		}
	}

	schema, err := GetResourceSchema(resourceKind)
	if err != nil {
		return nil, err
	}

	builder, err := query.ParseJSON(string(body))
	if err != nil {
		return nil, err
	}
	if builder == nil {
		return nil, nil
	}

	err = builder.Walk(func(operator string, key string, value *any) error {
		var err error
		fieldType, err := schema.GetFieldType(key)
		if err != nil {
			return err
		}
		*value, err = resolveFilter(operator, fieldType, *value, vars)
		if err != nil {
			return fmt.Errorf("invalid filter on key %s: %w", key, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return builder, nil
}

func resolveFilter(operator string, fieldType FieldType, value any, vars map[string]any) (any, error) {
	var err error
	switch operator {
	case OperatorIn:
		// we expect the value to be a values of the same type as fieldType
		if values, ok := value.([]any); ok {
			for idx := range values {
				if valueStr, ok := values[idx].(string); ok {
					values[idx], err = resolveValue(fieldType, valueStr, vars)
					if err != nil {
						return nil, err
					}
				}
			}
			return values, nil
		} else {
			return nil, fmt.Errorf("expected array, got: %T", value)
		}
	case OperatorExists:
		// we expect the field to be a map, and the value to match its underlying type
		if m, ok := fieldType.(TypeMap); ok {
			if valueStr, ok := value.(string); ok {
				value, err = resolveValue(m.underlyingType, valueStr, vars)
				if err != nil {
					return nil, err
				}
			}
		} else {
			return nil, fmt.Errorf("$exists can only be called on a map field, got: %T", fieldType)
		}
		return value, nil
	case OperatorMatch, OperatorLike, OperatorLT, OperatorGT, OperatorLTE, OperatorGTE:
		// we expect the field to be a map, and the value to match its underlying type
		if valueStr, ok := value.(string); ok {
			value, err = resolveValue(fieldType, valueStr, vars)
			if err != nil {
				return nil, err
			}
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unexpected operator: %s", operator)
	}
}

func resolveValue(fieldType FieldType, value string, vars map[string]any) (any, error) {
	switch fieldType.(type) {
	case TypeString:
		return ReplaceVariables(value, vars)
	case TypeBoolean:
		value, err := extractVariable[bool](value, vars)
		if err != nil {
			return nil, err
		}
		return *value, nil
	case TypeDate:
		value, err := extractVariable[string](value, vars)
		if err != nil {
			return nil, err
		}
		return *value, nil
	case TypeNumeric:
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
				return nil, fmt.Errorf("provided number should be an integer: %v", v)
			}
			return bigInt, nil
		}
		if x, ok := new(big.Int).SetString(string(*v), 10); ok {
			return x, nil
		} else {
			return nil, fmt.Errorf("provided number should be an integer: %v", v)
		}
	default:
		return nil, fmt.Errorf("unexpected FieldType: %#v", fieldType)
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
		return nil, fmt.Errorf("missing variable: `%s`", name)
	}
}
