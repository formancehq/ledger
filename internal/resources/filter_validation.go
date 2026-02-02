package resources

import (
	"encoding/json"
	"fmt"
	"reflect"
)

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
	// if value is a string, it may contain variable placeholders that we need to validate
	if value, ok := value.(string); ok && expectedType != ValueTypeString {
		err := validateVarRef(expectedType, value, vars)
		if err != nil {
			return err
		}
	} else {
		// otherwise check that the any's type matches
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
