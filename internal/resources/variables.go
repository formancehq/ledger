package resources

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/formancehq/go-libs/v3/time"
)

type VarSpec struct {
	Type    ValueType `json:"type,omitempty"`
	Default any       `json:"default"`
}

func (p *VarSpec) UnmarshalJSON(b []byte) error {
	// handle plain string as type
	var s string
	if err := unmarshalWithNumber(b, &s); err == nil {
		p.Type = ValueType(s)
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

func ValidateVars(vars map[string]VarSpec) error {
	for name, spec := range vars {
		// validate type
		if !slices.Contains(ValueTypes, spec.Type) {
			return fmt.Errorf("variable `%s` has invalid type `%s`, expected one of `%v`", name, spec, ValueTypes)
		}
		// validate default
		err := validateValueType(spec.Type, spec.Default)
		if err != nil {
			return fmt.Errorf("invalid default for variable `%s`: %w", name, err)
		}
	}
	return nil
}

func validateValueType(expectedType ValueType, v any) error {
	var err error
	switch expectedType {
	case ValueTypeBoolean:
		err = validateVariableDefault[bool](v, nil)
	case ValueTypeDate:
		err = validateVariableDefault(v, func(dateString string) error {
			_, err := time.ParseTime(dateString)
			if err != nil {
				return err
			}
			return nil
		})
	case ValueTypeInt:
		err = validateVariableDefault[json.Number](v, nil)
		if err != nil {
			err = validateVariableDefault[float64](v, nil)
		}
	case ValueTypeString:
		err = validateVariableDefault[string](v, nil)
	}
	if err != nil {
		return fmt.Errorf("invalid value for type: %w", err)
	}
	return nil
}
