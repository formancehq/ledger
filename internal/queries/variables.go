package queries

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v4/time"
)

type VarDecl struct {
	Type    FieldType
	Default any
}

func (p *VarDecl) UnmarshalJSON(b []byte) error {
	// handle plain string as type
	var s string
	if err := unmarshalWithNumber(b, &s); err == nil {
		p.Type, err = FieldTypeFromString(s)
		return err
	}
	// handle full object case
	var a struct {
		Type    string `json:"type,omitempty"`
		Default any    `json:"default"`
	}
	if err := unmarshalWithNumber(b, &a); err != nil {
		return err
	}
	var err error
	p.Default = a.Default
	p.Type, err = FieldTypeFromString(a.Type)
	return err
}

func (p VarDecl) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string `json:"type"`
		Default any    `json:"default,omitempty"`
	}{
		Type:    FieldTypeToString(p.Type),
		Default: p.Default,
	})
}

func ValidateVarDeclarations(vars map[string]VarDecl) error {
	for name, decl := range vars {
		// validate default
		err := validateValueType(decl.Type, decl.Default)
		if err != nil {
			return fmt.Errorf("invalid default for variable `%s`: %w", name, err)
		}
	}
	return nil
}

// expectedType must be non-nil
func validateValueType(expectedType FieldType, v any) error {
	var err error
	switch expectedType.(type) {
	case TypeBoolean:
		err = castAndValidateValue[bool](v, nil)
	case TypeDate:
		err = castAndValidateValue(v, func(dateString string) error {
			_, err := time.ParseTime(dateString)
			if err != nil {
				return err
			}
			return nil
		})
	case TypeNumeric:
		err = castAndValidateValue(v, func(n json.Number) error {
			if _, ok := new(big.Int).SetString(string(n), 10); !ok {
				return fmt.Errorf("number should be an integer: %v", n)
			}
			return nil
		})
		if err != nil {
			err = castAndValidateValue(v, func(f float64) error {
				if !new(big.Float).SetFloat64(f).IsInt() {
					return fmt.Errorf("number should be an integer: %v", f)
				}
				return nil
			})
		}
		if err != nil {
			err = castAndValidateValue[*big.Int](v, nil)
		}

	case TypeString:
		err = castAndValidateValue[string](v, nil)
	default:
		err = fmt.Errorf("type cannot be constructed, you may need to specify a key with `[my_key]`")
	}
	if err != nil {
		return fmt.Errorf("invalid value `%v` for type `%s`: %w", v, FieldTypeToString(expectedType), err)
	}
	return nil
}

func castAndValidateValue[T any](value any, validate func(T) error) error {
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
		return errors.New("value doesn't match expected type")
	}
}
