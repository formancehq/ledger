package queries

import (
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v4/time"
)

func FieldTypeFromString(s string) (FieldType, error) {
	switch s {
	case "boolean":
		return NewTypeBoolean(), nil
	case "date":
		return NewTypeDate(), nil
	case "int":
		return NewTypeNumeric(), nil
	case "string":
		return NewTypeString(), nil
	default:
		return nil, fmt.Errorf("invalid type `%s`, expected one of `boolean`, `date`, `int`, `string`", s)
	}
}

func FieldTypeToString(t FieldType) string {
	switch v := t.(type) {
	case TypeBoolean:
		return "boolean"
	case TypeDate:
		return "date"
	case TypeMap:
		return fmt.Sprintf("map[string]%s", FieldTypeToString(v.underlyingType))
	case TypeNumeric:
		return "int"
	case TypeString:
		return "string"
	default:
		panic(fmt.Sprintf("unexpected queries.FieldType: %#v", t))
	}
}

type FieldType interface {
	Operators() []string
	ValidateValue(operator string, value any) error
	Index() FieldType // later we might want to pass the index as argument
	IsPaginated() bool
}

type Field struct {
	Aliases     []string
	Type        FieldType
	IsPaginated bool
}

func (f Field) WithAliases(aliases ...string) Field {
	f.Aliases = append(f.Aliases, aliases...)
	return f
}

func (f Field) Paginated() Field {
	f.IsPaginated = true
	return f
}

func (f Field) MatchKey(name, key string) bool {
	if key == name {
		return true
	}
	for _, alias := range f.Aliases {
		if key == alias {
			return true
		}
	}

	return false
}

func NewField(t FieldType) Field {
	return Field{
		Aliases: []string{},
		Type:    t,
	}
}

// NewStringField creates a new field with TypeString as its type.
func NewStringField() Field {
	return NewField(NewTypeString())
}

// NewDateField creates a new field with TypeDate as its type.
func NewDateField() Field {
	return NewField(NewTypeDate())
}

// NewMapField creates a new field with TypeMap as its type, using the provided underlying type.
func NewMapField(underlyingType FieldType) Field {
	return NewField(NewTypeMap(underlyingType))
}

// NewNumericField creates a new field with TypeNumeric as its type.
func NewNumericField() Field {
	return NewField(NewTypeNumeric())
}

// NewBooleanField creates a new field with TypeBoolean as its type.
func NewBooleanField() Field {
	return NewField(NewTypeBoolean())
}

// NewStringMapField creates a new field with TypeMap as its type, using TypeString as the underlying type.
func NewStringMapField() Field {
	return NewMapField(NewTypeString())
}

// NewNumericMapField creates a new field with TypeMap as its type, using TypeNumeric as the underlying type.
func NewNumericMapField() Field {
	return NewMapField(NewTypeNumeric())
}

type TypeString struct{}

func (t TypeString) IsPaginated() bool {
	return false
}

func (t TypeString) Index() FieldType {
	return nil
}

func (t TypeString) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLike,
		OperatorIn,
	}
}

func (t TypeString) ValidateValue(operator string, value any) error {
	switch operator {
	case OperatorIn:
		values, ok := value.([]any)
		if !ok {
			return fmt.Errorf("expected array value for operator %s, got %T", OperatorIn, value)
		}
		for _, v := range values {
			_, ok := v.(string)
			if !ok {
				return fmt.Errorf("expected string value in array for operator %s, got %T", OperatorIn, v)
			}
		}
	default:
		_, ok := value.(string)
		if !ok {
			return fmt.Errorf("expected string value, got %T", value)
		}
	}

	return nil
}

var _ FieldType = (*TypeString)(nil)

func NewTypeString() TypeString {
	return TypeString{}
}

type TypeDate struct{}

func (t TypeDate) IsPaginated() bool {
	return true
}

func (t TypeDate) Index() FieldType {
	return nil
}

func (t TypeDate) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLT,
		OperatorGT,
		OperatorLTE,
		OperatorGTE,
	}
}

func (t TypeDate) ValidateValue(_ string, value any) error {
	switch value := value.(type) {
	case string:
		_, err := time.ParseTime(value)
		if err != nil {
			return fmt.Errorf("invalid date value: %w", err)
		}
	case time.Time, *time.Time:
	default:
		return fmt.Errorf("expected string, time.Time, or *time.Time value, got %T", value)
	}
	return nil
}

func NewTypeDate() TypeDate {
	return TypeDate{}
}

var _ FieldType = (*TypeDate)(nil)

type TypeMap struct {
	underlyingType FieldType
}

func (t TypeMap) IsPaginated() bool {
	return false
}

func (t TypeMap) Index() FieldType {
	return t.underlyingType
}

func (t TypeMap) Operators() []string {
	return append(t.underlyingType.Operators(), OperatorMatch, OperatorExists)
}

func (t TypeMap) ValidateValue(operator string, value any) error {
	return t.underlyingType.ValidateValue(operator, value)
}

func NewTypeMap(underlyingType FieldType) TypeMap {
	return TypeMap{
		underlyingType: underlyingType,
	}
}

var _ FieldType = (*TypeMap)(nil)

type TypeNumeric struct{}

func (t TypeNumeric) IsPaginated() bool {
	return true
}

func (t TypeNumeric) Index() FieldType {
	return nil
}

func (t TypeNumeric) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLT,
		OperatorGT,
		OperatorLTE,
		OperatorGTE,
	}
}

func (t TypeNumeric) ValidateValue(_ string, value any) error {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float64, float32, big.Int,
		*int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float64, *float32, *big.Int:
		return nil
	default:
		return fmt.Errorf("expected numeric value, got %T", value)
	}
}

func NewTypeNumeric() TypeNumeric {
	return TypeNumeric{}
}

var _ FieldType = (*TypeNumeric)(nil)

type TypeBoolean struct{}

func (t TypeBoolean) IsPaginated() bool {
	return false
}

func (t TypeBoolean) Index() FieldType {
	return nil
}

func (t TypeBoolean) Operators() []string {
	return []string{
		OperatorMatch,
	}
}

func (t TypeBoolean) ValidateValue(_ string, value any) error {
	_, ok := value.(bool)
	if !ok {
		return fmt.Errorf("expected boolean value, got %T", value)
	}

	return nil
}

func NewTypeBoolean() TypeBoolean {
	return TypeBoolean{}
}

var _ FieldType = (*TypeBoolean)(nil)
