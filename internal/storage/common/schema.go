package common

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/time"
)

const (
	OperatorMatch  = "$match"
	OperatorExists = "$exists"
	OperatorLike   = "$like"
	OperatorLT     = "$lt"
	OperatorGT     = "$gt"
	OperatorLTE    = "$lte"
	OperatorGTE    = "$gte"
)

type FieldType interface {
	Operators() []string
	ValidateValue(value any) error
	IsIndexable() bool
}

type Field struct {
	Aliases []string
	Type    FieldType
}

func (f Field) WithAliases(aliases ...string) Field {
	f.Aliases = append(f.Aliases, aliases...)
	return f
}

func (f Field) matchKey(name, key string) bool {
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

func (t TypeString) IsIndexable() bool {
	return false
}

func (t TypeString) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLike,
	}
}

func (t TypeString) ValidateValue(value any) error {
	_, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value, got %T", value)
	}
	return nil
}

var _ FieldType = (*TypeString)(nil)

func NewTypeString() TypeString {
	return TypeString{}
}

type TypeDate struct{}

func (t TypeDate) IsIndexable() bool {
	return false
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

func (t TypeDate) ValidateValue(value any) error {
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

func (t TypeMap) IsIndexable() bool {
	return true
}

func (t TypeMap) Operators() []string {
	return append(t.underlyingType.Operators(), OperatorMatch, OperatorExists)
}

func (t TypeMap) ValidateValue(value any) error {
	return t.underlyingType.ValidateValue(value)
}

func NewTypeMap(underlyingType FieldType) TypeMap {
	return TypeMap{
		underlyingType: underlyingType,
	}
}

var _ FieldType = (*TypeMap)(nil)

type TypeNumeric struct{}

func (t TypeNumeric) IsIndexable() bool {
	return false
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

func (t TypeNumeric) ValidateValue(value any) error {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float64, float32,
		*int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float64, *float32:
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

func (t TypeBoolean) IsIndexable() bool {
	return false
}

func (t TypeBoolean) Operators() []string {
	return []string{
		OperatorMatch,
	}
}

func (t TypeBoolean) ValidateValue(value any) error {
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
