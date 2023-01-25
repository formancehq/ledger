package spec

import (
	"errors"
	"reflect"
)

type ScalarSchema[T comparable] struct {
	Required     bool `json:"required"`
	DefaultValue *T   `json:"defaultValue"`
	Enum         []T  `json:"enum"`
}

func (s ScalarSchema[T]) validate(value any) error {
	if !s.Required {
		return nil
	}
	if value == nil && s.DefaultValue == nil {
		return errors.New("required value")
	}
	return nil
}

func (s ScalarSchema[T]) WithRequired() ScalarSchema[T] {
	s.Required = true
	return s
}

func (s ScalarSchema[T]) getType() string {
	var t T
	return reflect.TypeOf(t).Name()
}

func (s ScalarSchema[T]) WithDefault(def T) ScalarSchema[T] {
	s.DefaultValue = &def
	return s
}

func NewUInt64Schema() ScalarSchema[uint64] {
	return ScalarSchema[uint64]{}
}

func NewStringSchema() ScalarSchema[string] {
	return ScalarSchema[string]{}
}

func init() {
	registerBaseSchema(NewUInt64Schema())
	registerBaseSchema(NewStringSchema())
}
