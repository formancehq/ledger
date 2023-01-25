package spec

import (
	"fmt"

	"github.com/pkg/errors"
)

type PolymorphicObjectSchema struct {
	Discriminator string                  `json:"discriminator"`
	Options       map[string]ObjectSchema `json:"options"`
}

var _ Schema = PolymorphicObjectSchema{}

func (s PolymorphicObjectSchema) getType() string {
	return "polymorphic-object"
}

func (s PolymorphicObjectSchema) validate(value any) (err error) {
	m, ok := value.(map[string]any)
	if !ok {
		return errors.New("expected map")
	}

	discriminator, ok := m[s.Discriminator]
	if !ok {
		return fmt.Errorf("expected discriminator: '%s'", s.Discriminator)
	}

	schema, ok := s.Options[discriminator.(string)]
	if !ok {
		return fmt.Errorf("unexpected discriminator: %s", discriminator)
	}

	mCp := make(map[string]any)
	for key, value := range m {
		mCp[key] = value
	}

	return schema.validate(mCp)
}

func (s PolymorphicObjectSchema) WithOption(kind string, schema ObjectSchema) PolymorphicObjectSchema {
	s.Options[kind] = schema
	return s
}

func NewPolymorphicObjectSchema(discriminator string) PolymorphicObjectSchema {
	return PolymorphicObjectSchema{
		Discriminator: discriminator,
		Options:       map[string]ObjectSchema{},
	}
}

func init() {
	registerBaseSchema(NewPolymorphicObjectSchema(""))
}
