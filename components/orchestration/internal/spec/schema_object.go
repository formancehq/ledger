package spec

import (
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"
)

type ObjectSchema struct {
	Properties map[string]Schema `json:"properties"`
}

var _ Schema = ObjectSchema{}
var _ json.Marshaler = ObjectSchema{}
var _ json.Unmarshaler = &ObjectSchema{}

func (s ObjectSchema) getType() string {
	return "object"
}

func (s ObjectSchema) MarshalJSON() ([]byte, error) {
	type aux ObjectSchema
	type schemaWithType struct {
		Schema any    `json:"schema"`
		Type   string `json:"type"`
	}
	return json.Marshal(struct {
		aux
		Properties map[string]schemaWithType `json:"properties"`
	}{
		aux: aux(s),
		Properties: func() map[string]schemaWithType {
			ret := make(map[string]schemaWithType)
			for key, schema := range s.Properties {
				ret[key] = schemaWithType{
					Schema: schema,
					Type:   schema.getType(),
				}
			}
			return ret
		}(),
	})
}

func (s *ObjectSchema) UnmarshalJSON(data []byte) error {
	type schemaWithType struct {
		Schema json.RawMessage `json:"schema"`
		Type   string          `json:"type"`
	}
	type aux ObjectSchema
	type wrappedType struct {
		aux
		Properties map[string]schemaWithType `json:"properties"`
	}
	x := wrappedType{}
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}

	*s = ObjectSchema(x.aux)
	s.Properties = map[string]Schema{}
	for key, property := range x.Properties {
		schema := reflect.New(reflect.TypeOf(baseSchemas[property.Type])).Interface()
		if err := json.Unmarshal(property.Schema, schema); err != nil {
			return err
		}

		s.Properties[key] = reflect.ValueOf(schema).Elem().Interface().(Schema)
	}
	return nil
}

func (s ObjectSchema) validate(raw any) error {
	m, ok := raw.(map[string]any)
	if !ok {
		return errors.New("expected map")
	}
	for key, schema := range s.Properties {
		if err := schema.validate(m[key]); err != nil {
			return errors.Wrapf(err, "validating key: %s", key)
		}
	}
	return nil
}

func (s ObjectSchema) WithProperty(name string, value Schema) ObjectSchema {
	s.Properties[name] = value
	return s
}

func NewObjectSchema() ObjectSchema {
	return ObjectSchema{
		Properties: map[string]Schema{},
	}
}

func init() {
	registerBaseSchema(NewObjectSchema())
}
