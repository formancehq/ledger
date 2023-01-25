package spec

import (
	"fmt"
)

var specifications = map[string]Specification{}

func RegisterSpecification(name string, specification Specification) {
	specifications[name] = specification
}

type Specification struct {
	ObjectSchema
}

func (s Specification) Validate(value map[string]any) error {
	return s.ObjectSchema.validate(value)
}

func NewSpecification(schema ObjectSchema) Specification {
	return Specification{
		ObjectSchema: schema,
	}
}

func ResolveSpecification(name string) (*Specification, error) {
	specification, ok := specifications[name]
	if !ok {
		return nil, fmt.Errorf("specification '%s' not found", name)
	}

	return &specification, nil
}
