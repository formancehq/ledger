package queries

import (
	"errors"
	"fmt"
	"regexp"
	"slices"

	"github.com/formancehq/go-libs/v3/pointer"
)

const (
	OperatorMatch  = "$match"
	OperatorIn     = "$in"
	OperatorExists = "$exists"
	OperatorLike   = "$like"
	OperatorLT     = "$lt"
	OperatorGT     = "$gt"
	OperatorLTE    = "$lte"
	OperatorGTE    = "$gte"
)

type EntitySchema struct {
	Fields map[string]Field
}

func (s EntitySchema) GetFieldByNameOrAlias(name string) (string, *Field) {
	for fieldName, field := range s.Fields {
		if fieldName == name || slices.Contains(field.Aliases, name) {
			return fieldName, &field
		}
	}

	return "", nil
}

func (s EntitySchema) GetFieldType(access string) (*ValueType, error) {
	key, idx, err := parseAccess(access)
	if err != nil {
		return nil, err
	}
	_, field := s.GetFieldByNameOrAlias(key)
	if field == nil {
		return nil, fmt.Errorf("unknown field: %s", key)
	}
	if idx != "" && !field.Type.IsIndexable() {
		return nil, fmt.Errorf("unexpected field indexing: %s", access)
	}
	return pointer.For(field.Type.ValueType()), nil
}

var accessRegex = regexp.MustCompile(`^([a-z_]+)(?:\[([a-zA-Z0-9_/]+)\])?$`)

func parseAccess(input string) (string, string, error) {
	m := accessRegex.FindStringSubmatch(input)
	if m == nil {
		return "", "", errors.New("invalid field name")
	}
	return m[1], m[2], nil
}
