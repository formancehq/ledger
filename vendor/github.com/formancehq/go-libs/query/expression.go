package query

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type Context interface {
	BuildMatcher(key, operator string, value any) (string, []any, error)
}
type ContextFn func(key, operator string, value any) (string, []any, error)

func (fn ContextFn) BuildMatcher(key, operator string, value any) (string, []any, error) {
	return fn(key, operator, value)
}

type Builder interface {
	Build(Context) (string, []any, error)
	Walk(f func(operator string, key string, value any) error) error
}

type set struct {
	operator string
	items    []Builder
}

func (set set) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"$" + set.operator: set.items,
	})
}

var _ Builder = (*set)(nil)

func (set set) Walk(f func(operator string, key string, value any) error) error {
	for _, item := range set.items {
		if err := item.Walk(f); err != nil {
			return err
		}
	}

	return nil
}

func (set set) Build(ctx Context) (string, []any, error) {
	if len(set.items) == 0 {
		return "1 = 1", nil, nil
	}

	clauses := make([]string, 0)
	args := make([]any, 0)
	for _, builder := range set.items {
		clause, clauseArgs, err := builder.Build(ctx)
		if err != nil {
			return "", nil, err
		}

		if clause == "" {
			continue
		}

		clauses = append(clauses, clause)
		args = append(args, clauseArgs...)
	}
	return "(" + strings.Join(clauses, fmt.Sprintf(") %s (", set.operator)) + ")", args, nil
}

type keyValue struct {
	operator string
	key      string
	value    any
}

func (kv keyValue) Walk(f func(operator string, key string, value any) error) error {
	return f(kv.operator, kv.key, kv.value)
}

func (kv keyValue) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		kv.operator: map[string]any{
			kv.key: kv.value,
		},
	})
}

var _ Builder = (*keyValue)(nil)

func (kv keyValue) Build(ctx Context) (string, []any, error) {
	return ctx.BuildMatcher(kv.key, kv.operator, kv.value)
}

type not struct {
	expression Builder
}

func (not not) Walk(f func(operator string, key string, value any) error) error {
	return not.expression.Walk(f)
}

func (not not) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"$not": not.expression,
	})
}

var _ Builder = (*not)(nil)

func (not not) Build(context Context) (string, []any, error) {
	sub, args, err := not.expression.Build(context)
	if err != nil {
		return "", nil, err
	}
	return fmt.Sprintf("not (%s)", sub), args, nil
}

func Not(expr Builder) not {
	return not{
		expression: expr,
	}
}

func Match(key string, value any) keyValue {
	return keyValue{
		operator: "$match",
		key:      key,
		value:    value,
	}
}

func Or(items ...Builder) set {
	return set{
		operator: "or",
		items:    items,
	}
}

func And(items ...Builder) set {
	return set{
		operator: "and",
		items:    items,
	}
}

func Lt(key string, value any) keyValue {
	return keyValue{
		operator: "$lt",
		key:      key,
		value:    value,
	}
}

func Lte(key string, value any) keyValue {
	return keyValue{
		operator: "$lte",
		key:      key,
		value:    value,
	}
}

func Gt(key string, value any) keyValue {
	return keyValue{
		operator: "$gt",
		key:      key,
		value:    value,
	}
}

func Gte(key string, value any) keyValue {
	return keyValue{
		operator: "$gte",
		key:      key,
		value:    value,
	}
}

func Exists(key string, value any) keyValue {
	return keyValue{
		operator: "$exists",
		key:      key,
		value:    value,
	}
}

func singleKey(m map[string]any) (string, any, error) {
	switch {
	case len(m) == 0:
		return "", nil, fmt.Errorf("expected single key, found none")
	case len(m) > 1:
		return "", nil, fmt.Errorf("expected single key, found more then one")
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

func parseSet(operator string, value any) (set, error) {
	set := set{
		operator: operator[1:],
	}
	switch value := value.(type) {
	case []any:
		for ind, sub := range value {
			switch sub := sub.(type) {
			case map[string]any:
				subExpression, err := mapMapToExpression(sub)
				if err != nil {
					return set, err
				}
				set.items = append(set.items, subExpression)
			default:
				return set, fmt.Errorf("unexpected type %T when decoding %s clause at index %d", value, operator, ind)
			}
		}
		return set, nil
	default:
		return set, fmt.Errorf("unexpected type %T", value)
	}
}

func parseKeyValue(operator string, m any) (keyValue, error) {
	kv := keyValue{
		operator: operator,
	}
	switch m := m.(type) {
	case map[string]any:
		key, value, err := singleKey(m)
		if err != nil {
			return kv, err
		}
		kv.key = key
		kv.value = value
		return kv, nil
	default:
		return kv, fmt.Errorf("unexpected type %T", m)
	}
}

func parseNot(m any) (not, error) {
	switch m := m.(type) {
	case map[string]any:
		expression, err := mapMapToExpression(m)
		if err != nil {
			return not{}, err
		}
		return not{
			expression: expression,
		}, nil
	default:
		return not{}, fmt.Errorf("unexpected type %T", m)
	}
}

func mapMapToExpression(m map[string]any) (Builder, error) {
	operator, value, err := singleKey(m)
	if err != nil {
		return nil, err
	}
	switch operator {
	case "$and", "$or":
		and, err := parseSet(operator, value)
		if err != nil {
			return nil, errors.Wrap(err, "parsing $and")
		}
		return and, nil
	case "$match", "$gte", "$lte", "$gt", "$lt", "$exists":
		match, err := parseKeyValue(operator, value)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing %s", operator)
		}
		return match, nil
	case "$not":
		match, err := parseNot(value)
		if err != nil {
			return nil, errors.Wrapf(err, "parsing %s", operator)
		}
		return match, nil
	default:
		return nil, fmt.Errorf("unexpected operator %s", operator)
	}
}

func ParseJSON(data string) (Builder, error) {
	if len(data) == 0 {
		return nil, nil
	}
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		return nil, err
	}

	if len(m) == 0 {
		return nil, nil
	}

	return mapMapToExpression(m)
}
