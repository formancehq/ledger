package core

import (
	"errors"
	"reflect"
	"strings"
)

const (
	PolicyAccept = "accept"
	PolicyDeny   = "deny"
)

type EvalContext struct {
	Variables map[string]interface{}
	Metadata  Metadata
}

type Rule struct {
	Account string
	Policy  string
	Expr    Expr
}

type Expr interface {
	Eval(EvalContext) bool
}

type Value interface {
	eval(ctx EvalContext) interface{}
}

type or struct {
	exprs []Expr
}

func (o *or) Eval(ctx EvalContext) bool {
	for _, e := range o.exprs {
		if e.Eval(ctx) {
			return true
		}
	}
	return false
}

type and struct {
	exprs []Expr
}

func (o *and) Eval(ctx EvalContext) bool {
	for _, e := range o.exprs {
		if !e.Eval(ctx) {
			return false
		}
	}
	return true
}

type eq struct {
	Op1 Value
	Op2 Value
}

func (o *eq) Eval(ctx EvalContext) bool {
	return reflect.DeepEqual(o.Op1.eval(ctx), o.Op2.eval(ctx))
}

type gt struct {
	Op1 Value
	Op2 Value
}

func (o *gt) Eval(ctx EvalContext) bool {
	return o.Op1.eval(ctx).(int) > o.Op2.eval(ctx).(int)
}

type constantExpr struct {
	v interface{}
}

func (e constantExpr) eval(ctx EvalContext) interface{} {
	return e.v
}

type variableExpr struct {
	name string
}

func (e variableExpr) eval(ctx EvalContext) interface{} {
	return ctx.Variables[e.name]
}

type metaExpr struct {
	name string
}

func (e metaExpr) eval(ctx EvalContext) interface{} {
	return string(ctx.Metadata[e.name])
}

func parse(v interface{}) (expr interface{}, err error) {
	switch vv := v.(type) {
	case map[string]interface{}:
		if len(vv) != 1 {
			return nil, errors.New("malformed expression")
		}
		for key, vvv := range vv {
			switch {
			case strings.HasPrefix(key, "$"):
				switch key {
				case "$meta":
					value, ok := vvv.(string)
					if !ok {
						return nil, errors.New("$meta operator invalid")
					}
					return &metaExpr{name: value}, nil
				case "$or", "$and":
					slice, ok := vvv.([]interface{})
					if !ok {
						return nil, errors.New("Expected slice for operator " + key)
					}
					exprs := make([]Expr, 0)
					for _, item := range slice {
						r, err := parse(item)
						if err != nil {
							return nil, err
						}
						expr, ok := r.(Expr)
						if !ok {
							return nil, errors.New("unexpected value when parsing " + key)
						}
						exprs = append(exprs, expr)
					}
					switch key {
					case "$and":
						expr = &and{exprs: exprs}
					case "$or":
						expr = &or{exprs: exprs}
					}
				case "$eq", "$gt", "$lt":
					vv, ok := vvv.([]interface{})
					if !ok {
						return nil, errors.New("expected array when using $eq")
					}
					if len(vv) != 2 {
						return nil, errors.New("expected 2 items when using $eq")
					}
					op1, err := parse(vv[0])
					if err != nil {
						return nil, err
					}
					op1Value, ok := op1.(Value)
					if !ok {
						return nil, errors.New("op1 must be valuable")
					}
					op2, err := parse(vv[1])
					if err != nil {
						return nil, err
					}
					op2Value, ok := op2.(Value)
					if !ok {
						return nil, errors.New("op2 must be valuable")
					}
					switch key {
					case "$eq":
						expr = &eq{
							Op1: op1Value,
							Op2: op2Value,
						}
					case "$gt":
						expr = &gt{
							Op1: op1Value,
							Op2: op2Value,
						}
					}
				default:
					return nil, errors.New("unknown operator '" + key + "'")
				}
			}
		}
	case string:
		if !strings.HasPrefix(vv, "$") {
			return constantExpr{v}, nil
		}
		return variableExpr{vv[1:]}, nil
	default:
		return constantExpr{v}, nil
	}

	return expr, nil
}

func ParseRuleExpr(v map[string]interface{}) (Expr, error) {
	ret, err := parse(v)
	if err != nil {
		return nil, err
	}
	return ret.(Expr), nil
}
