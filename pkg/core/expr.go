package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

type EvalContext struct {
	Variables map[string]interface{}
	Metadata  Metadata
	Asset     string
}

type Expr interface {
	Eval(EvalContext) bool
}

type Value interface {
	eval(ctx EvalContext) interface{}
}

type ExprOr []Expr

func (o ExprOr) Eval(ctx EvalContext) bool {
	for _, e := range o {
		if e.Eval(ctx) {
			return true
		}
	}
	return false
}

func (e ExprOr) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$or": []Expr(e),
	})
}

type ExprAnd []Expr

func (o ExprAnd) Eval(ctx EvalContext) bool {
	for _, e := range o {
		if !e.Eval(ctx) {
			return false
		}
	}
	return true
}

func (e ExprAnd) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$and": []Expr(e),
	})
}

type ExprEq struct {
	Op1 Value
	Op2 Value
}

func (o *ExprEq) Eval(ctx EvalContext) bool {
	return reflect.DeepEqual(o.Op1.eval(ctx), o.Op2.eval(ctx))
}

func (e ExprEq) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$eq": []interface{}{e.Op1, e.Op2},
	})
}

type ExprGt struct {
	Op1 Value
	Op2 Value
}

func (o *ExprGt) Eval(ctx EvalContext) bool {
	return o.Op1.eval(ctx).(float64) > o.Op2.eval(ctx).(float64)
}

func (e ExprGt) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$gt": []interface{}{e.Op1, e.Op2},
	})
}

type ExprLt struct {
	Op1 Value
	Op2 Value
}

func (o *ExprLt) Eval(ctx EvalContext) bool {
	return o.Op1.eval(ctx).(float64) < o.Op2.eval(ctx).(float64)
}

func (e ExprLt) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$lt": []interface{}{e.Op1, e.Op2},
	})
}

type ExprGte struct {
	Op1 Value
	Op2 Value
}

func (o *ExprGte) Eval(ctx EvalContext) bool {
	return o.Op1.eval(ctx).(float64) >= o.Op2.eval(ctx).(float64)
}

func (e ExprGte) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$gte": []interface{}{e.Op1, e.Op2},
	})
}

type ExprLte struct {
	Op1 Value
	Op2 Value
}

func (o *ExprLte) Eval(ctx EvalContext) bool {
	return o.Op1.eval(ctx).(float64) <= o.Op2.eval(ctx).(float64)
}

func (e ExprLte) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$lte": []interface{}{e.Op1, e.Op2},
	})
}

type ConstantExpr struct {
	Value interface{}
}

func (e ConstantExpr) eval(ctx EvalContext) interface{} {
	return e.Value
}

func (e ConstantExpr) MarshalJSON() ([]byte, error) {
	return json.Marshal(e.Value)
}

type VariableExpr struct {
	Name string
}

func (e VariableExpr) eval(ctx EvalContext) interface{} {
	return ctx.Variables[e.Name]
}

func (e VariableExpr) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`"$%s"`, e.Name)), nil
}

type MetaExpr struct {
	Name string
}

func (e MetaExpr) eval(ctx EvalContext) interface{} {
	return string(ctx.Metadata[e.Name])
}

func (e MetaExpr) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"$meta": e.Name,
	})
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
					return &MetaExpr{Name: value}, nil
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
						expr = ExprAnd(exprs)
					case "$or":
						expr = ExprOr(exprs)
					}
				case "$eq", "$gt", "$gte", "$lt", "$lte":
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
						expr = &ExprEq{
							Op1: op1Value,
							Op2: op2Value,
						}
					case "$gt":
						expr = &ExprGt{
							Op1: op1Value,
							Op2: op2Value,
						}
					case "$gte":
						expr = &ExprGte{
							Op1: op1Value,
							Op2: op2Value,
						}
					case "$lt":
						expr = &ExprLt{
							Op1: op1Value,
							Op2: op2Value,
						}
					case "$lte":
						expr = &ExprLte{
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
			return ConstantExpr{v}, nil
		}
		return VariableExpr{vv[1:]}, nil
	default:
		return ConstantExpr{v}, nil
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

func ParseRule(data string) (Expr, error) {
	m := make(map[string]interface{})
	err := json.Unmarshal([]byte(data), &m)
	if err != nil {
		return nil, err
	}
	return ParseRuleExpr(m)
}
