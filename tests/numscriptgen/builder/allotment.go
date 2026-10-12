package builder

import "math/big"

// A portion literal, e.g. `1/3`. Wrap it with ExprPortion to use it in an
// allotment clause; ExprPortionVar is the var-form alternative.
type Portion struct {
	num, denom *big.Int
}

func NewPortion(num, denom *big.Int) Portion {
	return Portion{num: num, denom: denom}
}

func (p Portion) render(env *env) {
	env.builder.WriteString(p.num.String())
	env.builder.WriteByte('/')
	env.builder.WriteString(p.denom.String())
}

// payload is the underlying shape shared by Source and KeptOrDest: both are
// render closures. AllotmentClause is generic over it so the same type can
// back both `sourceAllotment` and `destinationAllotment` clauses.
type payload interface {
	~func(*env, int)
}

// One clause of an allotment block: a portion plus its payload (a Source in
// source position, a KeptOrDest in destination position). Portion is an
// expression, not a literal, so a clause can be written either inline
// (ExprPortion) or through a var (ExprPortionVar).
type AllotmentClause[T payload] struct {
	Portion Expression[ExprTypePortion]
	Payload T
}
