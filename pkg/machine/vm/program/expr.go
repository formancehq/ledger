package program

import "github.com/numary/ledger/pkg/machine/internal"

const (
	OP_ADD = byte(iota + 1)
	OP_SUB
	OP_EQ
	OP_NEQ
	OP_LT
	OP_LTE
	OP_GT
	OP_GTE
	OP_NOT
	OP_AND
	OP_OR
)

type Expr interface {
	isExpr()
}

type ExprLiteral struct {
	Value internal.Value
}

func (e ExprLiteral) isExpr() {}

// Arithmetic

type ExprNumberOperation struct {
	Op  byte
	Lhs Expr
	Rhs Expr
}

func (e ExprNumberOperation) isExpr() {}

type ExprMonetaryOperation struct {
	Op  byte
	Lhs Expr
	Rhs Expr
}

func (e ExprMonetaryOperation) isExpr() {}

// Conditionals

type ExprNumberCondition struct {
	Lhs Expr
	Op  byte
	Rhs Expr
}

func (e ExprNumberCondition) isExpr() {}

// Logical operations

type ExprLogicalNot struct {
	Operand Expr
}
type ExprLogicalAnd struct {
	Lhs Expr
	Rhs Expr
}
type ExprLogicalOr struct {
	Lhs Expr
	Rhs Expr
}

func (e ExprLogicalNot) isExpr() {}
func (e ExprLogicalAnd) isExpr() {}
func (e ExprLogicalOr) isExpr()  {}

// Other

type ExprVariable string

func (e ExprVariable) isExpr() {}

type ExprTake struct {
	Amount Expr
	Source ValueAwareSource
}

func (e ExprTake) isExpr() {}

type ExprTakeAll struct {
	Asset  Expr
	Source Source
}

func (e ExprTakeAll) isExpr() {}

type ExprMonetaryNew struct {
	Asset  Expr
	Amount Expr
}

func (e ExprMonetaryNew) isExpr() {}

type ExprTernary struct {
	Cond    Expr
	IfTrue  Expr
	IfFalse Expr
}

func (e ExprTernary) isExpr() {}
