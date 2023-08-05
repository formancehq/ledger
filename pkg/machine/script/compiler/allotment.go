package compiler

import (
	"errors"
	"math/big"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/numary/ledger/pkg/machine/internal"
	"github.com/numary/ledger/pkg/machine/script/parser"
	"github.com/numary/ledger/pkg/machine/vm/program"
)

func (p *parseVisitor) CompileAllotment(c antlr.ParserRuleContext, portions []parser.IAllotmentPortionContext) ([]program.AllotmentPortion, *CompileError) {
	resPortions := []program.AllotmentPortion{}
	total := big.NewRat(0, 1)
	hasVariable := false
	hasRemaining := false
	for i := 0; i < len(portions); i++ {
		c := portions[i]
		switch c := c.(type) {
		case *parser.AllotmentPortionConstContext:
			portion, err := internal.ParsePortionSpecific(c.GetText())
			if err != nil {
				return nil, LogicError(c, err)
			}
			rat := *portion.Specific
			total.Add(&rat, total)
			resPortions = append(resPortions, program.AllotmentPortion{
				Expr:      program.ExprLiteral{Value: *portion},
				Remaining: false,
			})
		case *parser.AllotmentPortionVarContext:
			name := c.GetPor().GetText()[1:] // strip '$' prefix
			ty, ok := p.vars[name]
			if !ok {
				return nil, LogicError(c, errors.New("variable not declared"))
			}
			if ty != internal.TypePortion {
				return nil, LogicError(c, errors.New("wrong type, expected portion"))
			}
			portion := program.ExprVariable(name)
			resPortions = append(resPortions, program.AllotmentPortion{
				Expr:      portion,
				Remaining: false,
			})
			hasVariable = true
		case *parser.AllotmentPortionRemainingContext:
			if hasRemaining {
				return nil, LogicError(c,
					errors.New("two uses of `remaining` in the same allocation"),
				)
			}
			resPortions = append(resPortions, program.AllotmentPortion{
				Expr:      nil,
				Remaining: true,
			})
			hasRemaining = true
		}
	}
	if total.Cmp(big.NewRat(1, 1)) == 1 {
		return nil, LogicError(c,
			errors.New("the sum of known portions is greater than 100%"),
		)
	}
	if total.Cmp(big.NewRat(1, 1)) == -1 && !hasRemaining {
		return nil, LogicError(c,
			errors.New("the sum of portions might be less than 100%"),
		)
	}
	if total.Cmp(big.NewRat(1, 1)) == 0 && hasVariable {
		return nil, LogicError(c,
			errors.New("the sum of portions might be greater than 100%"),
		)
	}
	if total.Cmp(big.NewRat(1, 1)) == 0 && hasRemaining {
		return nil, LogicError(c,
			errors.New("known portions are already equal to 100%"),
		)
	}
	return resPortions, nil
}
