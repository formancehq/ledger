package compiler

import (
	"errors"
	"fmt"
	"math/big"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/script/parser"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
)

func (p *parseVisitor) VisitAllotment(c antlr.ParserRuleContext, portions []parser.IAllotmentPortionContext) *CompileError {
	total := big.NewRat(0, 1)
	hasVariable := false
	hasRemaining := false
	for i := len(portions) - 1; i >= 0; i-- {
		c := portions[i]
		switch c := c.(type) {
		case *parser.AllotmentPortionConstContext:
			portion, err := core.ParsePortionSpecific(c.GetText())
			if err != nil {
				return LogicError(c, err)
			}
			rat := *portion.Specific
			total.Add(&rat, total)
			addr, err := p.AllocateResource(program.Constant{Inner: *portion})
			if err != nil {
				return LogicError(c, err)
			}
			p.PushAddress(*addr)
		case *parser.AllotmentPortionVarContext:
			ty, _, err := p.VisitVariable(c.GetPor(), true)
			if err != nil {
				return err
			}
			if ty != core.TypePortion {
				return LogicError(c,
					fmt.Errorf("wrong type: expected type portion for variable: %v", ty),
				)
			}
			hasVariable = true
		case *parser.AllotmentPortionRemainingContext:
			if hasRemaining {
				return LogicError(c,
					errors.New("two uses of `remaining` in the same allocation"),
				)
			}
			addr, err := p.AllocateResource(program.Constant{Inner: core.NewPortionRemaining()})
			if err != nil {
				return LogicError(c, err)
			}
			p.PushAddress(*addr)
			hasRemaining = true
		}
	}
	if total.Cmp(big.NewRat(1, 1)) == 1 {
		return LogicError(c,
			errors.New("the sum of known portions is greater than 100%"),
		)
	}
	if total.Cmp(big.NewRat(1, 1)) == -1 && !hasRemaining {
		return LogicError(c,
			errors.New("the sum of portions might be less than 100%"),
		)
	}
	if total.Cmp(big.NewRat(1, 1)) == 0 && hasVariable {
		return LogicError(c,
			errors.New("the sum of portions might be greater than 100%"),
		)
	}
	if total.Cmp(big.NewRat(1, 1)) == 0 && hasRemaining {
		return LogicError(c,
			errors.New("known portions are already equal to 100%"),
		)
	}
	err := p.PushInteger(core.NewNumber(int64(len(portions))))
	if err != nil {
		return LogicError(c, err)
	}
	p.AppendInstruction(program.OP_MAKE_ALLOTMENT)
	return nil
}
