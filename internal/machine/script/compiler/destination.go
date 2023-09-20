package compiler

import (
	"errors"

	internal2 "github.com/formancehq/ledger/internal/machine/internal"
	"github.com/formancehq/ledger/internal/machine/script/parser"
	"github.com/formancehq/ledger/internal/machine/vm/program"
)

func (p *parseVisitor) VisitDestination(c parser.IDestinationContext) *CompileError {
	err := p.VisitDestinationRecursive(c)
	if err != nil {
		return err
	}
	p.AppendInstruction(program.OP_REPAY)
	return nil
}

func (p *parseVisitor) VisitDestinationRecursive(c parser.IDestinationContext) *CompileError {
	switch c := c.(type) {
	case *parser.DestAccountContext:
		p.AppendInstruction(program.OP_FUNDING_SUM)
		p.AppendInstruction(program.OP_TAKE)
		ty, _, err := p.VisitExpr(c.Expression(), true)
		if err != nil {
			return err
		}
		if ty != internal2.TypeAccount {
			return LogicError(c,
				errors.New("wrong type: expected account as destination"),
			)
		}
		p.AppendInstruction(program.OP_SEND)
		return nil
	case *parser.DestInOrderContext:
		dests := c.DestinationInOrder().GetDests()
		amounts := c.DestinationInOrder().GetAmounts()
		n := len(dests)

		// initialize the `kept` accumulator
		p.AppendInstruction(program.OP_FUNDING_SUM)
		p.AppendInstruction(program.OP_ASSET)
		err := p.PushInteger(internal2.NewNumber(0))
		if err != nil {
			return LogicError(c, err)
		}
		p.AppendInstruction(program.OP_MONETARY_NEW)

		err = p.Bump(1)
		if err != nil {
			return LogicError(c, err)
		}

		for i := 0; i < n; i++ {
			ty, _, compErr := p.VisitExpr(amounts[i], true)
			if compErr != nil {
				return compErr
			}
			if ty != internal2.TypeMonetary {
				return LogicError(c, errors.New("wrong type: expected monetary as max"))
			}
			p.AppendInstruction(program.OP_TAKE_MAX)
			err := p.Bump(2)
			if err != nil {
				return LogicError(c, err)
			}
			p.AppendInstruction(program.OP_DELETE)
			compErr = p.VisitKeptOrDestination(dests[i])
			if compErr != nil {
				return compErr
			}
			p.AppendInstruction(program.OP_FUNDING_SUM)
			err = p.Bump(3)
			if err != nil {
				return LogicError(c, err)
			}
			p.AppendInstruction(program.OP_MONETARY_ADD)
			err = p.Bump(1)
			if err != nil {
				return LogicError(c, err)
			}
			err = p.Bump(2)
			if err != nil {
				return LogicError(c, err)
			}
			err = p.PushInteger(internal2.NewNumber(2))
			if err != nil {
				return LogicError(c, err)
			}
			p.AppendInstruction(program.OP_FUNDING_ASSEMBLE)
		}
		p.AppendInstruction(program.OP_FUNDING_REVERSE)
		err = p.Bump(1)
		if err != nil {
			return LogicError(c, err)
		}
		p.AppendInstruction(program.OP_TAKE)
		p.AppendInstruction(program.OP_FUNDING_REVERSE)
		err = p.Bump(1)
		if err != nil {
			return LogicError(c, err)
		}
		p.AppendInstruction(program.OP_FUNDING_REVERSE)
		cerr := p.VisitKeptOrDestination(c.DestinationInOrder().GetRemainingDest())
		if cerr != nil {
			return cerr
		}
		err = p.Bump(1)
		if err != nil {
			return LogicError(c, err)
		}
		err = p.PushInteger(internal2.NewNumber(2))
		if err != nil {
			return LogicError(c, err)
		}
		p.AppendInstruction(program.OP_FUNDING_ASSEMBLE)
		return nil
	case *parser.DestAllotmentContext:
		err := p.VisitDestinationAllotment(c.DestinationAllotment())
		return err
	default:
		return InternalError(c)
	}
}

func (p *parseVisitor) VisitKeptOrDestination(c parser.IKeptOrDestinationContext) *CompileError {
	switch c := c.(type) {
	case *parser.IsKeptContext:
		return nil
	case *parser.IsDestinationContext:
		err := p.VisitDestinationRecursive(c.Destination())
		return err
	default:
		return InternalError(c)
	}
}

func (p *parseVisitor) VisitDestinationAllotment(c parser.IDestinationAllotmentContext) *CompileError {
	p.AppendInstruction(program.OP_FUNDING_SUM)
	err := p.VisitAllotment(c, c.GetPortions())
	if err != nil {
		return err
	}
	p.AppendInstruction(program.OP_ALLOC)
	err = p.VisitAllocDestination(c.GetDests())
	if err != nil {
		return err
	}
	return nil
}

func (p *parseVisitor) VisitAllocDestination(dests []parser.IKeptOrDestinationContext) *CompileError {
	err := p.Bump(int64(len(dests)))
	if err != nil {
		return LogicError(dests[0], err)
	}
	for _, dest := range dests {
		err = p.Bump(1)
		if err != nil {
			return LogicError(dest, err)
		}
		p.AppendInstruction(program.OP_TAKE)
		compErr := p.VisitKeptOrDestination(dest)
		if compErr != nil {
			return compErr
		}
		err = p.Bump(1)
		if err != nil {
			return LogicError(dest, err)
		}
		err = p.PushInteger(internal2.NewNumber(2))
		if err != nil {
			return LogicError(dest, err)
		}
		p.AppendInstruction(program.OP_FUNDING_ASSEMBLE)
	}
	return nil
}
