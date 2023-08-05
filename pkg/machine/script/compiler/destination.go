package compiler

import (
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/script/parser"
	"github.com/numary/ledger/pkg/machine/vm/program"
)

func (p *parseVisitor) CompileDestination(c parser.IDestinationContext) (program.Destination, *CompileError) {
	switch c := c.(type) {
	case *parser.DestAccountContext:
		account, err := p.CompileExprTy(c.Expression(), core.TypeAccount)
		if err != nil {
			return nil, err
		}
		return program.DestinationAccount{Expr: account}, nil
	case *parser.DestInOrderContext:
		parts := []program.DestinationInOrderPart{}
		dests := c.DestinationInOrder().GetDests()
		amounts := c.DestinationInOrder().GetAmounts()
		for i := 0; i < len(dests); i++ {
			amount, err := p.CompileExprTy(amounts[i], core.TypeMonetary)
			if err != nil {
				return nil, err
			}
			kod, err := p.VisitKeptOrDestination(dests[i])
			if err != nil {
				return nil, err
			}
			parts = append(parts, program.DestinationInOrderPart{
				Max: amount,
				Kod: *kod,
			})
		}
		remainingKod, err := p.VisitKeptOrDestination(c.DestinationInOrder().GetRemainingDest())
		if err != nil {
			return nil, err
		}
		return program.DestinationInOrder{
			Parts:     parts,
			Remaining: *remainingKod,
		}, nil
	case *parser.DestAllotmentContext:
		parts := []program.DestinationAllotmentPart{}
		portions, err := p.CompileAllotment(c, c.DestinationAllotment().GetPortions())
		if err != nil {
			return nil, err
		}
		for i, dest := range c.DestinationAllotment().GetDests() {
			kod, err := p.VisitKeptOrDestination(dest)
			if err != nil {
				return nil, err
			}
			parts = append(parts, program.DestinationAllotmentPart{
				Portion: portions[i],
				Kod:     *kod,
			})
		}
		return program.DestinationAllotment(parts), nil
	}
	return nil, InternalError(c)
}

func (p *parseVisitor) VisitKeptOrDestination(c parser.IKeptOrDestinationContext) (*program.KeptOrDestination, *CompileError) {
	switch c := c.(type) {
	case *parser.IsDestinationContext:
		dest, err := p.CompileDestination(c.Destination())
		if err != nil {
			return nil, err
		}
		return &program.KeptOrDestination{
			Kept:        false,
			Destination: dest,
		}, nil
	case *parser.IsKeptContext:
		return &program.KeptOrDestination{
			Kept:        true,
			Destination: nil,
		}, nil
	}
	return nil, InternalError(c)
}
