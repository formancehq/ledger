package compiler

import (
	"errors"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/script/parser"
	"github.com/numary/ledger/pkg/machine/vm/program"
)

type FallbackAccount core.Address

// CompileValueAwareSource returns the resource addresses of all the accounts
func (p *parseVisitor) CompileValueAwareSource(c parser.IValueAwareSourceContext) (program.ValueAwareSource, *CompileError) {
	switch c := c.(type) {
	case *parser.SrcContext:
		src, _, err := p.CompileSource(c.Source())
		return program.ValueAwareSourceSource{
			Source: src,
		}, err
	case *parser.SrcAllotmentContext:
		parts := program.ValueAwareSourceAllotment{}
		portions, err := p.CompileAllotment(c.SourceAllotment(), c.SourceAllotment().GetPortions())
		if err != nil {
			return nil, err
		}
		sources := c.SourceAllotment().GetSources()
		n := len(sources)
		for i := 0; i < n; i++ {
			src, _, compErr := p.CompileSource(sources[i])
			if compErr != nil {
				return nil, compErr
			}
			parts = append(parts, program.ValueAwareSourcePart{
				Portion: portions[i],
				Source:  src,
			})
		}
		return parts, nil
	}
	return nil, nil
}

// CompileSource returns the resource addresses of all the accounts,
// the addresses of accounts already emptied,
// and possibly a fallback account if the source has an unbounded overdraft allowance or contains @world
func (p *parseVisitor) CompileSource(c parser.ISourceContext) (program.Source, bool, *CompileError) {
	fallback := false
	switch c := c.(type) {
	case *parser.SrcAccountContext:
		account, compErr := p.CompileExprTy(c.SourceAccount().GetAccount(), core.TypeAccount)
		if compErr != nil {
			return nil, false, compErr
		}
		if p.isWorld(c.SourceAccount().GetAccount()) {
			fallback = true
		}
		var overdraft *program.Overdraft
		if c.SourceAccount().GetOverdraft() != nil {
			if fallback {
				return nil, false, LogicError(c, errors.New("this account already has an unlimited overdraft"))
			}
			switch c := c.SourceAccount().GetOverdraft().(type) {
			case *parser.SrcAccountOverdraftSpecificContext:
				mon, err := p.CompileExprTy(c.GetSpecific(), core.TypeMonetary)
				if err != nil {
					return nil, false, err
				}
				overdraft = &program.Overdraft{
					Unbounded: false,
					UpTo:      &mon,
				}
			case *parser.SrcAccountOverdraftUnboundedContext:
				overdraft = &program.Overdraft{
					Unbounded: true,
					UpTo:      nil,
				}
			}
		}
		return program.SourceAccount{
			Account:   account,
			Overdraft: overdraft,
		}, fallback, nil
	case *parser.SrcMaxedContext:
		src, _, err := p.CompileSource(c.SourceMaxed().GetSrc())
		if err != nil {
			return nil, false, err
		}
		max, err := p.CompileExprTy(c.SourceMaxed().GetMax(), core.TypeMonetary)
		if err != nil {
			return nil, false, err
		}
		return program.SourceMaxed{
			Source: src,
			Max:    max,
		}, false, nil
	case *parser.SrcInOrderContext:
		sources := c.SourceInOrder().GetSources()

		resSources := []program.Source{}
		fallback := false

		n := len(sources)
		for i := 0; i < n; i++ {
			if fallback {
				return nil, false, LogicError(c, errors.New("source is already unlimited at this point"))
			}
			subsource, subsourceFallback, err := p.CompileSource(sources[i])
			if err != nil {
				return nil, false, err
			}
			resSources = append(resSources, subsource)
			fallback = fallback || subsourceFallback
		}
		return program.SourceInOrder(resSources), fallback, nil
	}
	return nil, false, InternalError(c)
}
