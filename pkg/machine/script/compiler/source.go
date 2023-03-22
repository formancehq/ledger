package compiler

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/machine/script/parser"
	"github.com/formancehq/ledger/pkg/machine/vm/program"
)

type FallbackAccount core.Address

func (p *parseVisitor) TakeFromSource(fallback *FallbackAccount) error {
	if fallback == nil {
		p.AppendInstruction(program.OP_TAKE)
		err := p.Bump(1)
		if err != nil {
			return err
		}
		p.AppendInstruction(program.OP_REPAY)
		return nil
	}

	p.AppendInstruction(program.OP_TAKE_MAX)
	err := p.Bump(1)
	if err != nil {
		return err
	}
	p.AppendInstruction(program.OP_REPAY)
	p.PushAddress(core.Address(*fallback))
	err = p.Bump(2)
	if err != nil {
		return err
	}
	p.AppendInstruction(program.OP_TAKE_ALWAYS)
	err = p.PushInteger(core.NewNumber(2))
	if err != nil {
		return err
	}
	p.AppendInstruction(program.OP_FUNDING_ASSEMBLE)
	return nil
}

// VisitSource returns the resource addresses of all the accounts,
// the addresses of accounts already emptied,
// and possibly a fallback account if the source has an unbounded overdraft allowance or contains @world
func (p *parseVisitor) VisitSource(c parser.ISourceContext, pushAsset func(), isAll bool) (map[core.Address]struct{}, map[core.Address]struct{}, *FallbackAccount, *CompileError) {
	neededAccounts := map[core.Address]struct{}{}
	emptiedAccounts := map[core.Address]struct{}{}
	var fallback *FallbackAccount
	switch c := c.(type) {
	case *parser.SrcAccountContext:
		ty, accAddr, compErr := p.VisitExpr(c.SourceAccount().GetAccount(), true)
		if compErr != nil {
			return nil, nil, nil, compErr
		}
		if ty != core.TypeAccount {
			return nil, nil, nil, LogicError(c, errors.New("wrong type: expected account or allocation as destination"))
		}
		if p.isWorld(*accAddr) {
			f := FallbackAccount(*accAddr)
			fallback = &f
		}

		overdraft := c.SourceAccount().GetOverdraft()
		if overdraft == nil {
			// no overdraft: use zero monetary
			pushAsset()
			err := p.PushInteger(core.NewNumber(0))
			if err != nil {
				return nil, nil, nil, LogicError(c, err)
			}
			p.AppendInstruction(program.OP_MONETARY_NEW)
			p.AppendInstruction(program.OP_TAKE_ALL)
		} else {
			if p.isWorld(*accAddr) {
				return nil, nil, nil, LogicError(c, errors.New("@world is already set to an unbounded overdraft"))
			}
			switch c := overdraft.(type) {
			case *parser.SrcAccountOverdraftSpecificContext:
				ty, _, compErr := p.VisitExpr(c.GetSpecific(), true)
				if compErr != nil {
					return nil, nil, nil, compErr
				}
				if ty != core.TypeMonetary {
					return nil, nil, nil, LogicError(c, errors.New("wrong type: expected monetary"))
				}
				p.AppendInstruction(program.OP_TAKE_ALL)
			case *parser.SrcAccountOverdraftUnboundedContext:
				pushAsset()
				err := p.PushInteger(core.NewNumber(0))
				if err != nil {
					return nil, nil, nil, LogicError(c, err)
				}
				p.AppendInstruction(program.OP_MONETARY_NEW)
				p.AppendInstruction(program.OP_TAKE_ALL)
				f := FallbackAccount(*accAddr)
				fallback = &f
			}
		}
		neededAccounts[*accAddr] = struct{}{}
		emptiedAccounts[*accAddr] = struct{}{}

		if fallback != nil && isAll {
			return nil, nil, nil, LogicError(c, errors.New("cannot take all balance of an unbounded source"))
		}

	case *parser.SrcMaxedContext:
		accounts, _, subsourceFallback, compErr := p.VisitSource(c.SourceMaxed().GetSrc(), pushAsset, false)
		if compErr != nil {
			return nil, nil, nil, compErr
		}
		ty, _, compErr := p.VisitExpr(c.SourceMaxed().GetMax(), true)
		if compErr != nil {
			return nil, nil, nil, compErr
		}
		if ty != core.TypeMonetary {
			return nil, nil, nil, LogicError(c, errors.New("wrong type: expected monetary as max"))
		}
		for k, v := range accounts {
			neededAccounts[k] = v
		}
		p.AppendInstruction(program.OP_TAKE_MAX)
		err := p.Bump(1)
		if err != nil {
			return nil, nil, nil, LogicError(c, err)
		}
		p.AppendInstruction(program.OP_REPAY)
		if subsourceFallback != nil {
			p.PushAddress(core.Address(*subsourceFallback))
			err := p.Bump(2)
			if err != nil {
				return nil, nil, nil, LogicError(c, err)
			}
			p.AppendInstruction(program.OP_TAKE_ALL)
			err = p.PushInteger(core.NewNumber(2))
			if err != nil {
				return nil, nil, nil, LogicError(c, err)
			}
			p.AppendInstruction(program.OP_FUNDING_ASSEMBLE)
		} else {
			err := p.Bump(1)
			if err != nil {
				return nil, nil, nil, LogicError(c, err)
			}
			p.AppendInstruction(program.OP_DELETE)
		}
	case *parser.SrcInOrderContext:
		sources := c.SourceInOrder().GetSources()
		n := len(sources)
		for i := 0; i < n; i++ {
			accounts, emptied, subsourceFallback, compErr := p.VisitSource(sources[i], pushAsset, isAll)
			if compErr != nil {
				return nil, nil, nil, compErr
			}
			fallback = subsourceFallback
			if subsourceFallback != nil && i != n-1 {
				return nil, nil, nil, LogicError(c, errors.New("an unbounded subsource can only be in last position"))
			}
			for k, v := range accounts {
				neededAccounts[k] = v
			}
			for k, v := range emptied {
				if _, ok := emptiedAccounts[k]; ok {
					return nil, nil, nil, LogicError(sources[i], fmt.Errorf("%v is already empty at this stage", p.resources[k]))
				}
				emptiedAccounts[k] = v
			}
		}
		err := p.PushInteger(core.NewNumber(int64(n)))
		if err != nil {
			return nil, nil, nil, LogicError(c, err)
		}
		p.AppendInstruction(program.OP_FUNDING_ASSEMBLE)
	}
	for address := range neededAccounts {
		p.sources[address] = struct{}{}
	}
	return neededAccounts, emptiedAccounts, fallback, nil
}
