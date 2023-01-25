package compiler

import (
	"fmt"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/formancehq/machine/core"
	"github.com/formancehq/machine/script/parser"
	"github.com/formancehq/machine/vm/program"
	"github.com/pkg/errors"
)

type parseVisitor struct {
	errListener    *ErrorListener
	instructions   []byte
	resources      []program.Resource                         // must not exceed 65536 elements
	varIdx         map[string]core.Address                    // maps name to resource index
	neededBalances map[core.Address]map[core.Address]struct{} // for each account, set of assets needed
}

// Allocates constants if it hasn't already been,
// and returns its resource address.
func (p *parseVisitor) findConstant(constant program.Constant) (*core.Address, bool) {
	for i := 0; i < len(p.resources); i++ {
		if c, ok := p.resources[i].(program.Constant); ok {
			if core.ValueEquals(c.Inner, constant.Inner) {
				addr := core.Address(i)
				return &addr, true
			}
		}
	}
	return nil, false
}

func (p *parseVisitor) AllocateResource(res program.Resource) (*core.Address, error) {
	if c, ok := res.(program.Constant); ok {
		idx, ok := p.findConstant(c)
		if ok {
			return idx, nil
		}
	}
	if len(p.resources) >= 65536 {
		return nil, errors.New("number of unique constants exceeded 65536")
	}
	p.resources = append(p.resources, res)
	addr := core.NewAddress(uint16(len(p.resources) - 1))
	return &addr, nil
}

func (p *parseVisitor) isWorld(addr core.Address) bool {
	idx := int(addr)
	if idx < len(p.resources) {
		if c, ok := p.resources[idx].(program.Constant); ok {
			if acc, ok := c.Inner.(core.Account); ok {
				if string(acc) == "world" {
					return true
				}
			}
		}
	}
	return false
}

func (p *parseVisitor) VisitVariable(c parser.IVariableContext, push bool) (core.Type, *core.Address, *CompileError) {
	name := c.GetText()[1:] // strip '$' prefix
	if idx, ok := p.varIdx[name]; ok {
		res := p.resources[idx]
		if push {
			p.PushAddress(idx)
		}
		return res.GetType(), &idx, nil
	} else {
		return 0, nil, LogicError(c, errors.New("variable not declared"))
	}
}

func (p *parseVisitor) VisitExpr(c parser.IExpressionContext, push bool) (core.Type, *core.Address, *CompileError) {
	switch c := c.(type) {
	case *parser.ExprAddSubContext:
		ty, _, err := p.VisitExpr(c.GetLhs(), push)
		if err != nil {
			return 0, nil, err
		}
		if ty != core.TypeNumber {
			return 0, nil, LogicError(c, errors.New("tried to do arithmetic with wrong type"))
		}
		ty, _, err = p.VisitExpr(c.GetRhs(), push)
		if err != nil {
			return 0, nil, err
		}
		if ty != core.TypeNumber {
			return 0, nil, LogicError(c, errors.New("tried to do arithmetic with wrong type"))
		}
		if push {
			switch c.GetOp().GetTokenType() {
			case parser.NumScriptLexerOP_ADD:
				p.AppendInstruction(program.OP_IADD)
			case parser.NumScriptLexerOP_SUB:
				p.AppendInstruction(program.OP_ISUB)
			}
		}
		return core.TypeNumber, nil, nil
	case *parser.ExprLiteralContext:
		ty, addr, err := p.VisitLit(c.GetLit(), push)
		if err != nil {
			return 0, nil, err
		}
		return ty, addr, nil
	case *parser.ExprVariableContext:
		ty, addr, err := p.VisitVariable(c.GetVar_(), push)
		return ty, addr, err
	default:
		return 0, nil, InternalError(c)
	}
}

// pushes a value from a literal onto the stack
func (p *parseVisitor) VisitLit(c parser.ILiteralContext, push bool) (core.Type, *core.Address, *CompileError) {
	switch c := c.(type) {
	case *parser.LitAccountContext:
		account := core.Account(c.GetText()[1:])
		addr, err := p.AllocateResource(program.Constant{Inner: account})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return core.TypeAccount, addr, nil
	case *parser.LitAssetContext:
		asset := core.Asset(c.GetText())
		addr, err := p.AllocateResource(program.Constant{Inner: asset})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return core.TypeAsset, addr, nil
	case *parser.LitNumberContext:
		number, err := core.ParseNumber(c.GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			err := p.PushInteger(number)
			if err != nil {
				return 0, nil, LogicError(c, err)
			}
		}
		return core.TypeNumber, nil, nil
	case *parser.LitStringContext:
		addr, err := p.AllocateResource(program.Constant{
			Inner: core.String(strings.Trim(c.GetText(), `"`)),
		})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return core.TypeString, addr, nil
	case *parser.LitPortionContext:
		portion, err := core.ParsePortionSpecific(c.GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		addr, err := p.AllocateResource(program.Constant{Inner: *portion})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return core.TypePortion, addr, nil
	case *parser.LitMonetaryContext:
		asset := c.Monetary().GetAsset().GetText()
		amt, err := core.ParseMonetaryInt(c.Monetary().GetAmt().GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		monetary := core.Monetary{
			Asset:  core.Asset(asset),
			Amount: amt,
		}
		addr, err := p.AllocateResource(program.Constant{Inner: monetary})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return core.TypeMonetary, addr, nil
	default:
		return 0, nil, InternalError(c)
	}
}

// send statement
func (p *parseVisitor) VisitSend(c *parser.SendContext) *CompileError {
	var assetAddr core.Address
	var neededAccounts map[core.Address]struct{}
	if mon := c.GetMonAll(); mon != nil {
		asset := core.Asset(mon.GetAsset().GetText())
		addr, err := p.AllocateResource(program.Constant{Inner: asset})
		if err != nil {
			return LogicError(c, err)
		}
		assetAddr = *addr
		accounts, compErr := p.VisitValueAwareSource(c.GetSrc(), func() {
			p.PushAddress(*addr)
		}, nil)
		if compErr != nil {
			return compErr
		}
		neededAccounts = accounts
	}
	if mon := c.GetMon(); mon != nil {
		ty, monAddr, err := p.VisitExpr(c.GetMon(), false)
		if err != nil {
			return err
		}
		if ty != core.TypeMonetary {
			return LogicError(c, errors.New("wrong type for monetary value"))
		}
		assetAddr = *monAddr
		accounts, err := p.VisitValueAwareSource(c.GetSrc(), func() {
			p.PushAddress(*monAddr)
			p.AppendInstruction(program.OP_ASSET)
		}, monAddr)
		if err != nil {
			return err
		}
		neededAccounts = accounts
	}
	// add source accounts to the needed balances
	for acc := range neededAccounts {
		if b, ok := p.neededBalances[acc]; ok {
			b[assetAddr] = struct{}{}
		} else {
			p.neededBalances[acc] = map[core.Address]struct{}{
				assetAddr: {},
			}
		}
	}
	err := p.VisitDestination(c.GetDest())
	if err != nil {
		return err
	}
	return nil
}

// set_tx_meta statement
func (p *parseVisitor) VisitSetTxMeta(ctx *parser.SetTxMetaContext) *CompileError {
	_, _, compErr := p.VisitExpr(ctx.GetValue(), true)
	if compErr != nil {
		return compErr
	}

	keyAddr, err := p.AllocateResource(program.Constant{
		Inner: core.String(strings.Trim(ctx.GetKey().GetText(), `"`)),
	})
	if err != nil {
		return LogicError(ctx, err)
	}
	p.PushAddress(*keyAddr)

	p.AppendInstruction(program.OP_TX_META)

	return nil
}

// set_account_meta statement
func (p *parseVisitor) VisitSetAccountMeta(ctx *parser.SetAccountMetaContext) *CompileError {
	_, _, compErr := p.VisitExpr(ctx.GetValue(), true)
	if compErr != nil {
		return compErr
	}

	keyAddr, err := p.AllocateResource(program.Constant{
		Inner: core.String(strings.Trim(ctx.GetKey().GetText(), `"`)),
	})
	if err != nil {
		return LogicError(ctx, err)
	}
	p.PushAddress(*keyAddr)

	ty, accAddr, compErr := p.VisitExpr(ctx.GetAcc(), false)
	if compErr != nil {
		return compErr
	}
	if ty != core.TypeAccount {
		return LogicError(ctx, fmt.Errorf(
			"variable is of type %s, and should be of type account", ty))
	}
	p.PushAddress(*accAddr)

	p.AppendInstruction(program.OP_ACCOUNT_META)

	return nil
}

// print statement
func (p *parseVisitor) VisitPrint(ctx *parser.PrintContext) *CompileError {
	_, _, err := p.VisitExpr(ctx.GetExpr(), true)
	if err != nil {
		return err
	}

	p.AppendInstruction(program.OP_PRINT)

	return nil
}

// vars declaration block
func (p *parseVisitor) VisitVars(c *parser.VarListDeclContext) *CompileError {
	if len(c.GetV()) > 32768 {
		return LogicError(c, fmt.Errorf("number of variables exceeded %v", 32768))
	}

	for _, v := range c.GetV() {
		name := v.GetName().GetText()[1:]
		if _, ok := p.varIdx[name]; ok {
			return LogicError(c, errors.New("duplicate variable"))
		}
		var ty core.Type
		switch v.GetTy().GetText() {
		case "account":
			ty = core.TypeAccount
		case "number":
			ty = core.TypeNumber
		case "string":
			ty = core.TypeString
		case "monetary":
			ty = core.TypeMonetary
		case "portion":
			ty = core.TypePortion
		default:
			return InternalError(c)
		}

		var addr *core.Address
		var err error
		if v.GetOrig() == nil {
			addr, err = p.AllocateResource(program.Variable{Typ: ty, Name: name})
			if err != nil {
				return &CompileError{
					Msg: errors.Wrap(err,
						"allocating variable resource").Error(),
				}
			}
			p.varIdx[name] = *addr
			continue
		}

		switch c := v.GetOrig().(type) {
		case *parser.OriginAccountMetaContext:
			srcTy, src, compErr := p.VisitExpr(c.GetAccount(), false)
			if compErr != nil {
				return compErr
			}
			if srcTy != core.TypeAccount {
				return LogicError(c, errors.New(
					"variable type should be 'account' to pull account metadata"))
			}
			key := strings.Trim(c.GetKey().GetText(), `"`)
			addr, err = p.AllocateResource(program.VariableAccountMetadata{
				Typ:     ty,
				Name:    name,
				Account: *src,
				Key:     key,
			})
		case *parser.OriginAccountBalanceContext:
			if ty != core.TypeMonetary {
				return LogicError(c, fmt.Errorf(
					"variable type should be 'monetary' to pull account balance"))
			}
			accTy, accAddr, compErr := p.VisitExpr(c.GetAccount(), false)
			if compErr != nil {
				return compErr
			}
			if accTy != core.TypeAccount {
				return LogicError(c, errors.New(
					"variable type should be 'account' to pull account balance"))
			}

			asset := core.Asset(c.GetAsset().GetText())
			addr, err = p.AllocateResource(program.VariableAccountBalance{
				Name:    name,
				Account: *accAddr,
				Asset:   string(asset),
			})
			if err != nil {
				return LogicError(c, err)
			}
		}
		if err != nil {
			return LogicError(c, err)
		}

		p.varIdx[name] = *addr
	}

	return nil
}

func (p *parseVisitor) VisitScript(c parser.IScriptContext) *CompileError {
	switch c := c.(type) {
	case *parser.ScriptContext:
		vars := c.GetVars()
		if vars != nil {
			switch c := vars.(type) {
			case *parser.VarListDeclContext:
				if err := p.VisitVars(c); err != nil {
					return err
				}
			default:
				return InternalError(c)
			}
		}

		for _, stmt := range c.GetStmts() {
			var err *CompileError
			switch c := stmt.(type) {
			case *parser.PrintContext:
				err = p.VisitPrint(c)
			case *parser.FailContext:
				p.AppendInstruction(program.OP_FAIL)
			case *parser.SendContext:
				err = p.VisitSend(c)
			case *parser.SetTxMetaContext:
				err = p.VisitSetTxMeta(c)
			case *parser.SetAccountMetaContext:
				err = p.VisitSetAccountMeta(c)
			default:
				return InternalError(c)
			}
			if err != nil {
				return err
			}
		}
	default:
		return InternalError(c)
	}

	return nil
}

type CompileArtifacts struct {
	Source  string
	Tokens  []antlr.Token
	Errors  []CompileError
	Program *program.Program
}

func CompileFull(input string) CompileArtifacts {
	artifacts := CompileArtifacts{
		Source: input,
	}

	errListener := &ErrorListener{}

	is := antlr.NewInputStream(input)
	lexer := parser.NewNumScriptLexer(is)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errListener)

	stream := antlr.NewCommonTokenStream(lexer, antlr.LexerDefaultTokenChannel)
	p := parser.NewNumScriptParser(stream)
	p.RemoveErrorListeners()
	p.AddErrorListener(errListener)

	p.BuildParseTrees = true

	tree := p.Script()

	artifacts.Tokens = stream.GetAllTokens()
	artifacts.Errors = append(artifacts.Errors, errListener.Errors...)

	if len(errListener.Errors) != 0 {
		return artifacts
	}

	visitor := parseVisitor{
		errListener:    errListener,
		instructions:   make([]byte, 0),
		resources:      make([]program.Resource, 0),
		varIdx:         make(map[string]core.Address),
		neededBalances: make(map[core.Address]map[core.Address]struct{}),
	}

	err := visitor.VisitScript(tree)
	if err != nil {
		artifacts.Errors = append(artifacts.Errors, *err)
		return artifacts
	}

	artifacts.Program = &program.Program{
		Instructions:   visitor.instructions,
		Resources:      visitor.resources,
		NeededBalances: visitor.neededBalances,
	}

	return artifacts
}

func Compile(input string) (*program.Program, error) {
	artifacts := CompileFull(input)
	if len(artifacts.Errors) > 0 {
		err := CompileErrorList{
			Errors: artifacts.Errors,
			Source: artifacts.Source,
		}
		return nil, &err
	}

	return artifacts.Program, nil
}
