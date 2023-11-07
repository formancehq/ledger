package compiler

import (
	"fmt"
	"sort"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	internal2 "github.com/formancehq/ledger/internal/machine/internal"
	parser2 "github.com/formancehq/ledger/internal/machine/script/parser"
	program2 "github.com/formancehq/ledger/internal/machine/vm/program"
	"github.com/pkg/errors"
)

type parseVisitor struct {
	errListener  *ErrorListener
	instructions []byte
	// resources must not exceed 65536 elements
	resources []program2.Resource
	// sources store all source accounts
	// a source can be also a destination of another posting
	sources map[internal2.Address]struct{}
	// varIdx maps name to resource index
	varIdx map[string]internal2.Address
	// needBalances store for each account, the set of assets needed
	neededBalances map[internal2.Address]map[internal2.Address]struct{}
}

// Allocates constants if it hasn't already been,
// and returns its resource address.
func (p *parseVisitor) findConstant(constant program2.Constant) (*internal2.Address, bool) {
	for i := 0; i < len(p.resources); i++ {
		if c, ok := p.resources[i].(program2.Constant); ok {
			if internal2.ValueEquals(c.Inner, constant.Inner) {
				addr := internal2.Address(i)
				return &addr, true
			}
		}
	}
	return nil, false
}

func (p *parseVisitor) AllocateResource(res program2.Resource) (*internal2.Address, error) {
	if c, ok := res.(program2.Constant); ok {
		idx, ok := p.findConstant(c)
		if ok {
			return idx, nil
		}
	}
	if len(p.resources) >= 65536 {
		return nil, errors.New("number of unique constants exceeded 65536")
	}
	p.resources = append(p.resources, res)
	addr := internal2.NewAddress(uint16(len(p.resources) - 1))
	return &addr, nil
}

func (p *parseVisitor) isWorld(addr internal2.Address) bool {
	idx := int(addr)
	if idx < len(p.resources) {
		if c, ok := p.resources[idx].(program2.Constant); ok {
			if acc, ok := c.Inner.(internal2.AccountAddress); ok {
				if string(acc) == "world" {
					return true
				}
			}
		}
	}
	return false
}

func (p *parseVisitor) VisitVariable(c parser2.IVariableContext, push bool) (internal2.Type, *internal2.Address, *CompileError) {
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

func (p *parseVisitor) VisitExpr(c parser2.IExpressionContext, push bool) (internal2.Type, *internal2.Address, *CompileError) {
	switch c := c.(type) {
	case *parser2.ExprAddSubContext:
		lhsType, lhsAddr, err := p.VisitExpr(c.GetLhs(), push)
		if err != nil {
			return 0, nil, err
		}
		switch lhsType {
		case internal2.TypeNumber:
			rhsType, _, err := p.VisitExpr(c.GetRhs(), push)
			if err != nil {
				return 0, nil, err
			}
			if rhsType != internal2.TypeNumber {
				return 0, nil, LogicError(c, fmt.Errorf(
					"tried to do an arithmetic operation with incompatible left and right-hand side operand types: %s and %s",
					lhsType, rhsType))
			}
			if push {
				switch c.GetOp().GetTokenType() {
				case parser2.NumScriptLexerOP_ADD:
					p.AppendInstruction(program2.OP_IADD)
				case parser2.NumScriptLexerOP_SUB:
					p.AppendInstruction(program2.OP_ISUB)
				}
			}
			return internal2.TypeNumber, nil, nil
		case internal2.TypeMonetary:
			rhsType, _, err := p.VisitExpr(c.GetRhs(), push)
			if err != nil {
				return 0, nil, err
			}
			if rhsType != internal2.TypeMonetary {
				return 0, nil, LogicError(c, fmt.Errorf(
					"tried to do an arithmetic operation with incompatible left and right-hand side operand types: %s and %s",
					lhsType, rhsType))
			}
			if push {
				switch c.GetOp().GetTokenType() {
				case parser2.NumScriptLexerOP_ADD:
					p.AppendInstruction(program2.OP_MONETARY_ADD)
				case parser2.NumScriptLexerOP_SUB:
					p.AppendInstruction(program2.OP_MONETARY_SUB)
				}
			}
			return internal2.TypeMonetary, lhsAddr, nil
		default:
			return 0, nil, LogicError(c, fmt.Errorf(
				"tried to do an arithmetic operation with unsupported left-hand side operand type: %s",
				lhsType))
		}
	case *parser2.ExprLiteralContext:
		return p.VisitLit(c.GetLit(), push)
	case *parser2.ExprVariableContext:
		return p.VisitVariable(c.GetVar_(), push)
	default:
		return 0, nil, InternalError(c)
	}
}

func (p *parseVisitor) VisitLit(c parser2.ILiteralContext, push bool) (internal2.Type, *internal2.Address, *CompileError) {
	switch c := c.(type) {
	case *parser2.LitAccountContext:
		account := internal2.AccountAddress(c.GetText()[1:])
		addr, err := p.AllocateResource(program2.Constant{Inner: account})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return internal2.TypeAccount, addr, nil
	case *parser2.LitAssetContext:
		asset := internal2.Asset(c.GetText())
		addr, err := p.AllocateResource(program2.Constant{Inner: asset})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return internal2.TypeAsset, addr, nil
	case *parser2.LitNumberContext:
		number, err := internal2.ParseNumber(c.GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		addr, err := p.AllocateResource(program2.Constant{Inner: number})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return internal2.TypeNumber, addr, nil
	case *parser2.LitStringContext:
		addr, err := p.AllocateResource(program2.Constant{
			Inner: internal2.String(strings.Trim(c.GetText(), `"`)),
		})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return internal2.TypeString, addr, nil
	case *parser2.LitPortionContext:
		portion, err := internal2.ParsePortionSpecific(c.GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		addr, err := p.AllocateResource(program2.Constant{Inner: *portion})
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		if push {
			p.PushAddress(*addr)
		}
		return internal2.TypePortion, addr, nil
	case *parser2.LitMonetaryContext:
		typ, assetAddr, compErr := p.VisitExpr(c.Monetary().GetAsset(), false)
		if compErr != nil {
			return 0, nil, compErr
		}
		if typ != internal2.TypeAsset {
			return 0, nil, LogicError(c, fmt.Errorf(
				"the expression in monetary literal should be of type '%s' instead of '%s'",
				internal2.TypeAsset, typ))
		}

		amt, err := internal2.ParseMonetaryInt(c.Monetary().GetAmt().GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}

		var (
			monAddr          *internal2.Address
			alreadyAllocated bool
		)
		for i, r := range p.resources {
			switch v := r.(type) {
			case program2.Monetary:
				if v.Asset == *assetAddr && v.Amount.Equal(amt) {
					alreadyAllocated = true
					tmp := internal2.Address(uint16(i))
					monAddr = &tmp
					break
				}
			}
		}
		if !alreadyAllocated {
			monAddr, err = p.AllocateResource(program2.Monetary{
				Asset:  *assetAddr,
				Amount: amt,
			})
			if err != nil {
				return 0, nil, LogicError(c, err)
			}
		}
		if push {
			p.PushAddress(*monAddr)
		}
		return internal2.TypeMonetary, monAddr, nil
	default:
		return 0, nil, InternalError(c)
	}
}

func (p *parseVisitor) VisitMonetaryAll(c *parser2.SendContext, monAll parser2.IMonetaryAllContext) *CompileError {
	assetType, assetAddr, compErr := p.VisitExpr(monAll.GetAsset(), false)
	if compErr != nil {
		return compErr
	}
	if assetType != internal2.TypeAsset {
		return LogicError(c, fmt.Errorf(
			"send monetary all: the expression should be of type 'asset' instead of '%s'", assetType))
	}

	switch c := c.GetSrc().(type) {
	case *parser2.SrcContext:
		accounts, _, _, compErr := p.VisitSource(c.Source(), func() {
			p.PushAddress(*assetAddr)
		}, true)
		if compErr != nil {
			return compErr
		}
		p.setNeededBalances(accounts, assetAddr)

	case *parser2.SrcAllotmentContext:
		return LogicError(c, errors.New("cannot take all balance of an allotment source"))
	}
	return nil
}

func (p *parseVisitor) VisitMonetary(c *parser2.SendContext, mon parser2.IExpressionContext) *CompileError {
	monType, monAddr, compErr := p.VisitExpr(mon, false)
	if compErr != nil {
		return compErr
	}
	if monType != internal2.TypeMonetary {
		return LogicError(c, fmt.Errorf(
			"send monetary: the expression should be of type 'monetary' instead of '%s'", monType))
	}

	switch c := c.GetSrc().(type) {
	case *parser2.SrcContext:
		accounts, _, fallback, compErr := p.VisitSource(c.Source(), func() {
			p.PushAddress(*monAddr)
			p.AppendInstruction(program2.OP_ASSET)
		}, false)
		if compErr != nil {
			return compErr
		}
		p.setNeededBalances(accounts, monAddr)

		if _, _, err := p.VisitExpr(mon, true); err != nil {
			return err
		}

		if err := p.TakeFromSource(fallback); err != nil {
			return LogicError(c, err)
		}
	case *parser2.SrcAllotmentContext:
		if _, _, err := p.VisitExpr(mon, true); err != nil {
			return err
		}
		p.VisitAllotment(c.SourceAllotment(), c.SourceAllotment().GetPortions())
		p.AppendInstruction(program2.OP_ALLOC)

		sources := c.SourceAllotment().GetSources()
		n := len(sources)
		for i := 0; i < n; i++ {
			accounts, _, fallback, compErr := p.VisitSource(sources[i], func() {
				p.PushAddress(*monAddr)
				p.AppendInstruction(program2.OP_ASSET)
			}, false)
			if compErr != nil {
				return compErr
			}
			p.setNeededBalances(accounts, monAddr)

			if err := p.Bump(int64(i + 1)); err != nil {
				return LogicError(c, err)
			}

			if err := p.TakeFromSource(fallback); err != nil {
				return LogicError(c, err)
			}
		}

		if err := p.PushInteger(internal2.NewNumber(int64(n))); err != nil {
			return LogicError(c, err)
		}

		p.AppendInstruction(program2.OP_FUNDING_ASSEMBLE)
	}
	return nil
}

func (p *parseVisitor) setNeededBalances(accounts map[internal2.Address]struct{}, addr *internal2.Address) {
	for acc := range accounts {
		if b, ok := p.neededBalances[acc]; ok {
			b[*addr] = struct{}{}
		} else {
			p.neededBalances[acc] = map[internal2.Address]struct{}{
				*addr: {},
			}
		}
	}
}

func (p *parseVisitor) VisitSend(c *parser2.SendContext) *CompileError {
	if monAll := c.GetMonAll(); monAll != nil {
		if err := p.VisitMonetaryAll(c, monAll); err != nil {
			return err
		}
	} else if mon := c.GetMon(); mon != nil {
		if err := p.VisitMonetary(c, mon); err != nil {
			return err
		}
	}

	if err := p.VisitDestination(c.GetDest()); err != nil {
		return err
	}

	return nil
}

func (p *parseVisitor) VisitSetTxMeta(ctx *parser2.SetTxMetaContext) *CompileError {
	_, _, compErr := p.VisitExpr(ctx.GetValue(), true)
	if compErr != nil {
		return compErr
	}

	keyAddr, err := p.AllocateResource(program2.Constant{
		Inner: internal2.String(strings.Trim(ctx.GetKey().GetText(), `"`)),
	})
	if err != nil {
		return LogicError(ctx, err)
	}
	p.PushAddress(*keyAddr)

	p.AppendInstruction(program2.OP_TX_META)

	return nil
}

func (p *parseVisitor) VisitSetAccountMeta(ctx *parser2.SetAccountMetaContext) *CompileError {
	_, _, compErr := p.VisitExpr(ctx.GetValue(), true)
	if compErr != nil {
		return compErr
	}

	keyAddr, err := p.AllocateResource(program2.Constant{
		Inner: internal2.String(strings.Trim(ctx.GetKey().GetText(), `"`)),
	})
	if err != nil {
		return LogicError(ctx, err)
	}
	p.PushAddress(*keyAddr)

	ty, accAddr, compErr := p.VisitExpr(ctx.GetAcc(), false)
	if compErr != nil {
		return compErr
	}
	if ty != internal2.TypeAccount {
		return LogicError(ctx, fmt.Errorf(
			"set_account_meta: expression is of type %s, and should be of type account", ty))
	}
	p.PushAddress(*accAddr)

	p.AppendInstruction(program2.OP_ACCOUNT_META)

	return nil
}

func (p *parseVisitor) VisitSaveFromAccount(c *parser2.SaveFromAccountContext) *CompileError {
	var (
		typ     internal2.Type
		addr    *internal2.Address
		compErr *CompileError
	)
	if monAll := c.GetMonAll(); monAll != nil {
		typ, addr, compErr = p.VisitExpr(monAll.GetAsset(), false)
		if compErr != nil {
			return compErr
		}
		if typ != internal2.TypeAsset {
			return LogicError(c, fmt.Errorf(
				"save monetary all from account: the first expression should be of type 'asset' instead of '%s'", typ))
		}
	} else if mon := c.GetMon(); mon != nil {
		typ, addr, compErr = p.VisitExpr(mon, false)
		if compErr != nil {
			return compErr
		}
		if typ != internal2.TypeMonetary {
			return LogicError(c, fmt.Errorf(
				"save monetary from account: the first expression should be of type 'monetary' instead of '%s'", typ))
		}
	}
	p.PushAddress(*addr)

	typ, addr, compErr = p.VisitExpr(c.GetAcc(), false)
	if compErr != nil {
		return compErr
	}
	if typ != internal2.TypeAccount {
		return LogicError(c, fmt.Errorf(
			"save monetary from account: the second expression should be of type 'account' instead of '%s'", typ))
	}
	p.PushAddress(*addr)

	p.AppendInstruction(program2.OP_SAVE)

	return nil
}

func (p *parseVisitor) VisitPrint(ctx *parser2.PrintContext) *CompileError {
	_, _, err := p.VisitExpr(ctx.GetExpr(), true)
	if err != nil {
		return err
	}

	p.AppendInstruction(program2.OP_PRINT)

	return nil
}

func (p *parseVisitor) VisitVars(c *parser2.VarListDeclContext) *CompileError {
	if len(c.GetV()) > 32768 {
		return LogicError(c, fmt.Errorf("number of variables exceeded %v", 32768))
	}

	for _, v := range c.GetV() {
		name := v.GetName().GetText()[1:]
		if _, ok := p.varIdx[name]; ok {
			return LogicError(c, fmt.Errorf("duplicate variable $%s", name))
		}
		var ty internal2.Type
		switch v.GetTy().GetText() {
		case "account":
			ty = internal2.TypeAccount
		case "asset":
			ty = internal2.TypeAsset
		case "number":
			ty = internal2.TypeNumber
		case "string":
			ty = internal2.TypeString
		case "monetary":
			ty = internal2.TypeMonetary
		case "portion":
			ty = internal2.TypePortion
		default:
			return InternalError(c)
		}

		var addr *internal2.Address
		var err error
		if v.GetOrig() == nil {
			addr, err = p.AllocateResource(program2.Variable{Typ: ty, Name: name})
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
		case *parser2.OriginAccountMetaContext:
			srcTy, src, compErr := p.VisitExpr(c.GetAccount(), false)
			if compErr != nil {
				return compErr
			}
			if srcTy != internal2.TypeAccount {
				return LogicError(c, fmt.Errorf(
					"variable $%s: type should be 'account' to pull account metadata", name))
			}
			key := strings.Trim(c.GetKey().GetText(), `"`)
			addr, err = p.AllocateResource(program2.VariableAccountMetadata{
				Typ:     ty,
				Name:    name,
				Account: *src,
				Key:     key,
			})
		case *parser2.OriginAccountBalanceContext:
			if ty != internal2.TypeMonetary {
				return LogicError(c, fmt.Errorf(
					"variable $%s: type should be 'monetary' to pull account balance", name))
			}
			accTy, accAddr, compErr := p.VisitExpr(c.GetAccount(), false)
			if compErr != nil {
				return compErr
			}
			if accTy != internal2.TypeAccount {
				return LogicError(c, fmt.Errorf(
					"variable $%s: the first argument to pull account balance should be of type 'account'", name))
			}

			assTy, assAddr, compErr := p.VisitExpr(c.GetAsset(), false)
			if compErr != nil {
				return compErr
			}
			if assTy != internal2.TypeAsset {
				return LogicError(c, fmt.Errorf(
					"variable $%s: the second argument to pull account balance should be of type 'asset'", name))
			}

			addr, err = p.AllocateResource(program2.VariableAccountBalance{
				Name:    name,
				Account: *accAddr,
				Asset:   *assAddr,
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

func (p *parseVisitor) VisitScript(c parser2.IScriptContext) *CompileError {
	switch c := c.(type) {
	case *parser2.ScriptContext:
		vars := c.GetVars()
		if vars != nil {
			switch c := vars.(type) {
			case *parser2.VarListDeclContext:
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
			case *parser2.PrintContext:
				err = p.VisitPrint(c)
			case *parser2.FailContext:
				p.AppendInstruction(program2.OP_FAIL)
			case *parser2.SendContext:
				err = p.VisitSend(c)
			case *parser2.SetTxMetaContext:
				err = p.VisitSetTxMeta(c)
			case *parser2.SetAccountMetaContext:
				err = p.VisitSetAccountMeta(c)
			case *parser2.SaveFromAccountContext:
				err = p.VisitSaveFromAccount(c)
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
	Program *program2.Program
}

func CompileFull(input string) CompileArtifacts {
	artifacts := CompileArtifacts{
		Source: input,
	}

	errListener := &ErrorListener{}

	is := antlr.NewInputStream(input)
	lexer := parser2.NewNumScriptLexer(is)
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errListener)

	stream := antlr.NewCommonTokenStream(lexer, antlr.LexerDefaultTokenChannel)
	p := parser2.NewNumScriptParser(stream)
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
		resources:      make([]program2.Resource, 0),
		varIdx:         make(map[string]internal2.Address),
		neededBalances: make(map[internal2.Address]map[internal2.Address]struct{}),
		sources:        map[internal2.Address]struct{}{},
	}

	err := visitor.VisitScript(tree)
	if err != nil {
		artifacts.Errors = append(artifacts.Errors, *err)
		return artifacts
	}

	sources := make(internal2.Addresses, 0)
	for address := range visitor.sources {
		sources = append(sources, address)
	}
	sort.Stable(sources)

	artifacts.Program = &program2.Program{
		Instructions:   visitor.instructions,
		Resources:      visitor.resources,
		NeededBalances: visitor.neededBalances,
		Sources:        sources,
	}

	return artifacts
}

func Compile(input string) (*program2.Program, error) {
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
