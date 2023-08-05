package compiler

import (
	"fmt"
	"strings"

	"github.com/antlr/antlr4/runtime/Go/antlr"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/machine/script/parser"
	"github.com/numary/ledger/pkg/machine/vm/program"
	"github.com/pkg/errors"
)

type parseVisitor struct {
	errListener *ErrorListener
	vars        map[string]core.Type
}

func (p *parseVisitor) isWorld(expr parser.IExpressionContext) bool {
	if lit, ok := expr.(*parser.ExprLiteralContext); ok {
		_, value, _ := p.CompileLit(lit.GetLit())
		return core.ValueEquals(value, core.AccountAddress("world"))
	} else {
		return false
	}
}

func (p *parseVisitor) CompileExprTy(c parser.IExpressionContext, ty core.Type) (program.Expr, *CompileError) {
	exprTy, expr, err := p.CompileExpr(c)
	if err != nil {
		return nil, err
	}
	if exprTy != ty {
		return nil, LogicError(c, fmt.Errorf("wrong type: expected %v and found %v", ty, exprTy))
	}
	return expr, err
}

func (p *parseVisitor) CompileExpr(c parser.IExpressionContext) (core.Type, program.Expr, *CompileError) {
	switch c := c.(type) {
	case *parser.ExprAddSubContext:
		lhsType, lhs, err := p.CompileExpr(c.GetLhs())
		if err != nil {
			return 0, nil, err
		}
		switch lhsType {
		case core.TypeNumber:
			rhs, err := p.CompileExprTy(c.GetRhs(), core.TypeNumber)
			if err != nil {
				return 0, nil, err
			}
			expr := program.ExprNumberOperation{
				Lhs: lhs,
				Rhs: rhs,
			}
			switch c.GetOp().GetTokenType() {
			case parser.NumScriptLexerOP_ADD:
				expr.Op = program.OP_ADD
			case parser.NumScriptLexerOP_SUB:
				expr.Op = program.OP_SUB
			}
			return core.TypeNumber, expr, nil
		case core.TypeMonetary:
			rhs, err := p.CompileExprTy(c.GetRhs(), core.TypeMonetary)
			if err != nil {
				return 0, nil, err
			}
			expr := program.ExprMonetaryOperation{
				Lhs: lhs,
				Rhs: rhs,
			}
			switch c.GetOp().GetTokenType() {
			case parser.NumScriptLexerOP_ADD:
				expr.Op = program.OP_ADD
			case parser.NumScriptLexerOP_SUB:
				expr.Op = program.OP_SUB
			}
			return core.TypeMonetary, expr, nil
		default:
			return 0, nil, LogicError(c, errors.New("tried to do arithmetic with wrong type"))
		}
	case *parser.ExprArithmeticConditionContext:
		lhsType, lhs, err := p.CompileExpr(c.GetLhs())
		if err != nil {
			return 0, nil, err
		}
		switch lhsType {
		case core.TypeNumber:
			rhs, err := p.CompileExprTy(c.GetRhs(), core.TypeNumber)
			if err != nil {
				return 0, nil, err
			}
			expr := program.ExprNumberCondition{
				Lhs: lhs,
				Rhs: rhs,
			}
			switch c.GetOp().GetTokenType() {
			case parser.NumScriptLexerOP_EQ:
				expr.Op = program.OP_EQ
			case parser.NumScriptLexerOP_NEQ:
				expr.Op = program.OP_NEQ
			case parser.NumScriptLexerOP_LT:
				expr.Op = program.OP_LT
			case parser.NumScriptLexerOP_LTE:
				expr.Op = program.OP_LTE
			case parser.NumScriptLexerOP_GT:
				expr.Op = program.OP_GT
			case parser.NumScriptLexerOP_GTE:
				expr.Op = program.OP_GTE
			default:
				return 0, nil, InternalError(c)
			}
			return core.TypeBool, expr, nil
		// case core.TypeMonetary: TODO
		default:
			return 0, nil, LogicError(c, errors.New("tried to do arithmetic with wrong type"))
		}

	case *parser.ExprLogicalNotContext:
		operand, err := p.CompileExprTy(c.GetLhs(), core.TypeBool)
		if err != nil {
			return 0, nil, err
		}
		expr := program.ExprLogicalNot{
			Operand: operand,
		}
		return core.TypeBool, expr, nil

	case *parser.ExprLogicalAndContext:
		lhs, err := p.CompileExprTy(c.GetLhs(), core.TypeBool)
		if err != nil {
			return 0, nil, err
		}
		rhs, err := p.CompileExprTy(c.GetRhs(), core.TypeBool)
		if err != nil {
			return 0, nil, err
		}
		expr := program.ExprLogicalAnd{
			Lhs: lhs,
			Rhs: rhs,
		}
		return core.TypeBool, expr, nil

	case *parser.ExprLogicalOrContext:
		lhs, err := p.CompileExprTy(c.GetLhs(), core.TypeBool)
		if err != nil {
			return 0, nil, err
		}
		rhs, err := p.CompileExprTy(c.GetRhs(), core.TypeBool)
		if err != nil {
			return 0, nil, err
		}
		expr := program.ExprLogicalOr{
			Lhs: lhs,
			Rhs: rhs,
		}
		return core.TypeBool, expr, nil

	case *parser.ExprLiteralContext:
		ty, value, err := p.CompileLit(c.GetLit())
		if err != nil {
			return 0, nil, err
		}
		return ty, program.ExprLiteral{Value: value}, nil
	case *parser.ExprVariableContext:
		name := c.GetVar_().GetText()[1:] // strip '$' prefix
		if ty, ok := p.vars[name]; ok {
			return ty, program.ExprVariable(name), nil
		} else {
			return 0, nil, LogicError(c, errors.New("variable not declared"))
		}
	case *parser.ExprMonetaryNewContext:
		asset, compErr := p.CompileExprTy(c.Monetary().GetAsset(), core.TypeAsset)
		if compErr != nil {
			return 0, nil, compErr
		}
		amt, compErr := p.CompileExprTy(c.Monetary().GetAmt(), core.TypeNumber)
		if compErr != nil {
			return 0, nil, compErr
		}
		return core.TypeMonetary, program.ExprMonetaryNew{
			Asset:  asset,
			Amount: amt,
		}, nil
	case *parser.ExprTernaryContext:
		cond, compErr := p.CompileExprTy(c.GetCond(), core.TypeBool)
		if compErr != nil {
			return 0, nil, compErr
		}
		typeIfTrue, exprIfTrue, compErr := p.CompileExpr(c.GetIfTrue())
		if compErr != nil {
			return 0, nil, compErr
		}
		typeIfFalse, exprIfFalse, compErr := p.CompileExpr(c.GetIfFalse())
		if compErr != nil {
			return 0, nil, compErr
		}
		if typeIfTrue != typeIfFalse {
			return 0, nil, LogicError(c, errors.New("mismatching types"))
		}
		return typeIfTrue, program.ExprTernary{
			Cond:    cond,
			IfTrue:  exprIfTrue,
			IfFalse: exprIfFalse,
		}, nil

	case *parser.ExprEnclosedContext:
		return p.CompileExpr(c.GetExpr())

	default:
		return 0, nil, InternalError(c)
	}
}

func (p *parseVisitor) CompileLit(c parser.ILiteralContext) (core.Type, core.Value, *CompileError) {
	switch c := c.(type) {
	case *parser.LitAccountContext:
		account := core.AccountAddress(c.GetText()[1:])
		return core.TypeAccount, account, nil
	case *parser.LitAssetContext:
		asset := core.Asset(c.GetText())
		return core.TypeAsset, asset, nil
	case *parser.LitNumberContext:
		number, err := core.ParseNumber(c.GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		return core.TypeNumber, number, nil
	case *parser.LitStringContext:
		str := core.String(strings.Trim(c.GetText(), `"`))
		return core.TypeString, str, nil
	case *parser.LitPortionContext:
		portion, err := core.ParsePortionSpecific(c.GetText())
		if err != nil {
			return 0, nil, LogicError(c, err)
		}
		return core.TypePortion, *portion, nil
	default:
		return 0, nil, InternalError(c)
	}
}

func (p *parseVisitor) CompileSend(c *parser.SendContext) (program.Instruction, *CompileError) {
	mon, err := p.CompileExprTy(c.GetMon(), core.TypeMonetary)
	if err != nil {
		return nil, err
	}
	valueAwareSource, err := p.CompileValueAwareSource(c.GetSrc())
	if err != nil {
		return nil, err
	}
	destination, err := p.CompileDestination(c.GetDest())
	if err != nil {
		return nil, err
	}
	return program.InstructionAllocate{
		Funding: program.ExprTake{
			Amount: mon,
			Source: valueAwareSource,
		},
		Destination: destination,
	}, nil
}

func (p *parseVisitor) CompileSendAll(c *parser.SendAllContext) (program.Instruction, *CompileError) {
	source, hasFallback, err := p.CompileSource(c.GetSrc())
	if err != nil {
		return nil, err
	}
	asset, err := p.CompileExprTy(c.GetMonAll().GetAsset(), core.TypeAsset)
	if err != nil {
		return nil, err
	}
	if hasFallback {
		return nil, LogicError(c, errors.New("cannot take all balance of an unlimited source"))
	}
	destination, err := p.CompileDestination(c.GetDest())
	if err != nil {
		return nil, err
	}
	return program.InstructionAllocate{
		Funding: program.ExprTakeAll{
			Asset:  asset,
			Source: source,
		},
		Destination: destination,
	}, nil
}

func (p *parseVisitor) CompileSetTxMeta(ctx *parser.SetTxMetaContext) (program.Instruction, *CompileError) {
	_, value, err := p.CompileExpr(ctx.GetValue())
	if err != nil {
		return nil, err
	}
	return program.InstructionSetTxMeta{
		Key:   strings.Trim(ctx.GetKey().GetText(), `"`),
		Value: value,
	}, nil

}

func (p *parseVisitor) CompileSetAccountMeta(ctx *parser.SetAccountMetaContext) (program.Instruction, *CompileError) {
	account, err := p.CompileExprTy(ctx.GetAcc(), core.TypeAccount)
	if err != nil {
		return nil, err
	}

	_, value, err := p.CompileExpr(ctx.GetValue())
	if err != nil {
		return nil, err
	}

	return program.InstructionSetAccountMeta{
		Account: account,
		Key:     strings.Trim(ctx.GetKey().GetText(), `"`),
		Value:   value,
	}, nil
}

func (p *parseVisitor) CompileSave(ctx *parser.SaveFromAccountContext) (program.Instruction, *CompileError) {
	if monAll := ctx.GetMonAll(); monAll != nil {
		asset, err := p.CompileExprTy(ctx.MonetaryAll().GetAsset(), core.TypeAsset)
		if err != nil {
			return nil, err
		}
		account, err := p.CompileExprTy(ctx.GetAcc(), core.TypeAccount)
		if err != nil {
			return nil, err
		}
		return program.InstructionSaveAll{
			Asset:   asset,
			Account: account,
		}, nil
	} else if mon := ctx.GetMon(); mon != nil {
		mon, err := p.CompileExprTy(ctx.GetMon(), core.TypeMonetary)
		if err != nil {
			return nil, err
		}
		account, err := p.CompileExprTy(ctx.GetAcc(), core.TypeAccount)
		if err != nil {
			return nil, err
		}
		return program.InstructionSave{
			Amount:  mon,
			Account: account,
		}, nil
	} else {
		return nil, InternalError(ctx)
	}
}

func (p *parseVisitor) CompilePrint(ctx *parser.PrintContext) (program.Instruction, *CompileError) {
	_, expr, err := p.CompileExpr(ctx.GetExpr())
	if err != nil {
		return nil, err
	}
	return program.InstructionPrint{Expr: expr}, nil
}

func (p *parseVisitor) CompileVars(c *parser.VarListDeclContext) ([]program.VarDecl, *CompileError) {
	varsDecl := make([]program.VarDecl, 0)

	for _, v := range c.GetV() {
		name := v.GetName().GetText()[1:]
		if _, ok := p.vars[name]; ok {
			return nil, LogicError(c, fmt.Errorf("duplicate variable $%s", name))
		}
		var ty core.Type
		switch v.GetTy().GetText() {
		case "account":
			ty = core.TypeAccount
		case "asset":
			ty = core.TypeAsset
		case "number":
			ty = core.TypeNumber
		case "string":
			ty = core.TypeString
		case "monetary":
			ty = core.TypeMonetary
		case "portion":
			ty = core.TypePortion
		case "bool":
			ty = core.TypeBool
		default:
			return nil, InternalError(c)
		}

		p.vars[name] = ty

		varDecl := program.VarDecl{
			Typ:    ty,
			Name:   name,
			Origin: nil,
		}

		switch c := v.GetOrig().(type) {
		case *parser.OriginAccountMetaContext:
			account, compErr := p.CompileExprTy(c.GetAccount(), core.TypeAccount)
			if compErr != nil {
				return nil, compErr
			}
			key := strings.Trim(c.GetKey().GetText(), `"`)
			varDecl.Origin = program.VarOriginMeta{
				Account: account,
				Key:     key,
			}
		case *parser.OriginAccountBalanceContext:
			if ty != core.TypeMonetary {
				return nil, LogicError(c, fmt.Errorf(
					"variable $%s: type should be 'monetary' to pull account balance", name))
			}
			account, compErr := p.CompileExprTy(c.GetAccount(), core.TypeAccount)
			if compErr != nil {
				return nil, compErr
			}
			asset, compErr := p.CompileExprTy(c.GetAsset(), core.TypeAsset)
			if compErr != nil {
				return nil, compErr
			}
			varDecl.Origin = program.VarOriginBalance{
				Account: account,
				Asset:   asset,
			}
		}
		varsDecl = append(varsDecl, varDecl)
	}

	return varsDecl, nil
}

func (p *parseVisitor) CompileScript(c parser.IScriptContext) (*program.Program, *CompileError) {
	var varsDecl []program.VarDecl
	var instructions []program.Instruction
	var err *CompileError
	switch c := c.(type) {
	case *parser.ScriptContext:
		vars := c.GetVars()
		if vars != nil {
			switch c := vars.(type) {
			case *parser.VarListDeclContext:
				varsDecl, err = p.CompileVars(c)
				if err != nil {
					return nil, err
				}
			default:
				return nil, InternalError(c)
			}
		}

		for _, statement := range c.GetStmts() {
			var instr program.Instruction
			var err *CompileError
			switch c := statement.(type) {
			case *parser.PrintContext:
				instr, err = p.CompilePrint(c)
			case *parser.FailContext:
				instr = program.InstructionFail{}
			case *parser.SaveFromAccountContext:
				instr, err = p.CompileSave(c)
			case *parser.SendContext:
				instr, err = p.CompileSend(c)
			case *parser.SendAllContext:
				instr, err = p.CompileSendAll(c)
			case *parser.SetTxMetaContext:
				instr, err = p.CompileSetTxMeta(c)
			case *parser.SetAccountMetaContext:
				instr, err = p.CompileSetAccountMeta(c)
			default:
				return nil, InternalError(c)
			}
			if err != nil {
				return nil, err
			}
			instructions = append(instructions, instr)
		}
	default:
		return nil, InternalError(c)
	}

	return &program.Program{
		VarsDecl:    varsDecl,
		Instruction: instructions,
	}, nil
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
		errListener: errListener,
		vars:        make(map[string]core.Type),
	}

	program, err := visitor.CompileScript(tree)
	if err != nil {
		artifacts.Errors = append(artifacts.Errors, *err)
		return artifacts
	}

	artifacts.Program = program

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
