package numscriptgen

import (
	"math/big"

	"github.com/formancehq/numscript/builder"
)

// toBuilder converts a (post-cleanup) generated program into builder
// statements, ready to be passed to builder.BuildProgram. Each account and
// asset occurrence is rendered inline or through a var according to its
// AsVar flag — see toBuilderAccount.
func toBuilder(p Program) []builder.Statement {
	out := make([]builder.Statement, len(p))
	for i, s := range p {
		out[i] = toBuilderStatement(s)
	}
	return out
}

func toBuilderStatement(s Statement) builder.Statement {
	src := toBuilderSource(s.Source)
	dest := toBuilderDestination(s.Destination)

	if s.IsSendAll {
		return builder.StmtSendAll(toBuilderAsset(s.Asset, s.AssetAsVar), src, dest)
	}
	return builder.StmtSend(toBuilderMonetary(s.Amount), src, dest)
}

// toBuilderAccount renders one account occurrence, either inline (`@addr`) or
// through a pooled var (`$accountN` bound to addr). builder.ExprAccount pools
// by address, so every var-form occurrence of an address shares one var while
// inline occurrences stay literal — letting a single script reference the same
// account both ways.
func toBuilderAccount(address string, asVar bool) builder.Expression[builder.ExprTypeAccount] {
	if asVar {
		return builder.ExprAccount(address)
	}
	return builder.UnsafeAccount(address)
}

// toBuilderString renders one string occurrence, var or inline.
func toBuilderString(s string, asVar bool) builder.Expression[builder.ExprTypeString] {
	if asVar {
		return builder.ExprString(s)
	}
	return builder.ExprStringLit(s)
}

// toBuilderAsset renders one asset occurrence, var or inline. Note the asset
// slot of a monetary literal accepts a var in both grammars (`[$assetN 10]`),
// unlike the amount slot — see toBuilderMonetary.
func toBuilderAsset(asset string, asVar bool) builder.Expression[builder.ExprTypeAsset] {
	if asVar {
		return builder.ExprAsset(asset)
	}
	return builder.UnsafeAsset(asset)
}

func toBuilderMonetary(m Monetary) builder.Expression[builder.ExprTypeMonetary] {
	if m.AsVar && m.Amount.Sign() >= 0 {
		return builder.ExprMonetaryVar(m.Asset + " " + m.Amount.String())
	}
	if m.Amount.Sign() < 0 {
		// A bracketed monetary literal's amount slot (`[ASSET N]`) only ever
		// accepts a bare non-negative number in both grammars — there's no
		// legal way to write `[ASSET -7]` directly. `[ASSET 0] - [ASSET 7]`
		// is the only legal way to reach a negative monetary value
		// (verified directly against the oracle); see
		// builder.ExprMonetarySub.
		abs := new(big.Int).Neg(m.Amount)
		zero := builder.ExprMonetary(toBuilderAsset(m.Asset, m.AssetAsVar), builder.ExprNumberBigInt(big.NewInt(0)))
		magnitude := builder.ExprMonetary(toBuilderAsset(m.Asset, m.AssetAsVar), builder.ExprNumberBigInt(abs))
		return builder.ExprMonetarySub(zero, magnitude)
	}
	return builder.ExprMonetary(toBuilderAsset(m.Asset, m.AssetAsVar), builder.ExprNumberBigInt(m.Amount))
}

func toBuilderNumExpr(e NumExpr) builder.Expression[builder.ExprTypeNumber] {
	switch e.Kind {
	case NumLit:
		if e.LitAsVar {
			return builder.ExprNumberVar(e.Lit)
		}
		return builder.ExprNumberBigInt(e.Lit)
	case NumAdd:
		return builder.ExprAdd(toBuilderNumExpr(*e.Left), toBuilderNumExpr(*e.Right))
	case NumSub:
		return builder.ExprSub(toBuilderNumExpr(*e.Left), toBuilderNumExpr(*e.Right))
	default:
		panic("gen: unknown num expr kind")
	}
}

// varExprs holds the converted builder expression for each declared var,
// split by the builder type its Kind produces: a VarFromBalance decl is
// Expression[ExprTypeMonetary], a VarFromMeta decl is
// Expression[ExprTypeNumber]. Both are indexed by the same position in
// Script.Vars; only the slot matching a given VarDecl's Kind is populated.
type varExprs struct {
	monetary []builder.Expression[builder.ExprTypeMonetary]
	number   []builder.Expression[builder.ExprTypeNumber]
	// setTxMeta[i] emits `set_tx_meta(<key>, $var_i)` for a VarFromMeta decl.
	// A meta var can be declared as any of the six types, and Go cannot hold
	// those differently-typed Expression[T] values in one slice, so the
	// statement is built here — where the type is still known — and the
	// consumer only supplies the key.
	setTxMeta []func(key string) builder.Statement
}

func toBuilderVarExprs(vars []VarDecl) varExprs {
	ve := varExprs{
		monetary:  make([]builder.Expression[builder.ExprTypeMonetary], len(vars)),
		number:    make([]builder.Expression[builder.ExprTypeNumber], len(vars)),
		setTxMeta: make([]func(string) builder.Statement, len(vars)),
	}
	// The account expression of each MetaAccount declaration, so a later
	// declaration can chain onto it (AccountFromVarIdx). Reusing the same
	// expression everywhere keeps it one origin var in the script.
	accountOrigin := make([]builder.Expression[builder.ExprTypeAccount], len(vars))
	for i, v := range vars {
		var account builder.Expression[builder.ExprTypeAccount]
		if v.AccountFromVarIdx != nil {
			account = accountOrigin[*v.AccountFromVarIdx]
			if account == nil {
				panic("gen: AccountFromVarIdx does not point at an earlier MetaAccount declaration")
			}
		} else {
			account = toBuilderAccount(v.Account, v.AccountAsVar)
		}
		switch v.Kind {
		case VarFromBalance:
			ve.monetary[i] = builder.NewMonetaryVarFromBalance(account, toBuilderAsset(v.Asset, v.AssetAsVar))
		case VarFromMeta:
			switch v.MetaType {
			case MetaString:
				x := builder.NewStringVarFromMeta(account, v.Key)
				ve.setTxMeta[i] = func(k string) builder.Statement { return builder.StmtSetTxMeta(k, x) }
			case MetaMonetary:
				x := builder.NewMonetaryVarFromMeta(account, v.Key)
				ve.setTxMeta[i] = func(k string) builder.Statement { return builder.StmtSetTxMeta(k, x) }
			case MetaAsset:
				x := builder.NewAssetVarFromMeta(account, v.Key)
				ve.setTxMeta[i] = func(k string) builder.Statement { return builder.StmtSetTxMeta(k, x) }
			case MetaAccount:
				x := builder.NewAccountVarFromMeta(account, v.Key)
				accountOrigin[i] = x
				ve.setTxMeta[i] = func(k string) builder.Statement { return builder.StmtSetTxMeta(k, x) }
			case MetaPortion:
				x := builder.NewPortionVarFromMeta(account, v.Key)
				ve.setTxMeta[i] = func(k string) builder.Statement { return builder.StmtSetTxMeta(k, x) }
			default:
				x := builder.NewNumberVarFromMeta(account, v.Key)
				ve.number[i] = x
				ve.setTxMeta[i] = func(k string) builder.Statement { return builder.StmtSetTxMeta(k, x) }
			}
		default:
			panic("gen: unknown var decl kind")
		}
	}
	return ve
}

// AccountVarFill pairs a declared account-typed var with the literal value
// it must be bound to at run time — collected during toBuilderScript and
// consumed by GenerateScript (api.go), which merges it into the vars
// bindings map builder.BuildProgram returns. Unlike a VarFromBalance/
// VarFromMeta var (whose value the compiler computes from an origin call),
// a plain account-typed var has no origin — its value can only come from
// the caller-supplied vars map, exactly as GenerateScript already does for
// the account/asset/etc. pools.
type AccountVarFill struct {
	Var   *builder.Var[builder.ExprTypeAccount]
	Value string
}

// toBuilderScript converts a full generated Script (vars declarations,
// seed-funding statements, the core send-only program interleaved with
// extra non-send statements per Script.Order) into a flat list of builder
// statements, ready for builder.BuildProgram, plus the account-var runtime
// bindings the caller must additionally fill in (see AccountVarFill).
func toBuilderScript(s Script) ([]builder.Statement, []AccountVarFill) {
	ve := toBuilderVarExprs(s.Vars)

	accountVars := make([]builder.Var[builder.ExprTypeAccount], len(s.AccountVars))
	accountVarExprs := make([]builder.Expression[builder.ExprTypeAccount], len(s.AccountVars))
	fills := make([]AccountVarFill, len(s.AccountVars))
	for i, v := range s.AccountVars {
		accountVars[i] = builder.NewAccountVar()
		accountVarExprs[i] = builder.ExprVar(&accountVars[i])
		fills[i] = AccountVarFill{Var: &accountVars[i], Value: v.Value}
	}

	body := bodyStatements(s)
	out := make([]builder.Statement, 0, len(s.Seeds)+len(body))
	out = append(out, toBuilder(s.Seeds)...)
	for _, st := range body {
		if st.send != nil {
			out = append(out, toBuilderStatement(*st.send))
		} else {
			out = append(out, toBuilderExtra(*st.extra, ve, accountVarExprs))
		}
	}

	return out, fills
}

func toBuilderExtra(e ExtraStatement, ve varExprs, accountVarExprs []builder.Expression[builder.ExprTypeAccount]) builder.Statement {
	switch e.Kind {
	case ExtraSave:
		var mon builder.Expression[builder.ExprTypeMonetary]
		if e.VarIdx != nil {
			mon = ve.monetary[*e.VarIdx]
		} else {
			mon = toBuilderMonetary(*e.Monetary)
		}
		return builder.StmtSave(mon, toBuilderAccount(e.Account, e.AccountAsVar))

	case ExtraSaveAll:
		return builder.StmtSaveAll(toBuilderAsset(e.Asset, e.AssetAsVar), toBuilderAccount(e.Account, e.AccountAsVar))

	case ExtraSetTxMeta:
		if e.StringValue != nil {
			return builder.StmtSetTxMeta(e.Key, toBuilderString(*e.StringValue, e.StringValueAsVar))
		}
		return builder.StmtSetTxMeta(e.Key, toBuilderNumExpr(e.Value))

	case ExtraSetAccountMeta:
		acct := toBuilderAccount(e.Account, e.AccountAsVar)
		if e.StringValue != nil {
			return builder.StmtSetAccountMeta(acct, e.Key, toBuilderString(*e.StringValue, e.StringValueAsVar))
		}
		return builder.StmtSetAccountMeta(acct, e.Key, toBuilderNumExpr(e.Value))

	case ExtraSendVar:
		return builder.StmtSend(
			ve.monetary[*e.VarIdx],
			builder.SrcAccountOverdraft(toBuilderAccount(e.Account, e.AccountAsVar), builder.UnboundedOverdraft()),
			builder.DestAccount(toBuilderAccount(e.Destination, e.DestinationAsVar)),
		)

	case ExtraSetTxMetaVar:
		return ve.setTxMeta[*e.VarIdx](e.Key)

	case ExtraSendFromAccountVar:
		return builder.StmtSend(
			toBuilderMonetary(*e.Monetary),
			builder.SrcAccount(accountVarExprs[*e.AccountVarIdx]),
			builder.DestAccount(toBuilderAccount(e.Account, e.AccountAsVar)),
		)

	case ExtraSendToAccountVar:
		return builder.StmtSend(
			toBuilderMonetary(*e.Monetary),
			builder.SrcAccount(toBuilderAccount(e.Account, e.AccountAsVar)),
			builder.DestAccount(accountVarExprs[*e.AccountVarIdx]),
		)

	default:
		panic("gen: unknown extra statement kind")
	}
}

// toBuilderPortion renders a portion inline as an `n/d` literal, or through a
// runtime-bound var. The var form only compiles inside an allotment with a
// `remaining` clause: without one, the oracle cannot prove the block sums to
// 100% and rejects it at compile time ("might be less than 100%"), which
// Compare tolerates, silently skipping the comparison. gen.go only sets
// PortionAsVar alongside a remaining clause for that reason.
//
// The binding uses big.Rat's normalized num/denom, which both engines parse.
func toBuilderPortion(r *big.Rat, asVar bool) builder.Expression[builder.ExprTypePortion] {
	if asVar {
		return builder.ExprPortionVar(r.Num().String() + "/" + r.Denom().String())
	}
	return builder.ExprPortion(builder.NewPortion(new(big.Int).Set(r.Num()), new(big.Int).Set(r.Denom())))
}

func toBuilderSource(s Source) builder.Source {
	switch s.Kind {
	case SrcAccount:
		if s.Color != "" {
			return builder.SrcColored(toBuilderAccount(s.Account, s.AccountAsVar), toBuilderString(s.Color, s.ColorAsVar))
		}
		return builder.SrcAccount(toBuilderAccount(s.Account, s.AccountAsVar))

	case SrcAccountOverdraft:
		overdraft := builder.UnboundedOverdraft()
		if s.Overdraft != nil {
			overdraft = builder.BoundedOverdraft(toBuilderMonetary(*s.Overdraft))
		}
		if s.Color != "" {
			return builder.SrcColoredOverdraft(toBuilderAccount(s.Account, s.AccountAsVar), toBuilderString(s.Color, s.ColorAsVar), overdraft)
		}
		return builder.SrcAccountOverdraft(toBuilderAccount(s.Account, s.AccountAsVar), overdraft)

	case SrcCapped:
		return builder.SrcCapped(toBuilderMonetary(*s.Cap), toBuilderSource(*s.Inner))

	case SrcInorder, SrcOneof:
		sources := make([]builder.Source, len(s.Sources))
		for i, inner := range s.Sources {
			sources[i] = toBuilderSource(inner)
		}
		if s.Kind == SrcOneof {
			return builder.SrcOneof(sources...)
		}
		return builder.SrcInorder(sources...)

	case SrcAllotment:
		clauses := make([]builder.AllotmentClause[builder.Source], len(s.Clauses))
		for i, c := range s.Clauses {
			clauses[i] = builder.AllotmentClause[builder.Source]{
				Portion: toBuilderClausePortion(c.Portion, c.PortionAsVar, c.Div),
				Payload: toBuilderSource(c.Source),
			}
		}
		if s.AllotmentRemaining != nil {
			return builder.SrcAllotmentWithRemaining(clauses, toBuilderSource(*s.AllotmentRemaining))
		}
		return builder.SrcAllotment(clauses...)

	default:
		panic("gen: unknown source kind")
	}
}

// toBuilderClausePortion renders one allotment clause's portion: a division
// expression when Div is set (numerator through a number var, so negative
// values need no unary minus), otherwise the literal/var portion.
func toBuilderClausePortion(r *big.Rat, asVar bool, div *PortionDiv) builder.Expression[builder.ExprTypePortion] {
	if div != nil {
		return builder.ExprPortionDiv(
			builder.ExprNumberVar(div.Num),
			builder.ExprNumberBigInt(div.Den),
		)
	}
	return toBuilderPortion(r, asVar)
}

func toBuilderDestination(d Destination) builder.Destination {
	switch d.Kind {
	case DestAccount:
		return builder.DestAccount(toBuilderAccount(d.Account, d.AccountAsVar))

	case DestInorder, DestOneof:
		clauses := make([]builder.DestInorderClause, len(d.InorderClauses))
		for i, c := range d.InorderClauses {
			clauses[i] = builder.DestInorderClause{
				Max:  toBuilderMonetary(c.Max),
				Dest: toBuilderKeptOrDest(c.KeptOrDest),
			}
		}
		if d.Kind == DestOneof {
			return builder.DestOneof(clauses, toBuilderKeptOrDest(*d.Remaining))
		}
		return builder.DestInorder(clauses, toBuilderKeptOrDest(*d.Remaining))

	case DestAllotment:
		clauses := make([]builder.AllotmentClause[builder.KeptOrDest], len(d.AllotClauses))
		for i, c := range d.AllotClauses {
			clauses[i] = builder.AllotmentClause[builder.KeptOrDest]{
				Portion: toBuilderClausePortion(c.Portion, c.PortionAsVar, c.Div),
				Payload: toBuilderKeptOrDest(c.KeptOrDest),
			}
		}
		if d.AllotRemaining != nil {
			return builder.DestAllotmentWithRemaining(clauses, toBuilderKeptOrDest(*d.AllotRemaining))
		}
		return builder.DestAllotment(clauses...)

	default:
		panic("gen: unknown destination kind")
	}
}

func toBuilderKeptOrDest(k KeptOrDest) builder.KeptOrDest {
	if k.Kind == Kept {
		return builder.Kept()
	}
	return builder.To(toBuilderDestination(*k.Dest))
}
