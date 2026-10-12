package numscriptgen

import (
	"math/big"
	"math/rand"

	"github.com/formancehq/ledger/v3/tests/numscriptgen/builder"
)

// renderer turns the generator's AST into builder statements. The AST fixes
// every value; the renderer draws how each one is written — a literal, a var,
// or one of the arithmetic forms the grammar allows — so the same value
// reaches the engines through every expression shape.
type renderer struct {
	rng *rand.Rand
}

// toBuilder converts a generated program into builder
// statements, ready to be passed to builder.BuildProgram. Each account and
// asset occurrence is rendered inline or through a var according to its
// AsVar flag — see toBuilderAccount.
func (rd renderer) toBuilder(p Program) []builder.Statement {
	out := make([]builder.Statement, len(p))
	for i, s := range p {
		out[i] = rd.toBuilderStatement(s)
	}
	return out
}

func (rd renderer) toBuilderStatement(s Statement) builder.Statement {
	src := rd.toBuilderSource(s.Source)
	dest := rd.toBuilderDestination(s.Destination)

	if s.IsSendAll {
		return builder.StmtSendAll(rd.toBuilderAsset(s.Asset, s.AssetAsVar), src, dest)
	}
	return builder.StmtSend(rd.toBuilderMonetary(s.Amount), src, dest)
}

// toBuilderAccount renders one account occurrence, either inline (`@addr`) or
// through a pooled var (`$accountN` bound to addr). builder.ExprAccount pools
// by address, so every var-form occurrence of an address shares one var while
// inline occurrences stay literal — letting a single script reference the same
// account both ways.
func (rd renderer) toBuilderAccount(address string, asVar bool) builder.Expression[builder.ExprTypeAccount] {
	if asVar {
		return builder.ExprAccount(address)
	}
	return builder.UnsafeAccount(address)
}

// toBuilderString renders one string occurrence, var or inline.
func (rd renderer) toBuilderString(s string, asVar bool) builder.Expression[builder.ExprTypeString] {
	if asVar {
		return builder.ExprString(s)
	}
	return builder.ExprStringLit(s)
}

// toBuilderAsset renders one asset occurrence, var or inline. Note the asset
// slot of a monetary literal accepts a var in both grammars (`[$assetN 10]`),
// unlike the amount slot — see toBuilderMonetary.
func (rd renderer) toBuilderAsset(asset string, asVar bool) builder.Expression[builder.ExprTypeAsset] {
	if asVar {
		return builder.ExprAsset(asset)
	}
	return builder.UnsafeAsset(asset)
}

func (rd renderer) toBuilderMonetary(m Monetary) builder.Expression[builder.ExprTypeMonetary] {
	if m.AsVar && m.Amount.Sign() >= 0 {
		return builder.ExprMonetaryVar(m.Asset + " " + m.Amount.String())
	}

	asset := func() builder.Expression[builder.ExprTypeAsset] { return rd.toBuilderAsset(m.Asset, m.AssetAsVar) }
	lit := func(n *big.Int) builder.Expression[builder.ExprTypeMonetary] {
		return builder.ExprMonetary(asset(), builder.ExprNumberBigInt(n))
	}

	if m.Amount.Sign() < 0 {
		abs := new(big.Int).Neg(m.Amount)
		switch rd.rng.Intn(3) {
		case 0:
			return builder.ExprMonetarySub(lit(big.NewInt(0)), lit(abs))
		case 1:
			return builder.ExprNeg(lit(abs))
		default:
			return builder.ExprMonetary(asset(), builder.ExprNeg(builder.ExprNumberBigInt(abs)))
		}
	}

	switch rd.rng.Intn(16) {
	case 0:
		return builder.ExprMonetary(asset(), builder.ExprNumberVar(m.Amount))
	case 1:
		x, y := rd.split(m.Amount)
		return builder.ExprMonetary(asset(), rd.sum(builder.ExprNumberBigInt(x), y))
	case 2:
		x, y := rd.split(m.Amount)
		if y.Sign() < 0 {
			return builder.ExprMonetarySub(lit(x), lit(new(big.Int).Neg(y)))
		}
		return builder.ExprMonetaryAdd(lit(x), lit(y))
	case 3:
		return builder.ExprParen(lit(m.Amount))
	default:
		return lit(m.Amount)
	}
}

// split draws x and y with x + y = n, x non-negative and y of either sign.
func (rd renderer) split(n *big.Int) (x, y *big.Int) {
	x = big.NewInt(int64(rd.rng.Intn(1000)))
	return x, new(big.Int).Sub(n, x)
}

// sum renders x + y, writing a negative y as a subtraction.
func (rd renderer) sum(x builder.Expression[builder.ExprTypeNumber], y *big.Int) builder.Expression[builder.ExprTypeNumber] {
	if y.Sign() < 0 {
		return builder.ExprSub(x, builder.ExprNumberBigInt(new(big.Int).Neg(y)))
	}
	return builder.ExprAdd(x, builder.ExprNumberBigInt(y))
}

func (rd renderer) toBuilderNumExpr(e NumExpr) builder.Expression[builder.ExprTypeNumber] {
	var out builder.Expression[builder.ExprTypeNumber]
	switch e.Kind {
	case NumLit:
		switch {
		case e.LitAsVar:
			out = builder.ExprNumberVar(e.Lit)
		case e.Lit.Sign() < 0:
			out = builder.ExprNeg(builder.ExprNumberBigInt(new(big.Int).Neg(e.Lit)))
		default:
			out = builder.ExprNumberBigInt(e.Lit)
		}
	case NumAdd:
		out = builder.ExprAdd(rd.toBuilderNumExpr(*e.Left), rd.toBuilderNumExpr(*e.Right))
	case NumSub:
		out = builder.ExprSub(rd.toBuilderNumExpr(*e.Left), rd.toBuilderNumExpr(*e.Right))
	default:
		panic("gen: unknown num expr kind")
	}

	if e.Paren {
		out = builder.ExprParen(out)
	}
	if e.Neg {
		out = builder.ExprNeg(out)
	}

	return out
}

// toBuilderMetaLiteral renders set_tx_meta (account nil) or set_account_meta
// of a value typed other than number or string.
func (rd renderer) toBuilderMetaLiteral(l MetaLiteral, account builder.Expression[builder.ExprTypeAccount], key string) builder.Statement {
	switch l.Type {
	case MetaMonetary:
		return setMeta(account, key, rd.toBuilderMonetary(l.Monetary))
	case MetaAccount:
		return setMeta(account, key, rd.toBuilderAccount(l.Account, rd.rng.Intn(3) == 0))
	case MetaAsset:
		return setMeta(account, key, rd.toBuilderAsset(l.Asset, rd.rng.Intn(3) == 0))
	case MetaPortion:
		return setMeta(account, key, rd.toBuilderPortion(l.Portion, rd.rng.Intn(3) == 0))
	default:
		panic("gen: meta literal of an unsupported type")
	}
}

func setMeta[T builder.ExprType](account builder.Expression[builder.ExprTypeAccount], key string, value builder.Expression[T]) builder.Statement {
	if account == nil {
		return builder.StmtSetTxMeta(key, value)
	}
	return builder.StmtSetAccountMeta(account, key, value)
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

func (rd renderer) toBuilderVarExprs(vars []VarDecl) varExprs {
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
			account = rd.toBuilderAccount(v.Account, v.AccountAsVar)
		}
		switch v.Kind {
		case VarFromBalance:
			ve.monetary[i] = builder.NewMonetaryVarFromBalance(account, rd.toBuilderAsset(v.Asset, v.AssetAsVar))
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
func (rd renderer) toBuilderScript(s Script) ([]builder.Statement, []AccountVarFill) {
	ve := rd.toBuilderVarExprs(s.Vars)

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
	out = append(out, rd.toBuilder(s.Seeds)...)
	for _, st := range body {
		if st.send != nil {
			out = append(out, rd.toBuilderStatement(*st.send))
		} else {
			out = append(out, rd.toBuilderExtra(*st.extra, ve, accountVarExprs))
		}
	}

	return out, fills
}

func (rd renderer) toBuilderExtra(e ExtraStatement, ve varExprs, accountVarExprs []builder.Expression[builder.ExprTypeAccount]) builder.Statement {
	switch e.Kind {
	case ExtraSave:
		var mon builder.Expression[builder.ExprTypeMonetary]
		if e.VarIdx != nil {
			mon = ve.monetary[*e.VarIdx]
		} else {
			mon = rd.toBuilderMonetary(*e.Monetary)
		}
		return builder.StmtSave(mon, rd.toBuilderAccount(e.Account, e.AccountAsVar))

	case ExtraSaveAll:
		return builder.StmtSaveAll(rd.toBuilderAsset(e.Asset, e.AssetAsVar), rd.toBuilderAccount(e.Account, e.AccountAsVar))

	case ExtraSetTxMeta:
		if e.Literal != nil {
			return rd.toBuilderMetaLiteral(*e.Literal, nil, e.Key)
		}
		if e.StringValue != nil {
			return builder.StmtSetTxMeta(e.Key, rd.toBuilderString(*e.StringValue, e.StringValueAsVar))
		}
		return builder.StmtSetTxMeta(e.Key, rd.toBuilderNumExpr(e.Value))

	case ExtraSetAccountMeta:
		acct := rd.toBuilderAccount(e.Account, e.AccountAsVar)
		if e.Literal != nil {
			return rd.toBuilderMetaLiteral(*e.Literal, acct, e.Key)
		}
		if e.StringValue != nil {
			return builder.StmtSetAccountMeta(acct, e.Key, rd.toBuilderString(*e.StringValue, e.StringValueAsVar))
		}
		return builder.StmtSetAccountMeta(acct, e.Key, rd.toBuilderNumExpr(e.Value))

	case ExtraSendVar:
		return builder.StmtSend(
			ve.monetary[*e.VarIdx],
			builder.SrcAccountOverdraft(rd.toBuilderAccount(e.Account, e.AccountAsVar), builder.UnboundedOverdraft()),
			builder.DestAccount(rd.toBuilderAccount(e.Destination, e.DestinationAsVar)),
		)

	case ExtraSetTxMetaVar:
		return ve.setTxMeta[*e.VarIdx](e.Key)

	case ExtraSendFromAccountVar:
		return builder.StmtSend(
			rd.toBuilderMonetary(*e.Monetary),
			builder.SrcAccount(accountVarExprs[*e.AccountVarIdx]),
			builder.DestAccount(rd.toBuilderAccount(e.Account, e.AccountAsVar)),
		)

	case ExtraSendToAccountVar:
		return builder.StmtSend(
			rd.toBuilderMonetary(*e.Monetary),
			builder.SrcAccount(rd.toBuilderAccount(e.Account, e.AccountAsVar)),
			builder.DestAccount(accountVarExprs[*e.AccountVarIdx]),
		)

	default:
		panic("gen: unknown extra statement kind")
	}
}

// toBuilderPortion renders a portion inline — an `n/d` literal, or a
// percentage when it has one — or through a runtime-bound var bound to
// big.Rat's normalized num/denom.
func (rd renderer) toBuilderPortion(r *big.Rat, asVar bool) builder.Expression[builder.ExprTypePortion] {
	if asVar {
		return builder.ExprPortionVar(r.Num().String() + "/" + r.Denom().String())
	}
	if finiteDecimal(r) && rd.rng.Intn(3) == 0 {
		return builder.ExprPercent(r)
	}
	return builder.ExprPortion(builder.NewPortion(new(big.Int).Set(r.Num()), new(big.Int).Set(r.Denom())))
}

// finiteDecimal reports whether r has a terminating decimal expansion — its
// reduced denominator has no prime factor but 2 and 5 — so it can be written
// as a percentage literal.
func finiteDecimal(r *big.Rat) bool {
	d := new(big.Int).Set(r.Denom())
	for _, p := range []int64{2, 5} {
		bp := big.NewInt(p)
		for new(big.Int).Mod(d, bp).Sign() == 0 {
			d.Div(d, bp)
		}
	}
	return d.Cmp(big.NewInt(1)) == 0
}

func (rd renderer) toBuilderSource(s Source) builder.Source {
	switch s.Kind {
	case SrcAccount:
		if s.Color != "" {
			return builder.SrcColored(rd.toBuilderAccount(s.Account, s.AccountAsVar), rd.toBuilderString(s.Color, s.ColorAsVar))
		}
		return builder.SrcAccount(rd.toBuilderAccount(s.Account, s.AccountAsVar))

	case SrcAccountOverdraft:
		overdraft := builder.UnboundedOverdraft()
		if s.Overdraft != nil {
			overdraft = builder.BoundedOverdraft(rd.toBuilderMonetary(*s.Overdraft))
		}
		if s.Color != "" {
			return builder.SrcColoredOverdraft(rd.toBuilderAccount(s.Account, s.AccountAsVar), rd.toBuilderString(s.Color, s.ColorAsVar), overdraft)
		}
		return builder.SrcAccountOverdraft(rd.toBuilderAccount(s.Account, s.AccountAsVar), overdraft)

	case SrcCapped:
		return builder.SrcCapped(rd.toBuilderMonetary(*s.Cap), rd.toBuilderSource(*s.Inner))

	case SrcInorder, SrcOneof:
		sources := make([]builder.Source, len(s.Sources))
		for i, inner := range s.Sources {
			sources[i] = rd.toBuilderSource(inner)
		}
		if s.Kind == SrcOneof {
			return builder.SrcOneof(sources...)
		}
		return builder.SrcInorder(sources...)

	case SrcAllotment:
		clauses := make([]builder.AllotmentClause[builder.Source], len(s.Clauses))
		for i, c := range s.Clauses {
			clauses[i] = builder.AllotmentClause[builder.Source]{
				Portion: rd.toBuilderClausePortion(c.Portion, c.PortionAsVar, c.Div),
				Payload: rd.toBuilderSource(c.Source),
			}
		}
		if s.AllotmentRemaining != nil {
			return builder.SrcAllotmentWithRemaining(clauses, rd.toBuilderSource(*s.AllotmentRemaining))
		}
		return builder.SrcAllotment(clauses...)

	default:
		panic("gen: unknown source kind")
	}
}

// toBuilderClausePortion renders one allotment clause's portion: a division
// expression when Div is set (numerator through a number var, so negative
// values need no unary minus), otherwise the literal/var portion.
func (rd renderer) toBuilderClausePortion(r *big.Rat, asVar bool, div *PortionDiv) builder.Expression[builder.ExprTypePortion] {
	if div != nil {
		return builder.ExprPortionDiv(
			builder.ExprNumberVar(div.Num),
			builder.ExprNumberBigInt(div.Den),
		)
	}
	return rd.toBuilderPortion(r, asVar)
}

func (rd renderer) toBuilderDestination(d Destination) builder.Destination {
	switch d.Kind {
	case DestAccount:
		return builder.DestAccount(rd.toBuilderAccount(d.Account, d.AccountAsVar))

	case DestInorder, DestOneof:
		clauses := make([]builder.DestInorderClause, len(d.InorderClauses))
		for i, c := range d.InorderClauses {
			clauses[i] = builder.DestInorderClause{
				Max:  rd.toBuilderMonetary(c.Max),
				Dest: rd.toBuilderKeptOrDest(c.KeptOrDest),
			}
		}
		if d.Kind == DestOneof {
			return builder.DestOneof(clauses, rd.toBuilderKeptOrDest(*d.Remaining))
		}
		return builder.DestInorder(clauses, rd.toBuilderKeptOrDest(*d.Remaining))

	case DestAllotment:
		clauses := make([]builder.AllotmentClause[builder.KeptOrDest], len(d.AllotClauses))
		for i, c := range d.AllotClauses {
			clauses[i] = builder.AllotmentClause[builder.KeptOrDest]{
				Portion: rd.toBuilderClausePortion(c.Portion, c.PortionAsVar, c.Div),
				Payload: rd.toBuilderKeptOrDest(c.KeptOrDest),
			}
		}
		if d.AllotRemaining != nil {
			return builder.DestAllotmentWithRemaining(clauses, rd.toBuilderKeptOrDest(*d.AllotRemaining))
		}
		return builder.DestAllotment(clauses...)

	default:
		panic("gen: unknown destination kind")
	}
}

func (rd renderer) toBuilderKeptOrDest(k KeptOrDest) builder.KeptOrDest {
	if k.Kind == Kept {
		return builder.Kept()
	}
	return builder.To(rd.toBuilderDestination(*k.Dest))
}
