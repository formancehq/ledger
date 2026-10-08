// Package gen produces random numscript programs for differential testing.
//
// Generation targets this package's own AST rather than builder/ directly:
// cleanup.go rewrites the tree in place, and builder's Source/Destination are
// opaque render closures. convert.go renders once cleanup has settled.
package numscriptgen

import "math/big"

// An "AsVar" field marks one value occurrence as written through a `vars {}`
// variable instead of inline. Per occurrence, not per value: the same account
// can be inline in one place and a var in another, which is what makes two
// resources alias one account (formancehq/ledger#2056).
//
// Only the rendering changes, never the value, so cleanup.go is unaffected.
type Monetary struct {
	Asset      string
	AssetAsVar bool
	// Ignored for negative amounts: they have no literal form to bind.
	AsVar  bool
	Amount *big.Int
}

type SourceKind int

const (
	SrcAccount SourceKind = iota
	SrcAccountOverdraft
	SrcCapped
	SrcInorder
	SrcAllotment
	// numscript-only (experimental-oneof): scripts containing it skip the
	// oracle legs.
	SrcOneof
)

type Source struct {
	Kind SourceKind

	// SrcAccount, SrcAccountOverdraft
	Account      string
	AccountAsVar bool

	// SrcAccount, SrcAccountOverdraft: non-empty adds a `\ "COLOR"` clause.
	// numscript-only (experimental-asset-colors): scripts containing it skip
	// the oracle legs.
	Color      string
	ColorAsVar bool

	// SrcAccountOverdraft only: nil means unbounded overdraft
	Overdraft *Monetary

	// SrcCapped only
	Cap   *Monetary
	Inner *Source

	// SrcInorder, SrcOneof
	Sources []Source

	// SrcAllotment only
	Clauses []SourceAllotmentClause
	// SrcAllotment only: non-nil adds a trailing `remaining from` clause, and
	// the clause portions sum to strictly less than 1.
	AllotmentRemaining *Source
}

// PortionDiv is a portion written as a division expression `$num/den` (the
// numerator always renders through a runtime-bound number var, so any sign is
// expressible without unary minus). Unlike literals and portion vars, the value
// is unconstrained: negative and over-one portions are reachable this way, and
// both engines must reject them the same way. numscript-only: the oracle's grammar
// has no division expression.
type PortionDiv struct {
	Num *big.Int
	Den *big.Int
}

func (d PortionDiv) Rat() *big.Rat { return new(big.Rat).SetFrac(d.Num, d.Den) }

type SourceAllotmentClause struct {
	Portion *big.Rat
	// Rendered as a runtime-bound portion var instead of an n/d literal. Only
	// legal in an allotment with a remaining clause: without one, the oracle
	// rejects the block at compile time ("might be less than 100%").
	PortionAsVar bool
	// When non-nil, the clause portion is Div rendered as a division expression
	// and Portion/PortionAsVar are ignored.
	Div    *PortionDiv
	Source Source
}

type DestKind int

const (
	DestAccount DestKind = iota
	DestInorder
	DestAllotment
	// numscript-only (experimental-oneof), same clause fields as DestInorder.
	DestOneof
)

type Destination struct {
	Kind DestKind

	// DestAccount only
	Account      string
	AccountAsVar bool

	// DestInorder, DestOneof
	InorderClauses []DestInorderClause
	Remaining      *KeptOrDest

	// DestAllotment only
	AllotClauses []DestAllotmentClause
	// DestAllotment only: non-nil adds a trailing `remaining` clause, and the
	// clause portions sum to strictly less than 1.
	AllotRemaining *KeptOrDest
}

type DestInorderClause struct {
	Max        Monetary
	KeptOrDest KeptOrDest
}

type DestAllotmentClause struct {
	Portion *big.Rat
	// Same constraint as SourceAllotmentClause.PortionAsVar.
	PortionAsVar bool
	// Same meaning as SourceAllotmentClause.Div.
	Div        *PortionDiv
	KeptOrDest KeptOrDest
}

type KeptOrDestKind int

const (
	Kept KeptOrDestKind = iota
	To
)

type KeptOrDest struct {
	Kind KeptOrDestKind
	Dest *Destination // valid only when Kind == To
}

type Statement struct {
	// If IsSendAll, Asset is used (unbounded send). Otherwise Amount is used.
	IsSendAll bool
	Amount    Monetary
	Asset     string
	// IsSendAll only.
	AssetAsVar  bool
	Source      Source
	Destination Destination
}

type Program []Statement

// NumExprKind is the arithmetic allowed in set_tx_meta/set_account_meta
// values. Only +/- over literals and vars: that is all NumScript.g4 has.
type NumExprKind int

const (
	NumLit NumExprKind = iota
	NumAdd
	NumSub
)

type NumExpr struct {
	Kind NumExprKind

	// NumLit only
	Lit *big.Int
	// NumLit only.
	LitAsVar bool

	// NumAdd, NumSub only
	Left, Right *NumExpr
}

// VarDeclKind is the origin of a `vars {}` declaration this generator can
// produce: either `monetary $name = balance(<account>, <asset>)` or
// `number $name = meta(<account>, "<key>")`.
type VarDeclKind int

const (
	VarFromBalance VarDeclKind = iota
	VarFromMeta
)

// MetaType is the type a `meta()` origin var is declared as. meta() returns
// TypeAny, so the declaration decides how the stored string is parsed and the
// var only compiles if the type matches the preset value.
type MetaType int

const (
	MetaNumber MetaType = iota
	MetaString
	MetaMonetary
	MetaAsset
	MetaAccount
	MetaPortion
)

// VarDecl is a `vars {}` declaration whose value comes from the compiler, not
// from a runtime binding.
type VarDecl struct {
	Kind    VarDeclKind
	Account string
	// Makes `balance($accountN, ...)` and a literal `@acc` elsewhere alias
	// one account.
	AccountAsVar bool
	// Chains this origin onto an earlier declaration: the account read by
	// balance()/meta() is the origin var at this index, which must be an
	// earlier VarFromMeta declaration of MetaType MetaAccount. Account then
	// holds the account name that parent's meta value resolves to (known at
	// generation time), so the balance/metadata this declaration observes can
	// still be steered; AccountAsVar is ignored.
	AccountFromVarIdx *int

	// VarFromBalance only
	Asset      string
	AssetAsVar bool

	// VarFromMeta only
	Key string
	// VarFromMeta only. Must match how the preset value at (Account, Key) is
	// written.
	MetaType MetaType
}

// BalanceKey identifies one (account, asset) starting balance.
type BalanceKey struct {
	Account string
	Asset   string
}

// MetaKey identifies one (account, key) starting metadata entry.
type MetaKey struct {
	Account string
	Key     string
}

// ExtraStatementKind enumerates the non-send statement kinds this generator
// can produce, on top of Program's send-only statements.
type ExtraStatementKind int

const (
	ExtraSave ExtraStatementKind = iota
	ExtraSaveAll
	ExtraSetTxMeta
	ExtraSetAccountMeta
	ExtraSendVar
	ExtraSetTxMetaVar
	ExtraSendFromAccountVar
	ExtraSendToAccountVar
)

// AccountVarDecl is an account-typed `vars {}` declaration bound at run time
// from the vars map, unlike VarDecl which the compiler computes from an origin
// call.
//
// Value is sometimes "world": the oracle rejects a var resolving to @world in
// source position, the interpreter accepts it. Compare tolerates that
// direction.
type AccountVarDecl struct {
	Value string
}

// ExtraStatement is one non-send statement. Only the fields relevant to Kind
// are populated.
type ExtraStatement struct {
	Kind ExtraStatementKind

	// ExtraSave: the amount to save, either a literal (Monetary) or a
	// reference to Script.Vars[*VarIdx] (a declared VarFromBalance var) —
	// exactly one of the two is set. ExtraSendFromAccountVar,
	// ExtraSendToAccountVar: the send amount (always a literal).
	Monetary *Monetary

	// ExtraSave, ExtraSendVar (must index a VarFromBalance decl),
	// ExtraSetTxMetaVar (must index a VarFromMeta decl)
	VarIdx *int

	// ExtraSendFromAccountVar, ExtraSendToAccountVar: index into
	// Script.AccountVars for the account-typed var used as source/
	// destination (respectively); the other side is the literal Account.
	AccountVarIdx *int

	// ExtraSave (source account), ExtraSetAccountMeta, ExtraSendVar (source),
	// ExtraSendFromAccountVar (destination), ExtraSendToAccountVar (source)
	Account      string
	AccountAsVar bool

	// ExtraSaveAll
	Asset      string
	AssetAsVar bool

	// ExtraSetTxMeta, ExtraSetAccountMeta, ExtraSetTxMetaVar
	Key   string
	Value NumExpr
	// ExtraSetTxMeta, ExtraSetAccountMeta: when non-nil the meta value is this
	// string rather than the Value expression, exercising string-typed values.
	StringValue      *string
	StringValueAsVar bool

	// ExtraSendVar: `send $<Vars[VarIdx]> (source = Account, destination = Destination)`
	Destination      string
	DestinationAsVar bool
}

// Focus is the (account, asset) every statement of a scenario block shares.
type Focus struct {
	Account string
	Asset   string
}

// ScenarioStmt is one statement of a scenario block. Exactly one of Send and
// Extra is set. Kind is the setup/observer kind that built it, for coverage
// counting.
type ScenarioStmt struct {
	Kind  string
	Send  *Statement
	Extra *ExtraStatement
}

// Strategy is how one Script's body was assembled.
type Strategy int

const (
	// Program and Extra only, riffled.
	StrategyUniform Strategy = iota
	// Program and Extra riffled, with a scenario block spliced in.
	StrategyScenarioMixed
	// Seeds and a scenario block only.
	StrategyScenarioOnly
)

// Script is the full output of one round of generation: a `vars {}` block,
// optional seed-funding statements, the core send-only program, extra
// non-send statements, an optional scenario block, and/or pre-set starting
// balances/metadata (populated instead of, or alongside, Seeds — see
// genBalances). Order records a riffle-interleaving of Program and Extra (true
// = take next from Program, false = take next from Extra), so extra statements
// land throughout the script instead of only after every send. Scenario is
// emitted contiguously, just before the Order step at index ScenarioPos, or
// after the last step when ScenarioPos == len(Order). bodyStatements is the
// one walk of that layout.
type Script struct {
	Vars        []VarDecl
	AccountVars []AccountVarDecl
	Seeds       Program
	Program     Program
	Extra       []ExtraStatement
	Order       []bool
	Strategy    Strategy
	Focus       *Focus
	Scenario    []ScenarioStmt
	ScenarioPos int
	Balances    map[BalanceKey]*big.Int
	Metadata    map[MetaKey]string
}
