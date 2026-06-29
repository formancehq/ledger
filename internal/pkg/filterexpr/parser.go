package filterexpr

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// MaxParseDepth bounds the nesting depth participle is allowed to
// descend through during Parse. Participle does recursive-descent
// parsing on the grammar rules `UnaryExpr → 'not' UnaryExpr` and
// `Primary → '(' OrExpr ')'`; an input with 100k repetitions of
// `not ` or `(` overflows the Go stack — a fatal, unrecoverable
// process abort (review-2 L-19 / #341).
//
// 200 is well above any legitimate filter expression. The check is
// a syntactic upper bound (counts `not` keywords plus open parens),
// not an exact AST depth — but those tokens are the only sources of
// participle recursion in this grammar.
const MaxParseDepth = 200

// ErrFilterTooDeep is returned by Parse when the lexical nesting
// indicators exceed MaxParseDepth.
var ErrFilterTooDeep = fmt.Errorf("filter expression nesting exceeds maximum depth (%d)", MaxParseDepth)

// Custom lexer: Keywords are matched before Ident so that reserved words
// (and, or, not, between, metadata, address, source, destination, exists,
// true, false) cannot be consumed as bare values.
var filterLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Whitespace", Pattern: `\s+`},
	{Name: "OpEq", Pattern: `==`},
	{Name: "OpNe", Pattern: `!=`},
	{Name: "OpPrefix", Pattern: `\^=`},
	{Name: "OpGte", Pattern: `>=`},
	{Name: "OpLte", Pattern: `<=`},
	{Name: "OpGt", Pattern: `>`},
	{Name: "OpLt", Pattern: `<`},
	{Name: "LBracket", Pattern: `\[`},
	{Name: "RBracket", Pattern: `\]`},
	{Name: "LParen", Pattern: `\(`},
	{Name: "RParen", Pattern: `\)`},
	{Name: "Dollar", Pattern: `\$`},
	{Name: "String", Pattern: `"[^"]*"|'[^']*'`},
	{Name: "Comma", Pattern: `,`},
	{Name: "Keyword", Pattern: `\b(and|or|not|in|between|metadata|address|source|destination|ledger|exists|true|false)\b`},
	{Name: "Number", Pattern: `-?[0-9]+`},
	{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_:.\-/]*`},
})

var filterParser = participle.MustBuild[OrExpr](
	participle.Lexer(filterLexer),
	participle.Elide("Whitespace"),
)

// Parse parses a human-readable filter expression into a QueryFilter.
//
// Grammar:
//
//	expression     := or_expr
//	:= and_expr ("or" and_expr)*
//	and_expr       := unary_expr ("and" unary_expr)*
//	unary_expr     := "not" unary_expr | primary
//	:= "(" expression ")" | condition
//	:= metadata_cond | address_cond | source_cond | destination_cond
//	metadata_cond  := "metadata" "[" KEY "]" ("==" VALUE | "!=" VALUE | ">" VALUE | ">=" VALUE | "<" VALUE | "<=" VALUE | "between" VALUE "and" VALUE | "exists" | "in" "(" VALUE ("," VALUE)* ")")
//	address_cond   := ("address" | "source" | "destination") ("==" VALUE | "^=" VALUE | "in" "(" VALUE ("," VALUE)* ")")
//	value          := "$" Ident | "true" | "false" | String | Number | Ident
func Parse(input string) (*commonpb.QueryFilter, error) {
	// Reject pathologically nested inputs BEFORE handing them to
	// participle. Participle's recursive-descent parser would
	// otherwise stack-overflow on counts beyond a few thousand,
	// killing the process (#341). Count open parens plus `not`
	// occurrences as a conservative syntactic depth proxy.
	if strings.Count(input, "(")+strings.Count(input, "not") > MaxParseDepth {
		return nil, ErrFilterTooDeep
	}

	ast, err := filterParser.ParseString("", input)
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}

	return ast.toProto()
}

// --- AST types ---

type OrExpr struct {
	Operands []*AndExpr `parser:"@@ ('or' @@)*"`
}

func (e *OrExpr) toProto() (*commonpb.QueryFilter, error) {
	if len(e.Operands) == 1 {
		return e.Operands[0].toProto()
	}

	filters := make([]*commonpb.QueryFilter, len(e.Operands))
	for i, op := range e.Operands {
		f, err := op.toProto()
		if err != nil {
			return nil, err
		}

		filters[i] = f
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: filters},
		},
	}, nil
}

type AndExpr struct {
	Operands []*UnaryExpr `parser:"@@ ('and' @@)*"`
}

func (e *AndExpr) toProto() (*commonpb.QueryFilter, error) {
	if len(e.Operands) == 1 {
		return e.Operands[0].toProto()
	}

	filters := make([]*commonpb.QueryFilter, len(e.Operands))
	for i, op := range e.Operands {
		f, err := op.toProto()
		if err != nil {
			return nil, err
		}

		filters[i] = f
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: filters},
		},
	}, nil
}

type UnaryExpr struct {
	Not     *UnaryExpr `parser:"  'not' @@"`
	Primary *Primary   `parser:"| @@"`
}

func (e *UnaryExpr) toProto() (*commonpb.QueryFilter, error) {
	if e.Not != nil {
		inner, err := e.Not.toProto()
		if err != nil {
			return nil, err
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{Filter: inner},
			},
		}, nil
	}

	return e.Primary.toProto()
}

type Primary struct {
	Group     *OrExpr    `parser:"  '(' @@ ')'"`
	Condition *Condition `parser:"| @@"`
}

func (p *Primary) toProto() (*commonpb.QueryFilter, error) {
	if p.Group != nil {
		return p.Group.toProto()
	}

	return p.Condition.toProto()
}

type Condition struct {
	Asset    *AssetCond    `parser:"  @@"`
	Metadata *MetadataCond `parser:"| @@"`
	Address  *AddressCond  `parser:"| @@"`
	Ledger   *LedgerCond   `parser:"| @@"`
}

func (c *Condition) toProto() (*commonpb.QueryFilter, error) {
	if c.Asset != nil {
		return c.Asset.toProto()
	}

	if c.Metadata != nil {
		return c.Metadata.toProto()
	}

	if c.Ledger != nil {
		return c.Ledger.toProto()
	}

	return c.Address.toProto()
}

// --- Asset conditions ---

// AssetCond is the `has asset <assetRef>` filter. <assetRef> is a bare base
// ("USD" → precision 0) or base/precision ("USD/4"). Resolved via the
// ACCT_BUILTIN_INDEX_ASSET readstore index.
type AssetCond struct {
	Asset string `parser:"'has' 'asset' @Ident"`
}

func (a *AssetCond) toProto() (*commonpb.QueryFilter, error) {
	base, precision, err := splitAsset(a.Asset)
	if err != nil {
		return nil, err
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_AccountHasAsset{
			AccountHasAsset: &commonpb.AccountHasAssetCondition{
				AssetBase: base,
				Precision: uint32(precision),
			},
		},
	}, nil
}

// splitAsset splits an asset string of the form "BASE" or "BASE/PRECISION"
// into its base and precision parts. It defers to the canonical asset rules
// in internal/domain — the single source of truth shared by the hot path
// (domain.ValidateAsset in processor_transaction) and the volume-key encoder
// (domain.ParseAssetPrecision in keys.go) — rather than re-deriving them here.
// Validate-then-parse mirrors that idiom: ValidateAsset rejects a malformed
// base, a non-numeric or out-of-range precision, and the non-canonical
// suffixes ("USD/0", "USD/02", "USD/2/3") that would alias a different volume
// cell, so ParseAssetPrecision can split a known-canonical string. A bare base
// (no "/") yields precision 0.
func splitAsset(asset string) (string, uint8, error) {
	if err := domain.ValidateAsset(asset); err != nil {
		return "", 0, fmt.Errorf("invalid asset %q: %w", asset, err)
	}

	base, precision := domain.ParseAssetPrecision(asset)

	return base, precision, nil
}

// --- Metadata conditions ---

type MetadataCond struct {
	Key     string      `parser:"'metadata' '[' @(Ident | Keyword | String | Number) ']'"`
	Exists  bool        `parser:"( @'exists'"`
	Compare *MetadataOp `parser:"| @@ )"`
}

func (m *MetadataCond) toProto() (*commonpb.QueryFilter, error) {
	key := unquote(m.Key)
	field := &commonpb.FieldRef{Metadata: key}

	if m.Exists {
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field:     field,
					Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
				},
			},
		}, nil
	}

	return m.Compare.toProto(field)
}

type MetadataOp struct {
	Eq      *Value        `parser:"  '==' @@"`
	Ne      *Value        `parser:"| '!=' @@"`
	Gte     *Value        `parser:"| '>=' @@"`
	Gt      *Value        `parser:"| '>' @@"`
	Lte     *Value        `parser:"| '<=' @@"`
	Lt      *Value        `parser:"| '<' @@"`
	Between *BetweenRange `parser:"| 'between' @@"`
	In      []*Value      `parser:"| 'in' '(' @@ (',' @@)* ')'"`
}

// BetweenRange parses `LOW and HIGH` for the `between` operator. The inner
// 'and' is consumed here while parsing the metadata condition, so the outer
// AndExpr never sees it — participle's PEG ordering ensures the BetweenRange
// production wins over the AndExpr continuation at this point in the grammar.
type BetweenRange struct {
	Low  *Value `parser:"@@"`
	High *Value `parser:"'and' @@"`
}

func (op *MetadataOp) toProto(field *commonpb.FieldRef) (*commonpb.QueryFilter, error) {
	switch {
	case op.Eq != nil:
		return metadataEqualityToProto(field, op.Eq)
	case op.Ne != nil:
		inner, err := metadataEqualityToProto(field, op.Ne)
		if err != nil {
			return nil, err
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{Filter: inner},
			},
		}, nil
	case op.Gt != nil:
		return metadataRangeToProto(field, op.Gt, ">")
	case op.Gte != nil:
		return metadataRangeToProto(field, op.Gte, ">=")
	case op.Lt != nil:
		return metadataRangeToProto(field, op.Lt, "<")
	case op.Lte != nil:
		return metadataRangeToProto(field, op.Lte, "<=")
	case op.Between != nil:
		return metadataBetweenToProto(field, op.Between)
	case len(op.In) > 0:
		return metadataInToProto(field, op.In)
	default:
		return nil, errors.New("missing operator")
	}
}

// --- Ledger conditions ---

type LedgerCond struct {
	Exact *Value `parser:"'ledger' '==' @@"`
}

func (l *LedgerCond) toProto() (*commonpb.QueryFilter, error) {
	cond := &commonpb.StringCondition{}
	if l.Exact.Param != "" {
		cond.Value = &commonpb.StringCondition_Param{Param: l.Exact.Param}
	} else {
		cond.Value = &commonpb.StringCondition_Hardcoded{Hardcoded: l.Exact.resolve()}
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Ledger{
			Ledger: &commonpb.LedgerCondition{Cond: cond},
		},
	}, nil
}

// --- Address conditions ---

type AddressCond struct {
	Keyword string   `parser:"@('address' | 'source' | 'destination')"`
	Exact   *Value   `parser:"( '==' @@"`
	Prefix  *Value   `parser:"| '^=' @@"`
	In      []*Value `parser:"| 'in' '(' @@ (',' @@)* ')' )"`
}

func (a *AddressCond) toProto() (*commonpb.QueryFilter, error) {
	role := addressRole(a.Keyword)

	if len(a.In) > 0 {
		return addressInToProto(role, a.In)
	}

	am := &commonpb.AddressMatch{Role: role}

	if a.Exact != nil {
		if a.Exact.Param != "" {
			am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: a.Exact.Param}
		} else {
			am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: a.Exact.resolve()}
		}
	} else {
		if a.Prefix.Param != "" {
			am.Match = &commonpb.AddressMatch_ParamPrefix{ParamPrefix: a.Prefix.Param}
		} else {
			am.Match = &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: a.Prefix.resolve()}
		}
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{Address: am},
	}, nil
}

func addressRole(keyword string) commonpb.AddressRole {
	switch keyword {
	case "source":
		return commonpb.AddressRole_ADDRESS_ROLE_SOURCE
	case "destination":
		return commonpb.AddressRole_ADDRESS_ROLE_DESTINATION
	default:
		return commonpb.AddressRole_ADDRESS_ROLE_ANY
	}
}

// --- Value ---

type Value struct {
	Param string `parser:"  '$' @Ident"`
	Str   string `parser:"| @String"`
	Num   string `parser:"| @Number"`
	Bool  string `parser:"| @('true' | 'false')"`
	Bare  string `parser:"| @Ident"`
}

func (v *Value) resolve() string {
	if v.Str != "" {
		return unquote(v.Str)
	}

	if v.Num != "" {
		return v.Num
	}

	if v.Bool != "" {
		return v.Bool
	}

	return v.Bare
}

// --- Proto conversion helpers ---

func metadataEqualityToProto(field *commonpb.FieldRef, val *Value) (*commonpb.QueryFilter, error) {
	fc := &commonpb.FieldCondition{Field: field}

	if val.Param != "" {
		// Parameterized: default to string
		fc.Condition = &commonpb.FieldCondition_StringCond{
			StringCond: &commonpb.StringCondition{
				Value: &commonpb.StringCondition_Param{Param: val.Param},
			},
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{Field: fc},
		}, nil
	}

	raw := val.resolve()

	switch raw {
	case "true":
		fc.Condition = &commonpb.FieldCondition_BoolCond{
			BoolCond: &commonpb.BoolCondition{
				Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true},
			},
		}
	case "false":
		fc.Condition = &commonpb.FieldCondition_BoolCond{
			BoolCond: &commonpb.BoolCondition{
				Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: false},
			},
		}
	default:
		if intVal, intErr := strconv.ParseInt(raw, 10, 64); intErr == nil {
			fc.Condition = &commonpb.FieldCondition_IntCond{
				IntCond: &commonpb.IntCondition{
					Min: &intVal,
					Max: &intVal,
				},
			}
		} else {
			fc.Condition = &commonpb.FieldCondition_StringCond{
				StringCond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{Hardcoded: raw},
				},
			}
		}
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{Field: fc},
	}, nil
}

func metadataRangeToProto(field *commonpb.FieldRef, val *Value, op string) (*commonpb.QueryFilter, error) {
	if val.Param != "" {
		ic := &commonpb.IntCondition{}

		switch op {
		case ">":
			ic.ParamMin = val.Param
			ic.MinExclusive = true
		case ">=":
			ic.ParamMin = val.Param
		case "<":
			ic.ParamMax = val.Param
			ic.MaxExclusive = true
		case "<=":
			ic.ParamMax = val.Param
		}

		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field:     field,
					Condition: &commonpb.FieldCondition_IntCond{IntCond: ic},
				},
			},
		}, nil
	}

	raw := val.resolve()

	intVal, intErr := strconv.ParseInt(raw, 10, 64)
	if intErr != nil {
		return nil, fmt.Errorf("range operators only support integer values, got %q", raw)
	}

	ic := &commonpb.IntCondition{}

	switch op {
	case ">":
		ic.Min = &intVal
		ic.MinExclusive = true
	case ">=":
		ic.Min = &intVal
	case "<":
		ic.Max = &intVal
		ic.MaxExclusive = true
	case "<=":
		ic.Max = &intVal
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     field,
				Condition: &commonpb.FieldCondition_IntCond{IntCond: ic},
			},
		},
	}, nil
}

// metadataBetweenToProto desugars `metadata[key] between LOW and HIGH` into
// a single IntCondition with both bounds set, inclusive on both ends (SQL
// semantics: LOW <= value <= HIGH). Hardcoded bounds with LOW > HIGH return
// a parse error — a transposed pair is almost certainly a bug, not a request
// for the empty result.
func metadataBetweenToProto(field *commonpb.FieldRef, r *BetweenRange) (*commonpb.QueryFilter, error) {
	low := r.Low
	high := r.High

	if low.Param != "" || high.Param != "" {
		ic := &commonpb.IntCondition{}

		if low.Param != "" {
			ic.ParamMin = low.Param
		} else {
			v, err := parseIntValue(low)
			if err != nil {
				return nil, err
			}
			ic.Min = &v
		}

		if high.Param != "" {
			ic.ParamMax = high.Param
		} else {
			v, err := parseIntValue(high)
			if err != nil {
				return nil, err
			}
			ic.Max = &v
		}

		return wrapIntCondition(field, ic), nil
	}

	lowVal, err := parseIntValue(low)
	if err != nil {
		return nil, err
	}

	highVal, err := parseIntValue(high)
	if err != nil {
		return nil, err
	}

	if lowVal > highVal {
		return nil, fmt.Errorf("between bounds out of order: %d > %d", lowVal, highVal)
	}

	return wrapIntCondition(field, &commonpb.IntCondition{
		Min: &lowVal,
		Max: &highVal,
	}), nil
}

// parseIntValue parses a Value as a 64-bit signed integer, returning the same
// error message that metadataRangeToProto uses so the two operators report
// type-mismatches consistently.
func parseIntValue(v *Value) (int64, error) {
	raw := v.resolve()

	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("range operators only support integer values, got %q", raw)
	}

	return n, nil
}

// wrapIntCondition packages an IntCondition as a leaf FieldCondition.
func wrapIntCondition(field *commonpb.FieldRef, ic *commonpb.IntCondition) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     field,
				Condition: &commonpb.FieldCondition_IntCond{IntCond: ic},
			},
		},
	}
}

// metadataInToProto desugars `metadata[key] in (v1, v2, ...)` into
// an OrFilter of equality conditions, one per value.
func metadataInToProto(field *commonpb.FieldRef, values []*Value) (*commonpb.QueryFilter, error) {
	filters := make([]*commonpb.QueryFilter, len(values))
	for i, v := range values {
		f, err := metadataEqualityToProto(field, v)
		if err != nil {
			return nil, err
		}

		filters[i] = f
	}

	return wrapOrFilter(filters), nil
}

// addressInToProto desugars `address in (v1, v2, ...)` into
// an OrFilter of exact address matches, one per value.
func addressInToProto(role commonpb.AddressRole, values []*Value) (*commonpb.QueryFilter, error) {
	filters := make([]*commonpb.QueryFilter, len(values))
	for i, v := range values {
		am := &commonpb.AddressMatch{Role: role}
		if v.Param != "" {
			am.Match = &commonpb.AddressMatch_ParamExact{ParamExact: v.Param}
		} else {
			am.Match = &commonpb.AddressMatch_HardcodedExact{HardcodedExact: v.resolve()}
		}

		filters[i] = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{Address: am},
		}
	}

	return wrapOrFilter(filters), nil
}

// wrapOrFilter returns the single filter if len==1, otherwise wraps in OrFilter.
func wrapOrFilter(filters []*commonpb.QueryFilter) *commonpb.QueryFilter {
	if len(filters) == 1 {
		return filters[0]
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: filters},
		},
	}
}

func unquote(s string) string {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1]
	}

	return s
}
