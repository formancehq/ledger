package filterexpr

import (
	"fmt"
	"strconv"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// Custom lexer: Keywords are matched before Ident so that reserved words
// (and, or, not, metadata, address, source, destination, exists, true, false)
// cannot be consumed as bare values.
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
	{Name: "Keyword", Pattern: `\b(and|or|not|metadata|address|source|destination|ledger|exists|true|false)\b`},
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
//	or_expr        := and_expr ("or" and_expr)*
//	and_expr       := unary_expr ("and" unary_expr)*
//	unary_expr     := "not" unary_expr | primary
//	primary        := "(" expression ")" | condition
//	condition      := metadata_cond | address_cond | source_cond | destination_cond
//	metadata_cond  := "metadata" "[" KEY "]" ("==" VALUE | "!=" VALUE | ">" VALUE | ">=" VALUE | "<" VALUE | "<=" VALUE | "exists")
//	address_cond   := ("address" | "source" | "destination") ("==" VALUE | "^=" VALUE)
//	value          := "$" Ident | "true" | "false" | String | Number | Ident
func Parse(input string) (*commonpb.QueryFilter, error) {
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
	Metadata *MetadataCond `parser:"  @@"`
	Address  *AddressCond  `parser:"| @@"`
	Ledger   *LedgerCond   `parser:"| @@"`
}

func (c *Condition) toProto() (*commonpb.QueryFilter, error) {
	if c.Metadata != nil {
		return c.Metadata.toProto()
	}
	if c.Ledger != nil {
		return c.Ledger.toProto()
	}
	return c.Address.toProto()
}

// --- Metadata conditions ---

type MetadataCond struct {
	Key     string       `parser:"'metadata' '[' @(Ident | Keyword | String | Number) ']'"`
	Exists  bool         `parser:"( @'exists'"`
	Compare *MetadataOp  `parser:"| @@ )"`
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
	Eq  *Value `parser:"  '==' @@"`
	Ne  *Value `parser:"| '!=' @@"`
	Gte *Value `parser:"| '>=' @@"`
	Gt  *Value `parser:"| '>' @@"`
	Lte *Value `parser:"| '<=' @@"`
	Lt  *Value `parser:"| '<' @@"`
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
	default:
		return nil, fmt.Errorf("missing operator")
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
	Keyword string `parser:"@('address' | 'source' | 'destination')"`
	Exact   *Value `parser:"( '==' @@"`
	Prefix  *Value `parser:"| '^=' @@ )"`
}

func (a *AddressCond) toProto() (*commonpb.QueryFilter, error) {
	role := commonpb.AddressRole_ADDRESS_ROLE_ANY
	switch a.Keyword {
	case "source":
		role = commonpb.AddressRole_ADDRESS_ROLE_SOURCE
	case "destination":
		role = commonpb.AddressRole_ADDRESS_ROLE_DESTINATION
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

// --- Value ---

type Value struct {
	Param  string `parser:"  '$' @Ident"`
	Str    string `parser:"| @String"`
	Num    string `parser:"| @Number"`
	Bool   string `parser:"| @('true' | 'false')"`
	Bare   string `parser:"| @Ident"`
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

func unquote(s string) string {
	if len(s) >= 2 && ((s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'')) {
		return s[1 : len(s)-1]
	}
	return s
}
