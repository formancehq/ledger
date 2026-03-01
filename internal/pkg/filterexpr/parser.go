package filterexpr

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
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
//	condition      := metadata_cond | address_cond
//	metadata_cond  := "metadata" "[" KEY "]" ("==" VALUE | "!=" VALUE | "exists")
//	address_cond   := "address" ("==" VALUE | "^=" VALUE)
func Parse(input string) (*commonpb.QueryFilter, error) {
	p := &parser{tokens: tokenize(input)}
	filter, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if p.pos < len(p.tokens) {
		return nil, fmt.Errorf("unexpected token %q at position %d", p.tokens[p.pos].value, p.tokens[p.pos].pos)
	}
	return filter, nil
}

// Token types
type tokenKind int

const (
	tokenWord tokenKind = iota
	tokenString
	tokenLBracket
	tokenRBracket
	tokenLParen
	tokenRParen
	tokenOpEq
	tokenOpNe
	tokenOpPrefix
)

type token struct {
	kind  tokenKind
	value string
	pos   int
}

func tokenize(input string) []token {
	var tokens []token
	i := 0
	for i < len(input) {
		// Skip whitespace
		if unicode.IsSpace(rune(input[i])) {
			i++
			continue
		}

		pos := i

		// Two-character operators
		if i+1 < len(input) {
			twoChar := input[i : i+2]
			switch twoChar {
			case "==":
				tokens = append(tokens, token{kind: tokenOpEq, value: "==", pos: pos})
				i += 2
				continue
			case "!=":
				tokens = append(tokens, token{kind: tokenOpNe, value: "!=", pos: pos})
				i += 2
				continue
			case "^=":
				tokens = append(tokens, token{kind: tokenOpPrefix, value: "^=", pos: pos})
				i += 2
				continue
			}
		}

		// Single-character tokens
		switch input[i] {
		case '[':
			tokens = append(tokens, token{kind: tokenLBracket, value: "[", pos: pos})
			i++
			continue
		case ']':
			tokens = append(tokens, token{kind: tokenRBracket, value: "]", pos: pos})
			i++
			continue
		case '(':
			tokens = append(tokens, token{kind: tokenLParen, value: "(", pos: pos})
			i++
			continue
		case ')':
			tokens = append(tokens, token{kind: tokenRParen, value: ")", pos: pos})
			i++
			continue
		}

		// Quoted strings
		if input[i] == '"' || input[i] == '\'' {
			quote := input[i]
			i++
			start := i
			for i < len(input) && input[i] != quote {
				i++
			}
			if i >= len(input) {
				tokens = append(tokens, token{kind: tokenString, value: input[start:], pos: pos})
			} else {
				tokens = append(tokens, token{kind: tokenString, value: input[start:i], pos: pos})
				i++ // skip closing quote
			}
			continue
		}

		// Words (bare words, keywords, numbers, etc.)
		start := i
		for i < len(input) && !unicode.IsSpace(rune(input[i])) && !strings.ContainsRune("[]()!=^", rune(input[i])) {
			i++
		}
		if i > start {
			tokens = append(tokens, token{kind: tokenWord, value: input[start:i], pos: pos})
		}
	}
	return tokens
}

type parser struct {
	tokens []token
	pos    int
}

func (p *parser) peek() *token {
	if p.pos >= len(p.tokens) {
		return nil
	}
	return &p.tokens[p.pos]
}

func (p *parser) advance() token {
	t := p.tokens[p.pos]
	p.pos++
	return t
}

func (p *parser) expect(kind tokenKind) (token, error) {
	t := p.peek()
	if t == nil {
		return token{}, fmt.Errorf("unexpected end of expression, expected %s", kindName(kind))
	}
	if t.kind != kind {
		return token{}, fmt.Errorf("expected %s at position %d, got %q", kindName(kind), t.pos, t.value)
	}
	return p.advance(), nil
}

func kindName(k tokenKind) string {
	switch k {
	case tokenWord:
		return "word"
	case tokenString:
		return "string"
	case tokenLBracket:
		return "'['"
	case tokenRBracket:
		return "']'"
	case tokenLParen:
		return "'('"
	case tokenRParen:
		return "')'"
	case tokenOpEq:
		return "'=='"
	case tokenOpNe:
		return "'!='"
	case tokenOpPrefix:
		return "'^='"
	default:
		return "unknown"
	}
}

func (p *parser) parseExpression() (*commonpb.QueryFilter, error) {
	return p.parseOr()
}

func (p *parser) parseOr() (*commonpb.QueryFilter, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	var operands []*commonpb.QueryFilter
	for p.isKeyword("or") {
		p.advance()
		if operands == nil {
			operands = []*commonpb.QueryFilter{left}
		}
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		operands = append(operands, right)
	}

	if operands != nil {
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Or{
				Or: &commonpb.OrFilter{Filters: operands},
			},
		}, nil
	}
	return left, nil
}

func (p *parser) parseAnd() (*commonpb.QueryFilter, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	var operands []*commonpb.QueryFilter
	for p.isKeyword("and") {
		p.advance()
		if operands == nil {
			operands = []*commonpb.QueryFilter{left}
		}
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		operands = append(operands, right)
	}

	if operands != nil {
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{Filters: operands},
			},
		}, nil
	}
	return left, nil
}

func (p *parser) parseUnary() (*commonpb.QueryFilter, error) {
	if p.isKeyword("not") {
		p.advance()
		inner, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{Filter: inner},
			},
		}, nil
	}
	return p.parsePrimary()
}

func (p *parser) parsePrimary() (*commonpb.QueryFilter, error) {
	t := p.peek()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of expression")
	}

	// Grouped expression
	if t.kind == tokenLParen {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(tokenRParen); err != nil {
			return nil, err
		}
		return expr, nil
	}

	// Condition
	return p.parseCondition()
}

func (p *parser) parseCondition() (*commonpb.QueryFilter, error) {
	t := p.peek()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of expression, expected condition")
	}

	switch {
	case t.kind == tokenWord && t.value == "metadata":
		return p.parseMetadataCondition()
	case t.kind == tokenWord && t.value == "address":
		return p.parseAddressCondition()
	default:
		return nil, fmt.Errorf("expected 'metadata' or 'address' at position %d, got %q", t.pos, t.value)
	}
}

func (p *parser) parseMetadataCondition() (*commonpb.QueryFilter, error) {
	p.advance() // consume "metadata"

	if _, err := p.expect(tokenLBracket); err != nil {
		return nil, err
	}

	keyTok, err := p.parseValue()
	if err != nil {
		return nil, fmt.Errorf("expected metadata key: %w", err)
	}

	if _, err := p.expect(tokenRBracket); err != nil {
		return nil, err
	}

	// Check for operator
	t := p.peek()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of expression after metadata[%s]", keyTok)
	}

	field := &commonpb.FieldRef{Metadata: keyTok}

	switch {
	case t.kind == tokenOpEq:
		p.advance()
		return p.parseMetadataEqualityCondition(field)
	case t.kind == tokenOpNe:
		p.advance()
		inner, err := p.parseMetadataEqualityCondition(field)
		if err != nil {
			return nil, err
		}
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{
				Not: &commonpb.NotFilter{Filter: inner},
			},
		}, nil
	case t.kind == tokenWord && t.value == "exists":
		p.advance()
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field:     field,
					Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("expected '==', '!=' or 'exists' at position %d, got %q", t.pos, t.value)
	}
}

func (p *parser) parseMetadataEqualityCondition(field *commonpb.FieldRef) (*commonpb.QueryFilter, error) {
	val, err := p.parseValue()
	if err != nil {
		return nil, fmt.Errorf("expected value: %w", err)
	}

	// Auto-type the value: bool, int64, or string
	fc := &commonpb.FieldCondition{Field: field}

	switch val {
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
		if intVal, intErr := strconv.ParseInt(val, 10, 64); intErr == nil {
			fc.Condition = &commonpb.FieldCondition_IntCond{
				IntCond: &commonpb.IntCondition{
					Min: &intVal,
					Max: &intVal,
				},
			}
		} else {
			fc.Condition = &commonpb.FieldCondition_StringCond{
				StringCond: &commonpb.StringCondition{
					Value: &commonpb.StringCondition_Hardcoded{Hardcoded: val},
				},
			}
		}
	}

	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{Field: fc},
	}, nil
}

func (p *parser) parseAddressCondition() (*commonpb.QueryFilter, error) {
	p.advance() // consume "address"

	t := p.peek()
	if t == nil {
		return nil, fmt.Errorf("unexpected end of expression after 'address'")
	}

	switch t.kind {
	case tokenOpEq:
		p.advance()
		val, err := p.parseValue()
		if err != nil {
			return nil, fmt.Errorf("expected address value: %w", err)
		}
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: val},
				},
			},
		}, nil
	case tokenOpPrefix:
		p.advance()
		val, err := p.parseValue()
		if err != nil {
			return nil, fmt.Errorf("expected address prefix value: %w", err)
		}
		return &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Address{
				Address: &commonpb.AddressMatch{
					Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: val},
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf("expected '==' or '^=' after 'address' at position %d, got %q", t.pos, t.value)
	}
}

func (p *parser) parseValue() (string, error) {
	t := p.peek()
	if t == nil {
		return "", fmt.Errorf("unexpected end of expression, expected value")
	}

	switch t.kind {
	case tokenString, tokenWord:
		return p.advance().value, nil
	default:
		return "", fmt.Errorf("expected value at position %d, got %q", t.pos, t.value)
	}
}

func (p *parser) isKeyword(keyword string) bool {
	t := p.peek()
	return t != nil && t.kind == tokenWord && t.value == keyword
}

