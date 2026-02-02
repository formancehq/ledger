package resources

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func ReplaceVariables(s string, vars map[string]any) (string, error) {
	strs, varRefs, err := ParseTemplate(s)
	if err != nil {
		return "", err
	}
	var buf strings.Builder
	for i := range len(strs) + 1 {
		if i < len(strs) {
			buf.WriteString(strs[i])
		}
		if i < len(varRefs) {
			if v, ok := vars[varRefs[i]]; ok {
				s, err := jsonToString(v)
				if err != nil {
					return "", err
				}
				buf.WriteString(s)
			} else {
				return "", fmt.Errorf("missing variable: %s", varRefs[i])
			}
		}
	}
	return buf.String(), nil
}

func jsonToString(value any) (string, error) {
	switch v := value.(type) {
	// we might want to disallow this completely
	case float64:
		if math.Floor(v) != v {
			return "", errors.New("numbers with decimals are not allowed")
		}
		return strconv.FormatInt(int64(v), 10), nil
	case json.Number:
		return string(v), nil
	case string:
		return v, nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return "", fmt.Errorf("unexpected variable type: %T", value)
	}

}

type parserState struct {
	str   string
	index int
}

type ParsingError struct {
	state     parserState
	expecting string
}

func (p parserState) newParsingError(expecting string) *ParsingError {
	return &ParsingError{
		state:     p,
		expecting: expecting,
	}
}

var _ error = (*ParsingError)(nil)

func (e ParsingError) Error() string {
	var currentChar string
	if e.state.isEOF() {
		currentChar = "EOF"
	} else {
		currentChar = fmt.Sprintf("'%c'", e.state.peek())
	}

	return fmt.Sprintf("i was expecting %s, but I got %s instead", e.expecting, currentChar)
}

func (p parserState) isEOF() bool {
	return len(p.str) <= p.index
}

// Panics on EOF
func (p parserState) peek() byte {
	return p.str[p.index]
}

// Panics on EOF
func (p *parserState) consume() byte {
	ch := p.peek()
	p.index++
	return ch
}

// Returns whether the lookahead is matched by the predicat.
// Consumes on match
func (p *parserState) tryConsuming(pred func(byte) bool) (byte, bool) {
	if p.isEOF() {
		return 0x0, false
	}

	ch := p.peek()
	if pred(ch) {
		p.consume()
		return ch, true
	}

	return 0x0, false
}

func (p *parserState) tryConsumingCh(lookup byte) bool {
	_, ok := p.tryConsuming(func(b byte) bool {
		return b == lookup
	})

	return ok
}

// lowercase chars
func isVarHeadChar(b byte) bool {
	return b >= 'a' && b <= 'z'
}

// alphanum chars or '_'
func isVarTailChar(b byte) bool {
	return isVarHeadChar(b) || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

// Parse and consume the var identifier until we get a non-identifier char
func (p *parserState) parseVarIdent() (string, *ParsingError) {
	var sb strings.Builder

	ch, ok := p.tryConsuming(isVarHeadChar)
	if !ok {
		return "", p.newParsingError("a lowercase char")
	}
	sb.WriteByte(ch)

	for {
		ch, ok := p.tryConsuming(isVarTailChar)
		if !ok {
			// first non-identifier char means we are outside interpolation
			break
		}

		sb.WriteByte(ch)
	}

	return sb.String(), nil
}

// parse the $abc syntax
// PRE: already consumed opening '${'
func (p *parserState) parseSimpleVar() (string, *ParsingError) {

	if p.tryConsumingCh('{') {
		return p.parseBracketVar()
	}

	return p.parseVarIdent()
}

// parse the ${abc} syntax
// PRE: already consumed opening '${'
func (p *parserState) parseBracketVar() (string, *ParsingError) {
	ident, err := p.parseVarIdent()
	if err != nil {
		return "", err
	}

	if !p.tryConsumingCh('}') {
		return "", p.newParsingError("'}'")
	}

	return ident, nil
}

func ParseTemplate(str string) ([]string, []string, *ParsingError) {
	p := parserState{str: str}

	// The following state is modelled as local scope by design
	var strs []string
	var vars []string
	currentStr := ""

	for !p.isEOF() {
		b := p.consume()

		switch b {
		case '$':
			// TODO do we append even empty str?
			strs = append(strs, currentStr)
			currentStr = ""

			var_, err := p.parseSimpleVar()
			if err != nil {
				return nil, nil, err
			}
			vars = append(vars, var_)

		default:
			currentStr += string(b)
		}
	}

	if currentStr != "" {
		strs = append(strs, currentStr)
	}

	return strs, vars, nil
}
