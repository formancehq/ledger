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

func (p *parserState) isEOF() bool {
	return len(p.str) <= p.index
}

// Panics on EOF
func (p *parserState) peek() byte {
	return p.str[p.index]
}

// Panics on EOF
func (p *parserState) consume() byte {
	ch := p.peek()
	p.index++
	return ch
}

// PRE: already consumed opening '<'
// POST: closing '>' is consumed
func (p *parserState) parseVar() (string, error) {
	buf := ""
	for !p.isEOF() {
		b := p.consume()
		switch b {
		case '>':
			return buf, nil
		default:
			// TODO maybe we want to allow other chars?
			isValidVarChar := (b >= 'a' && b <= 'z') || b == '_'
			if !isValidVarChar {
				return "", fmt.Errorf("invalid var char: '%b'", b)
			}

			buf += string(b)
		}
	}
	return buf, nil
}

func ParseTemplate(str string) ([]string, []string, error) {
	p := parserState{str: str}

	// The following state is modelled as local scope by design
	var strs []string
	var vars []string
	currentStr := ""

	for !p.isEOF() {
		b := p.consume()

		switch b {
		case '<':
			// TODO do we append even empty str?
			strs = append(strs, currentStr)
			currentStr = ""

			var_, err := p.parseVar()
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
