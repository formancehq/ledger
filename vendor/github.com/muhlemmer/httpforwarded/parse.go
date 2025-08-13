// Copyright 2009 The Go Authors. All rights reserved.
// Copyright 2016 Tim Heckman. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package httpforwarded

import (
	"bytes"
	"errors"
	"net/http"
	"strings"
	"unicode"
)

// Parse is a helper function for parsing the HTTP Forwarded header as defined
// in RFC-7239. There can be multiple Forwarded entries within a single HTTP
// request, so this function takes a slice of strings. The parse function takes
// care to preserve the order of parameter value as it was seen. Some of the
// parameters may have multiple values.
//
// This function was inspired by the mime.ParseMediaType() function in the
// Go stdlib.
func Parse(values []string) (map[string][]string, error) {
	if len(values) == 0 {
		return nil, nil
	}

	params := make(map[string][]string)

	for _, v := range values {
		for len(v) > 0 {
			// trim any leading whitespace
			v = strings.TrimLeftFunc(v, unicode.IsSpace)

			if len(v) == 0 {
				break
			}

			// consume a key=value pair from the value
			key, value, rest := consumeForwardedParam(v)

			if key == "" {
				if strings.TrimSpace(rest) == ";" {
					// Ignore trailing semicolons.
					// Not an error.
					return params, nil
				}
				// Parse error.
				return nil, errors.New("forwarded: invalid parameter")
			}

			if _, exists := params[key]; exists {
				params[key] = append(params[key], value)
			} else {
				// allocate new slice with a len of 1 and a cap of 3
				// then set the one value we have
				params[key] = make([]string, 1, 3)
				params[key][0] = value
			}

			v = rest
		}
	}

	return params, nil
}

// ParseFromRequest is a helper function that is identical to Parse, except that
// it automatically extracts the "Forwarded" header values from the *http.Request.
func ParseFromRequest(req *http.Request) (map[string][]string, error) {
	headerValues := req.Header[http.CanonicalHeaderKey("forwarded")]

	return Parse(headerValues)
}

// ParseParameter parses the Forwarded header values and returns a slice of
// parameter values. The paramName parameter should be a lowercase string.
func ParseParameter(paramName string, values []string) ([]string, error) {
	if paramName == "" {
		return nil, errors.New(`paramName must not be ""`)
	}

	if len(values) == 0 {
		return nil, nil
	}

	paramValues := make([]string, 0, 2)

	for _, v := range values {
		for len(v) > 0 {
			// trim any leading whitespace
			v = strings.TrimLeftFunc(v, unicode.IsSpace)

			if len(v) == 0 {
				break
			}

			// consume a key-value pair from the value
			key, value, rest := consumeForwardedParam(v)

			if key == "" {
				if strings.TrimSpace(rest) == ";" {
					// Ignore trailing semicolons.
					// Not an error.
					return paramValues, nil
				}
				// Parse error.
				return nil, errors.New("forwarded: invalid parameter")
			}

			v = rest

			// if this isn't the key we are looking for, move on
			if key != paramName {
				continue
			}

			paramValues = append(paramValues, value)
		}
	}

	return paramValues, nil
}

func consumeForwardedParam(v string) (param, value, rest string) {
	// trim any preliminary whitespace
	rest = strings.TrimLeftFunc(v, unicode.IsSpace)

	// if the first character is one of our separators
	// consume the separator and any whitespace
	if rest[0] == ';' || rest[0] == ',' {
		rest = strings.TrimLeftFunc(rest[1:], unicode.IsSpace)
	}

	// consume the parameter name and make sure it's not empty
	param, rest = consumeToken(rest)
	if param == "" {
		return "", "", v
	}

	// trim spaces and make sure we are at the '=' separator
	rest = strings.TrimLeftFunc(rest, unicode.IsSpace)
	if !strings.HasPrefix(rest, "=") {
		return "", "", v
	}

	// consume the equals sign and any whitespace
	rest = strings.TrimLeftFunc(rest[1:], unicode.IsSpace)

	// get the value of the parameter and make sure it's not empty
	value, rest2 := consumeValue(rest)
	if value == "" && rest2 == rest {
		return "", "", v
	}

	rest = rest2
	return strings.ToLower(param), value, rest
}

// consumeValue consumes a "value" per RFC 2045, where a value is
// either a 'token' or a 'quoted-string'.  On success, consumeValue
// returns the value consumed (and de-quoted/escaped, if a
// quoted-string) and the rest of the string. On failure, returns
// ("", v).
func consumeValue(v string) (value, rest string) {
	if v == "" {
		return
	}
	if v[0] != '"' {
		return consumeToken(v)
	}

	// parse a quoted-string
	rest = v[1:] // consume the leading quote
	buffer := new(bytes.Buffer)
	var nextIsLiteral bool
	for idx, r := range rest {
		switch {
		case nextIsLiteral:
			buffer.WriteRune(r)
			nextIsLiteral = false
		case r == '"':
			return buffer.String(), rest[idx+1:]
		case r == '\\':
			nextIsLiteral = true
		case r != '\r' && r != '\n':
			buffer.WriteRune(r)
		default:
			return "", v
		}
	}
	return "", v
}

// consumeToken consumes a token from the beginning of provided
// string, per RFC 2045 section 5.1 (referenced from 2183), and return
// the token consumed and the rest of the string. Returns ("", v) on
// failure to consume at least one character.
func consumeToken(v string) (token, rest string) {
	notPos := strings.IndexFunc(v, isNotTokenChar)
	if notPos == -1 {
		return v, ""
	}
	if notPos == 0 {
		return "", v
	}
	return v[0:notPos], v[notPos:]
}

// isTSpecial reports whether rune is in 'tspecials' as defined by RFC
// 1521 and RFC 2045.
func isTSpecial(r rune) bool {
	return strings.ContainsRune(`()<>@,;:\"/[]?=`, r)
}

// isTokenChar reports whether rune is in 'token' as defined by RFC
// 1521 and RFC 2045.
func isTokenChar(r rune) bool {
	// token := 1*<any (US-ASCII) CHAR except SPACE, CTLs,
	//             or tspecials>
	return r > 0x20 && r < 0x7f && !isTSpecial(r)
}

func isNotTokenChar(r rune) bool {
	return !isTokenChar(r)
}
