package sensitive

import "strings"

// redactKeywordDSN scans libpq/pgx keyword-value syntax without loading ambient
// credentials, service files or TLS keys. Preserve original non-secret values,
// including quoting/escaping, and mask every occurrence of a secret key. Invalid
// syntax is fully redacted; never return a parsing error containing raw input.
func redactKeywordDSN(raw string) string {
	const space = " \t\n\r\v\f"
	var fields []string
	for rest := strings.TrimSpace(raw); rest != ""; {
		eq := strings.IndexByte(rest, '=')
		if eq < 0 {
			return Marker
		}
		key := strings.Trim(rest[:eq], space)
		if key == "" || strings.ContainsAny(key, space+"'\\") {
			return Marker
		}
		rest = strings.TrimLeft(rest[eq+1:], space)
		end := 0
		quoted := strings.HasPrefix(rest, "'")
		if quoted {
			end = 1
		}
		closed := !quoted
		for end < len(rest) {
			c := rest[end]
			if c == '\\' {
				end += 2
				if end > len(rest) {
					return Marker
				}

				continue
			}
			if quoted && c == '\'' {
				end++
				closed = true

				break
			}
			if !quoted && strings.ContainsRune(space, rune(c)) {
				break
			}
			end++
		}
		if !closed {
			return Marker
		}
		value := rest[:end]
		if credentialQueryKeys[strings.ToLower(strings.ReplaceAll(key, "-", "_"))] {
			value = "'xxxxx'"
		}
		fields = append(fields, key+"="+value)
		rest = strings.TrimLeft(rest[end:], space)
	}

	return strings.Join(fields, " ")
}
