package http

import (
	"net/http"
	"strings"
)

// httpHeaderBigintAsString is the EN-1779 opt-in: when a client sends it, every
// Posting.amount in the response body is a quoted decimal string instead of a
// bare JSON number, so JavaScript clients do not truncate above 2^53. The header
// name and its lenient parsing match Ledger v2 for client parity.
const httpHeaderBigintAsString = "Formance-Bigint-As-String"

// wantsBigintAsString reports whether the client opted into quoted decimal
// amounts. Parsing is lenient and matches v2: true|yes|y|1, case-insensitive,
// surrounding whitespace ignored. Anything else — including an unrecognised
// value — is treated as absent rather than rejected, so a malformed header
// never turns a readable response into a 400.
func wantsBigintAsString(r *http.Request) bool {
	switch strings.ToLower(strings.TrimSpace(r.Header.Get(httpHeaderBigintAsString))) {
	case "true", "yes", "y", "1":
		return true
	default:
		return false
	}
}
