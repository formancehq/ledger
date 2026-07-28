package http

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
)

// unreachable reports a should-not-happen branch to Antithesis and returns the
// value the caller must raise: panic(unreachable(...)).
//
// Returning rather than panicking keeps the termination visible to the compiler
// at the call site, and folds details into the panic value. That value is what
// the jsonRecoverer middleware logs server-side (with a stack trace) before
// answering a sanitized JSON 500 — assert.Unreachable is a no-op outside the
// Antithesis environment, so without this the details map would never reach an
// operator's logs. The rendering is deterministic (fmt sorts map keys). The
// value can embed internal state, so it must never reach the client; jsonRecoverer
// sends it only to the server log and the OTel span (#375).
func unreachable(message string, details map[string]any) string {
	assert.Unreachable(message, details)

	if len(details) == 0 {
		return message
	}

	return fmt.Sprintf("%s %v", message, details)
}
