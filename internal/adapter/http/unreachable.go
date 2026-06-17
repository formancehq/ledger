package http

import "github.com/antithesishq/antithesis-sdk-go/assert"

// unreachable flags a should-not-happen branch to Antithesis and then panics.
//
// It is meant for invariant violations at the HTTP boundary — e.g. an apply
// that returned no error but also no log, which the backend contract forbids.
// The jsonRecoverer middleware recovers the panic into a JSON 500 with a stack
// trace, so the user-visible result is still a 500 while the stack pinpoints
// the offending handler. Under Antithesis, assert.Unreachable additionally
// fails the run, surfacing the violation loudly (CLAUDE.md invariant #7).
func unreachable(message string, details map[string]any) {
	assert.Unreachable(message, details)
	panic(message)
}
