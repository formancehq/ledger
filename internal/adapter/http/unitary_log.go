package http

import (
	"fmt"
	"maps"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// exactlyOneLog enforces the unitary-handler backend contract: one admitted
// request yields exactly one non-nil log, whose payload type is fixed by the
// request type. Zero logs, several logs, or a nil sole log are impossible
// backend responses; each fails loudly: assert.Unreachable flags the run under
// Antithesis (CLAUDE.md invariant #7) and the panic makes jsonRecoverer answer
// a sanitized JSON 500. operation names the endpoint and details carries the
// stable request context (ledger, transaction id, ...) threaded into the signal.
//
// Every assert.Unreachable message in this file must stay a string literal at
// the call site: the antithesis-go-instrumentor catalogues assertions by
// statically resolving that argument, and anything else degrades to a single
// anonymous catalog entry. Per-operation context therefore travels in the
// details map and in the panic value's message prefix.
func exactlyOneLog(operation string, logs []*commonpb.Log, details map[string]any) *commonpb.Log {
	if len(logs) != 1 {
		d := mergeDetails(details, map[string]any{"operation": operation, "log_count": len(logs)})
		assert.Unreachable("unitary apply did not return exactly one log", d)
		panic(invariantPanicValue(operation+" apply did not return exactly one log", d))
	}

	if logs[0] == nil {
		d := mergeDetails(details, map[string]any{"operation": operation})
		assert.Unreachable("unitary apply returned a nil log", d)
		panic(invariantPanicValue(operation+" apply returned a nil log", d))
	}

	return logs[0]
}

// unexpectedLogPayload builds the invariant signal for a sole log whose payload
// type is not the one the request implies, and returns the value the caller must
// raise: panic(unexpectedLogPayload(...)).
func unexpectedLogPayload(operation string, log *commonpb.Log, details map[string]any) string {
	d := observedPayloadDetails(log, mergeDetails(details, map[string]any{"operation": operation}))
	assert.Unreachable("unitary apply returned an unexpected log payload type", d)

	return invariantPanicValue(operation+" apply returned an unexpected log payload type", d)
}

// emptyLogPayload builds the invariant signal for a correctly-typed sole log
// whose meaningful body is nil — an impossible backend response that would
// otherwise serialize to a 2xx with a null data body. Returns the value the
// caller must raise: panic(emptyLogPayload(...)).
func emptyLogPayload(operation string, log *commonpb.Log, details map[string]any) string {
	d := observedPayloadDetails(log, mergeDetails(details, map[string]any{"operation": operation}))
	assert.Unreachable("unitary apply returned a log with no payload body", d)

	return invariantPanicValue(operation+" apply returned a log with no payload body", d)
}

// invariantPanicValue renders an invariant-violation message plus its diagnostic
// details into the panic value the handler raises. jsonRecoverer logs that value
// server-side (with a stack trace) and records it on the OTel span before
// answering a sanitized JSON 500; assert.Unreachable is a no-op outside the
// Antithesis environment, so the panic value is how the details reach an
// operator's logs. The rendering is deterministic (fmt sorts map keys). The
// value can embed internal state, so it must never reach the client (#375).
func invariantPanicValue(message string, details map[string]any) string {
	return fmt.Sprintf("%s %v", message, details)
}

// observedPayloadDetails merges the observed payload types of log into a copy of
// base: the outer payload type always, and — for an Apply log — the inner
// ledger-log payload type. Kept a pure function so the diagnostics are unit
// testable without raising a panic.
func observedPayloadDetails(log *commonpb.Log, base map[string]any) map[string]any {
	extra := map[string]any{
		"sequence":           log.GetSequence(),
		"outer_payload_type": fmt.Sprintf("%T", log.GetPayload().GetType()),
	}

	if apply := log.GetPayload().GetApply(); apply != nil {
		extra["inner_payload_type"] = fmt.Sprintf("%T", apply.GetLog().GetData().GetPayload())
	}

	return mergeDetails(base, extra)
}

// mergeDetails merges extra into a copy of base, leaving the caller's map
// untouched so the same request context can seed several invariant checks.
func mergeDetails(base, extra map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(extra))
	maps.Copy(merged, base)
	maps.Copy(merged, extra)

	return merged
}
