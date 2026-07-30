package http

import (
	"fmt"
	"maps"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// exactlyOneLog enforces the unitary-handler backend contract: one admitted
// request yields exactly one non-nil log, whose payload type is fixed by the
// request type. Zero logs, several logs, or a nil sole log are impossible
// backend responses; each panics through unreachable, so jsonRecoverer surfaces
// a sanitized JSON 500 while Antithesis flags the run (CLAUDE.md invariant #7).
// operation names the endpoint and details carries the stable request context
// (ledger, transaction id, ...) threaded into the signal.
func exactlyOneLog(operation string, logs []*commonpb.Log, details map[string]any) *commonpb.Log {
	if len(logs) != 1 {
		panic(unreachable(operation+" apply did not return exactly one log", mergeDetails(details, map[string]any{
			"log_count": len(logs),
		})))
	}

	if logs[0] == nil {
		panic(unreachable(operation+" apply returned a nil log", details))
	}

	return logs[0]
}

// unexpectedLogPayload builds the invariant signal for a sole log whose payload
// type is not the one the request implies, and returns the value the caller must
// raise: panic(unexpectedLogPayload(...)).
func unexpectedLogPayload(operation string, log *commonpb.Log, details map[string]any) string {
	return unreachable(operation+" apply returned an unexpected log payload type", observedPayloadDetails(log, details))
}

// emptyLogPayload builds the invariant signal for a correctly-typed sole log
// whose meaningful body is nil — an impossible backend response that would
// otherwise serialize to a 2xx with a null data body. Returns the value the
// caller must raise: panic(emptyLogPayload(...)).
func emptyLogPayload(operation string, log *commonpb.Log, details map[string]any) string {
	return unreachable(operation+" apply returned a log with no payload body", observedPayloadDetails(log, details))
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
