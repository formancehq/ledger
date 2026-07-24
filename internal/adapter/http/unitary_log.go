package http

import (
	"fmt"
	"maps"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// exactlyOneLog enforces the unitary-handler backend contract: one admitted
// request yields exactly one non-nil log, whose payload type is fixed by the
// request type. Zero logs, several logs, or a nil sole log are impossible
// backend responses; each fails loudly through unreachable, so jsonRecoverer
// surfaces a sanitized JSON 500 while Antithesis flags the run (CLAUDE.md
// invariant #7). operation names the endpoint and details carries the stable
// request context (ledger, transaction id) threaded into the signal.
func exactlyOneLog(operation string, logs []*commonpb.Log, details map[string]any) *commonpb.Log {
	if len(logs) != 1 {
		unreachable(operation+" apply did not return exactly one log", logDetails(details, map[string]any{
			"log_count": len(logs),
		}))
	}

	if logs[0] == nil {
		unreachable(operation+" apply returned a nil log", logDetails(details, map[string]any{
			"log_count": len(logs),
		}))
	}

	return logs[0]
}

// unexpectedLogPayload flags a sole log whose payload type is not the one the
// request type implies. It records the actual outer payload type — and, for an
// Apply log, the inner ledger-log payload type — alongside the request context
// so the invariant signal pinpoints the mismatch.
func unexpectedLogPayload(operation string, log *commonpb.Log, details map[string]any) {
	extra := map[string]any{
		"sequence":           log.GetSequence(),
		"outer_payload_type": fmt.Sprintf("%T", log.GetPayload().GetType()),
	}

	if apply := log.GetPayload().GetApply(); apply != nil {
		extra["inner_payload_type"] = fmt.Sprintf("%T", apply.GetLog().GetData().GetPayload())
	}

	unreachable(operation+" apply returned an unexpected log payload type", logDetails(details, extra))
}

// logDetails merges extra into a copy of base, leaving the caller's map
// untouched so the same request context can seed several invariant checks.
func logDetails(base, extra map[string]any) map[string]any {
	merged := make(map[string]any, len(base)+len(extra))
	maps.Copy(merged, base)
	maps.Copy(merged, extra)

	return merged
}
