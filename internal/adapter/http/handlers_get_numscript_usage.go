package http

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// templateUsageJSON is the camelCase JSON DTO for TemplateUsage. It
// matches the OpenAPI contract for `GET /v3/{ledger}/numscripts/{name}/usage`:
//
//	{ "count": <uint64>, "lastUsed": "<RFC3339 timestamp | omitted>" }
//
// Emitting the raw protobuf struct would leak snake_case field tags
// (`last_used`) and the wire encoding of Timestamp (`{data: <int64>}`) —
// neither of which is what the API contract promises.
type templateUsageJSON struct {
	Count    uint64  `json:"count"`
	LastUsed *string `json:"lastUsed,omitempty"`
}

func toTemplateUsageJSON(usage *commonpb.TemplateUsage) *templateUsageJSON {
	out := &templateUsageJSON{Count: usage.GetCount()}

	if ts := usage.GetLastUsed(); ts != nil && ts.GetData() != 0 {
		// Timestamp.Data is Unix microseconds — use the canonical AsTime()
		// converter (time.UnixMicro) rather than treating Data as nanoseconds.
		formatted := ts.AsTime().UTC().Format(time.RFC3339Nano)
		out.LastUsed = &formatted
	}

	return out
}

// handleGetNumscriptUsage handles GET /{ledgerName}/numscripts/{name}/usage.
// Returns the invocation counter + last-used timestamp populated by the
// usagebuilder subsystem. Values are eventually consistent with the FSM
// (may lag by up to one usagebuilder tick). A never-invoked template on
// an existing ledger returns a zero-valued response, not a 404 — clients
// treat 0 uniformly. Unknown / soft-deleted ledgers surface a 404 (via
// the underlying controller).
func (s *Server) handleGetNumscriptUsage(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	usage, err := s.backend.GetTemplateUsage(r.Context(), ledgerName, name)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toTemplateUsageJSON(usage))
}
