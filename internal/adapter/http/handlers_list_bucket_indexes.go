package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleListBucketIndexes handles GET /indexes to list bucket-wide or
// cluster-wide indexes. The scope query parameter selects the flavor:
// "all" (default, matches the ListIndexesRequest proto default) streams
// every entry across ledgers; "bucket" surfaces only bucket-scoped
// entries (empty ledger). Callers looking for a specific ledger must
// use the per-ledger route GET /v3/{ledgerName}/indexes.
//
// Note on bucket-scoped entries: today NO production call site writes
// an Index entry to SubAttrIndex with an empty ledger — processCreateIndex
// requires a loaded ledger, and the audit index lives in a dedicated
// keyspace (SubInternalAuditIndex, read-store zone 0x05) instead of the
// SubAttrIndex registry. So scope=bucket returns empty today. The
// route is the hook for future cross-ledger indexes that DO land in the
// registry. Audit index observability specifically is tracked as
// EN-1481 with its own endpoint layout (GET /v3/audit-entries), not
// through this route.
func (s *Server) handleListBucketIndexes(w http.ResponseWriter, r *http.Request) {
	var scope servicepb.ListIndexesRequest_Scope
	switch r.URL.Query().Get("scope") {
	case "", "all":
		scope = servicepb.ListIndexesRequest_SCOPE_ALL
	case "bucket":
		scope = servicepb.ListIndexesRequest_SCOPE_BUCKET
	case "ledger":
		writeBadRequest(w, "INVALID_REQUEST", errors.New("scope=ledger is not supported here — use GET /v3/{ledgerName}/indexes"))

		return
	default:
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid scope parameter, expected all or bucket"))

		return
	}

	cursor, err := s.backend.ListIndexes(r.Context(), &servicepb.ListIndexesRequest{
		Scope: scope,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	entries, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	writeOK(w, entries)
}
