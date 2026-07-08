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
// entries (empty ledger, e.g. audit indexes). Callers looking for a
// specific ledger must use the per-ledger route
// GET /v3/{ledgerName}/indexes.
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
