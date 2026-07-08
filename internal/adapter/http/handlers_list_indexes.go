package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleListLedgerIndexes handles GET /{ledgerName}/indexes to list the
// indexes registered for a specific ledger.
func (s *Server) handleListLedgerIndexes(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	cursor, err := s.backend.ListIndexes(r.Context(), &servicepb.ListIndexesRequest{
		Scope:  servicepb.ListIndexesRequest_SCOPE_LEDGER,
		Ledger: ledgerName,
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

// handleListBucketIndexes handles GET /indexes to list bucket-wide or
// cluster-wide indexes. The scope query parameter selects the flavor:
// "bucket" (default) surfaces only bucket-scoped entries, "all" streams
// every entry across ledgers.
func (s *Server) handleListBucketIndexes(w http.ResponseWriter, r *http.Request) {
	var scope servicepb.ListIndexesRequest_Scope
	switch r.URL.Query().Get("scope") {
	case "", "bucket":
		scope = servicepb.ListIndexesRequest_SCOPE_BUCKET
	case "all":
		scope = servicepb.ListIndexesRequest_SCOPE_ALL
	default:
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid scope parameter, expected bucket or all"))

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
