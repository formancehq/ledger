package http

import (
	"net/http"

	servicepb "github.com/formancehq/ledger/pkg/client/v3/grpc"
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

	writeProtoListOK(w, entries)
}
