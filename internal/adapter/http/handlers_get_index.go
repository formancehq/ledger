package http

import (
	"net/http"

	servicepb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
)

// handleGetIndex handles GET /{ledgerName}/indexes/{canonicalId} to fetch
// a single index registry entry.
func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	canonical, ok := requireCanonicalID(w, r)
	if !ok {
		return
	}

	id, err := indexes.ParseCanonical(canonical)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	idx, err := s.backend.GetIndex(r.Context(), &servicepb.GetIndexRequest{
		Ledger: ledgerName,
		Id:     id,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeProtoOK(w, idx)
}
