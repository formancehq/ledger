package http

import (
	"net/http"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
)

// handleGetBucketIndex handles GET /indexes/{canonicalId} to fetch a single
// bucket-scoped Index registry entry (Ledger == ""). Ledger-scoped indexes
// go through GET /v3/{ledgerName}/indexes/{canonicalId} — a bucket-scoped
// lookup with the same canonical would silently return NotFound because
// the key is distinct.
func (s *Server) handleGetBucketIndex(w http.ResponseWriter, r *http.Request) {
	canonical, ok := requireCanonicalID(w, r)
	if !ok {
		return
	}

	id, err := indexes.ParseCanonical(canonical)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	idx, err := s.backend.GetIndex(r.Context(), &ledgerpb.GetIndexRequest{
		Id: id,
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeProtoOK(w, idx)
}
