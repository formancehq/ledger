package http

import (
	"net/http"
)

// handleListSigningKeys handles GET /signing-keys to list registered
// Ed25519 signing keys.
//
// Live, best-effort read: it drains the full cursor and does not expose the
// gRPC read-consistency options (checkpointId / minLogSequence) or a
// bidirectional cursor. Clients needing consistency-bounded reads use gRPC.
// Tracked follow-up (same carve-out as ListTransactions / the audit reads).
func (s *Server) handleListSigningKeys(w http.ResponseWriter, r *http.Request) {
	cursor, err := s.backend.ListSigningKeys(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	keys, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	writeProtoListOK(w, keys)
}
