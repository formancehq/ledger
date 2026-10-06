package http

import (
	"net/http"
)

// handleListSigningKeys handles GET /signing-keys to list registered
// Ed25519 signing keys.
//
// This route performs a live linearizable read and drains the full cursor. It
// does not expose the gRPC bidirectional cursor; signing-key reads are
// live-only on both transports.
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
