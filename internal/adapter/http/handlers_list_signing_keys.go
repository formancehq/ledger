package http

import (
	"net/http"
)

// handleListSigningKeys handles GET /signing-keys to list registered
// Ed25519 signing keys.
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
