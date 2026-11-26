package http

import (
	"net/http"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleGetSigningConfig handles GET /_/signing-config, returning
// {"requireSignatures": bool} at parity with the gRPC GetSigningConfig RPC.
// Like the signing-key list, it is a live read (linearizable unless the caller
// sends `X-Consistency: stale`).
func (s *Server) handleGetSigningConfig(w http.ResponseWriter, r *http.Request) {
	requireSignatures, err := s.backend.GetSigningConfig(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, r, &servicepb.GetSigningConfigResponse{RequireSignatures: requireSignatures})
}
