package http

import (
	"net/http"
)

// handleListChapters handles GET /chapters to list bucket chapters.
//
// Live, best-effort read: it drains the full cursor and does not expose the
// gRPC read-consistency options (checkpointId / minLogSequence) or a
// bidirectional cursor. Clients needing consistency-bounded reads use gRPC.
// Tracked follow-up (same carve-out as ListTransactions / the audit reads).
func (s *Server) handleListChapters(w http.ResponseWriter, r *http.Request) {
	cursor, err := s.backend.ListChapters(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	chapters, ok := drainCursor(w, r, cursor)
	if !ok {
		return
	}

	writeProtoListOK(w, chapters)
}
