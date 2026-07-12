package http

import (
	"net/http"
)

// handleListChapters handles GET /chapters to list bucket chapters.
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
