package http

import (
	"net/http"
)

type chapterScheduleResponse struct {
	Schedule string `json:"schedule"`
}

// handleGetChapterSchedule handles GET /chapter-schedule to fetch the
// cluster-wide chapter auto-rotation cron expression.
func (s *Server) handleGetChapterSchedule(w http.ResponseWriter, r *http.Request) {
	schedule, err := s.backend.GetChapterSchedule(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, chapterScheduleResponse{Schedule: schedule})
}
