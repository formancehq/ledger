package http

import (
	"net/http"
)

// chapterScheduleResponse mirrors servicepb.GetChapterScheduleResponse, whose
// single field is `cron` (protobuf-JSON). The HTTP body uses the same field
// name so REST and gRPC/protobuf-JSON clients see an identical shape.
type chapterScheduleResponse struct {
	Cron string `json:"cron"`
}

// handleGetChapterSchedule handles GET /chapter-schedule to fetch the
// cluster-wide chapter auto-rotation cron expression.
func (s *Server) handleGetChapterSchedule(w http.ResponseWriter, r *http.Request) {
	cron, err := s.backend.GetChapterSchedule(r.Context())
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, chapterScheduleResponse{Cron: cron})
}
