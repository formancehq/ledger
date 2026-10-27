package http

import (
	"io"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/application/numscriptanalysis"
)

// handleAnalyzeNumscript accepts source directly; it does not require a ledger.
func (s *Server) handleAnalyzeNumscript(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Source  string `json:"source"`
		Version string `json:"version"`
	}
	// Consume the entire request through MaxBytesReader. A streaming decoder
	// could accept the first JSON value and ignore a trailing value or an
	// oversized suffix.
	encoded, err := io.ReadAll(r.Body)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	if err := json.Unmarshal(encoded, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}
	result, err := numscriptanalysis.Analyze(body.Source, body.Version)
	if err != nil {
		handleError(w, r, err)

		return
	}
	writeOK(w, r, result)
}
