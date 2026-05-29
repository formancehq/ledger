package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/adapter/json"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// handleSaveNumscript handles PUT /{ledgerName}/numscripts/{name} to save a numscript.
func (s *Server) handleSaveNumscript(w http.ResponseWriter, r *http.Request) {
	ledgerName, ok := requireLedgerName(w, r)
	if !ok {
		return
	}

	name := chi.URLParam(r, "name")
	if name == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("numscript name is required"))

		return
	}

	var body struct {
		Content string `json:"content"`
		Version string `json:"version"`
	}
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		writeBadRequest(w, "INVALID_REQUEST", err)

		return
	}

	logs, err := s.backend.Apply(r.Context(), &servicepb.Request{
		Type: &servicepb.Request_SaveNumscript{
			SaveNumscript: &servicepb.SaveNumscriptRequest{
				Ledger:  ledgerName,
				Name:    name,
				Content: body.Content,
				Version: body.Version,
			},
		},
	})
	if err != nil {
		handleError(w, r, err)

		return
	}

	if len(logs) > 0 {
		if saved := logs[0].GetPayload().GetSavedNumscript(); saved != nil {
			writeCreated(w, saved.GetInfo())

			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}
