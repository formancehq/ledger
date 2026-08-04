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

	logs, err := s.applyUnsigned(r.Context(), r.Header.Get("Idempotency-Key"), &servicepb.Request{
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

	details := map[string]any{"ledger": ledgerName, "name": name}

	logEntry := exactlyOneLog("save-numscript", logs, details)

	saved := logEntry.GetPayload().GetSavedNumscript()
	if saved == nil {
		panic(unexpectedLogPayload("save-numscript", logEntry, details))
	}

	if saved.GetInfo() == nil {
		panic(emptyLogPayload("save-numscript", logEntry, details))
	}

	writeCreated(w, saved.GetInfo())
}
