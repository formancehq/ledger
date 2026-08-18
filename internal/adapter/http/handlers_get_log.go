package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// handleGetLog handles GET /logs/{sequence} to fetch a single system log by
// its bucket-wide sequence number.
func (s *Server) handleGetLog(w http.ResponseWriter, r *http.Request) {
	seqRaw := chi.URLParam(r, "sequence")

	sequence, err := strconv.ParseUint(seqRaw, 10, 64)
	if err != nil {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("invalid sequence parameter"))

		return
	}

	log, err := s.backend.GetLog(r.Context(), sequence)
	if err != nil {
		handleError(w, r, err)

		return
	}

	// EN-1779: the opt-in header changes the amount format only, so both branches
	// keep writeOKChecked — moving one to a ConfigStd writer would also change the
	// HTML escaping and add a trailing newline. This route is deliberately not
	// unified with the logs-list route, which has always used ConfigStd.
	var logValue any = log
	if wantsBigintAsString(r) {
		logValue = commonpb.StringAmountLog{Log: log}
	}

	writeOKChecked(w, r, logValue)
}
