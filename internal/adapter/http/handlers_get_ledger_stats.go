package http

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// ledgerStatsJSON is the camelCase JSON DTO for LedgerStats.
type ledgerStatsJSON struct {
	AccountCount     uint64 `json:"accountCount"`
	TransactionCount uint64 `json:"transactionCount"`
}

func toLedgerStatsJSON(stats *commonpb.LedgerStats) *ledgerStatsJSON {
	return &ledgerStatsJSON{
		AccountCount:     stats.GetAccountCount(),
		TransactionCount: stats.GetTransactionCount(),
	}
}

// handleGetLedgerStats handles GET /{ledgerName}/stats to retrieve ledger statistics.
func (s *Server) handleGetLedgerStats(w http.ResponseWriter, r *http.Request) {
	ledgerName := chi.URLParam(r, "ledgerName")
	if ledgerName == "" {
		writeBadRequest(w, "INVALID_REQUEST", errors.New("ledger name is required"))

		return
	}

	stats, err := s.backend.GetLedgerStats(r.Context(), ledgerName)
	if err != nil {
		handleError(w, r, err)

		return
	}

	writeOK(w, toLedgerStatsJSON(stats))
}
