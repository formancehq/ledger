package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
)

// handleError handles errors and returns appropriate HTTP responses.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		notFoundErr    *commonpb.NotFoundError
		refConflict    *processing.ErrTransactionReferenceConflict
		ikConflict     *processing.ErrIdempotencyKeyConflict
		ledgerExists   *processing.ErrLedgerAlreadyExists
		ledgerNotFound *processing.ErrLedgerNotFound
		txNotFound     *processing.ErrTransactionNotFound
		txReverted     *processing.ErrTransactionAlreadyReverted
		insufficient   *processing.ErrInsufficientFunds
		balNotFound    *processing.ErrBalanceNotFound
		parseErr       *processing.ErrNumscriptParse
		metaNotFound   *processing.ErrMetadataNotFound
	)

	switch {
	case errors.Is(err, commonpb.ErrNoLeader):
		w.Header().Set("Retry-After", "1")
		writeErrorResponse(w, http.StatusServiceUnavailable, "NO_LEADER", err)

	case errors.As(err, &notFoundErr):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.As(err, &ledgerExists):
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)

	case errors.As(err, &ledgerNotFound):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.As(err, &refConflict):
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)

	case errors.As(err, &ikConflict):
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)

	case errors.As(err, &txNotFound):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.As(err, &txReverted):
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)

	case errors.As(err, &insufficient):
		writeErrorResponse(w, http.StatusBadRequest, "INSUFFICIENT_FUNDS", err)

	case errors.As(err, &balNotFound):
		writeErrorResponse(w, http.StatusBadRequest, "BALANCE_NOT_FOUND", err)

	case errors.As(err, &parseErr):
		writeErrorResponse(w, http.StatusBadRequest, "SCRIPT_PARSE_ERROR", err)

	case errors.As(err, &metaNotFound):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.Is(err, processing.ErrTargetRequired),
		errors.Is(err, processing.ErrMetadataKeyRequired),
		errors.Is(err, processing.ErrScriptRequired):
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

	default:
		writeInternalServerError(w, r, err)
	}
}
