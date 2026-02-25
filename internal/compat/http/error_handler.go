package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
)

// handleError handles errors and returns appropriate HTTP responses.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		notFoundErr    *commonpb.NotFoundError
		refConflict    *domain.ErrTransactionReferenceConflict
		ikConflict     *domain.ErrIdempotencyKeyConflict
		ledgerExists   *domain.ErrLedgerAlreadyExists
		ledgerNotFound *domain.ErrLedgerNotFound
		txNotFound     *domain.ErrTransactionNotFound
		txReverted     *domain.ErrTransactionAlreadyReverted
		insufficient   *domain.ErrInsufficientFunds
		balNotFound    *domain.ErrBalanceNotFound
		parseErr       *numscript.ErrNumscriptParse
		metaNotFound   *domain.ErrMetadataNotFound
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

	case errors.Is(err, domain.ErrTargetRequired),
		errors.Is(err, domain.ErrMetadataKeyRequired),
		errors.Is(err, numscript.ErrScriptRequired),
		errors.Is(err, admission.ErrIdempotencyKeyTooLong):
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

	default:
		writeInternalServerError(w, r, err)
	}
}
