package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger-v3-poc/internal/application/admission"
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// handleError handles errors and returns appropriate HTTP responses.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	var (
		notFoundErr        *commonpb.NotFoundError
		refConflict        *domain.ErrTransactionReferenceConflict
		ikConflict         *domain.ErrIdempotencyKeyConflict
		ledgerExists       *domain.ErrLedgerAlreadyExists
		ledgerNotFound     *domain.ErrLedgerNotFound
		ledgerDeleted      *domain.ErrLedgerDeleted
		ledgerInMirror     *domain.ErrLedgerInMirrorMode
		ledgerNotInMirror  *domain.ErrLedgerNotInMirrorMode
		txNotFound         *domain.ErrTransactionNotFound
		txReverted         *domain.ErrTransactionAlreadyReverted
		insufficient       *domain.ErrInsufficientFunds
		balNotFound        *domain.ErrBalanceNotFound
		parseErr           *domain.ErrNumscriptParse
		nsNotFound         *domain.ErrNumscriptNotFound
		nsVersionExists    *domain.ErrNumscriptVersionAlreadyExists
		nsInvalidVersion   *domain.ErrNumscriptInvalidVersion
		metaNotFound       *domain.ErrMetadataNotFound
		pqExists           *domain.ErrPreparedQueryAlreadyExists
		pqNotFound         *domain.ErrPreparedQueryNotFound
		acctNotMatching    *domain.ErrAccountNotMatchingType
		acctTypeNotFound   *domain.ErrAccountTypeNotFound
		acctTypeExists     *domain.ErrAccountTypeAlreadyExists
		invalidPattern     *domain.ErrInvalidPattern
		acctTypeHasAccts   *domain.ErrAccountTypeHasAccounts
		migrationInProg    *domain.ErrAccountTypeMigrationInProgress
		migrationNotCompat *domain.ErrAccountTypeMigrationNotCompatible
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

	case errors.As(err, &ledgerDeleted):
		writeErrorResponse(w, http.StatusConflict, "LEDGER_DELETED", err)

	case errors.As(err, &ledgerInMirror):
		writeErrorResponse(w, http.StatusConflict, "LEDGER_IN_MIRROR_MODE", err)

	case errors.As(err, &ledgerNotInMirror):
		writeErrorResponse(w, http.StatusBadRequest, "LEDGER_NOT_IN_MIRROR_MODE", err)

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

	case errors.As(err, &nsNotFound):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.As(err, &nsVersionExists):
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)

	case errors.Is(err, domain.ErrNumscriptContentRequired):
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

	case errors.As(err, &nsInvalidVersion):
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

	case errors.As(err, &metaNotFound):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.As(err, &pqExists):
		writeErrorResponse(w, http.StatusConflict, "CONFLICT", err)

	case errors.As(err, &pqNotFound):
		writeErrorResponse(w, http.StatusNotFound, "NOT_FOUND", err)

	case errors.As(err, &acctNotMatching):
		writeErrorResponse(w, http.StatusBadRequest, "ACCOUNT_NOT_MATCHING_TYPE", err)

	case errors.As(err, &acctTypeNotFound):
		writeErrorResponse(w, http.StatusNotFound, "ACCOUNT_TYPE_NOT_FOUND", err)

	case errors.As(err, &acctTypeExists):
		writeErrorResponse(w, http.StatusConflict, "ACCOUNT_TYPE_ALREADY_EXISTS", err)

	case errors.As(err, &invalidPattern):
		writeErrorResponse(w, http.StatusBadRequest, "INVALID_PATTERN", err)

	case errors.As(err, &acctTypeHasAccts):
		writeErrorResponse(w, http.StatusConflict, "ACCOUNT_TYPE_HAS_ACCOUNTS", err)

	case errors.As(err, &migrationInProg):
		writeErrorResponse(w, http.StatusConflict, "ACCOUNT_TYPE_MIGRATION_IN_PROGRESS", err)

	case errors.As(err, &migrationNotCompat):
		writeErrorResponse(w, http.StatusBadRequest, "ACCOUNT_TYPE_MIGRATION_NOT_COMPATIBLE", err)

	case errors.Is(err, domain.ErrTargetRequired),
		errors.Is(err, domain.ErrMetadataKeyRequired),
		errors.Is(err, domain.ErrScriptRequired),
		errors.Is(err, admission.ErrIdempotencyKeyTooLong),
		errors.Is(err, admission.ErrIdempotencyKeyInvalidUTF8):
		writeErrorResponse(w, http.StatusBadRequest, "VALIDATION", err)

	default:
		writeInternalServerError(w, r, err)
	}
}
