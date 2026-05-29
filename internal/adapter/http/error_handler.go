package http

import (
	"errors"
	"net/http"

	"github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

type httpErrorMapping struct {
	match      func(error) bool
	statusCode int
	errorCode  string
	// headerFn optionally sets response headers before writing the error body.
	headerFn func(http.ResponseWriter)
}

func matchAs[T error]() func(error) bool {
	return func(err error) bool {
		var target T

		return errors.As(err, &target)
	}
}

func matchIs(sentinel error) func(error) bool {
	return func(err error) bool {
		return errors.Is(err, sentinel)
	}
}

var httpErrorMappings = []httpErrorMapping{
	{matchIs(commonpb.ErrNoLeader), http.StatusServiceUnavailable, "NO_LEADER",
		func(w http.ResponseWriter) { w.Header().Set("Retry-After", "1") }},

	{matchAs[*commonpb.NotFoundError](), http.StatusNotFound, "NOT_FOUND", nil},
	{matchAs[*domain.ErrLedgerAlreadyExists](), http.StatusConflict, "CONFLICT", nil},
	{matchAs[*domain.ErrLedgerNotFound](), http.StatusNotFound, "NOT_FOUND", nil},
	{matchAs[*domain.ErrLedgerDeleted](), http.StatusConflict, "LEDGER_DELETED", nil},
	{matchAs[*domain.ErrLedgerInMirrorMode](), http.StatusConflict, "LEDGER_IN_MIRROR_MODE", nil},
	{matchAs[*domain.ErrLedgerNotInMirrorMode](), http.StatusBadRequest, "LEDGER_NOT_IN_MIRROR_MODE", nil},
	{matchAs[*domain.ErrTransactionReferenceConflict](), http.StatusConflict, "CONFLICT", nil},
	{matchAs[*domain.ErrIdempotencyKeyConflict](), http.StatusConflict, "CONFLICT", nil},
	{matchAs[*domain.ErrTransactionNotFound](), http.StatusNotFound, "NOT_FOUND", nil},
	{matchAs[*domain.ErrTransactionAlreadyReverted](), http.StatusConflict, "CONFLICT", nil},
	{matchAs[*domain.ErrInsufficientFunds](), http.StatusBadRequest, "INSUFFICIENT_FUNDS", nil},
	{matchAs[*domain.ErrBalanceNotFound](), http.StatusBadRequest, "BALANCE_NOT_FOUND", nil},
	{matchAs[*domain.ErrNumscriptParse](), http.StatusBadRequest, "SCRIPT_PARSE_ERROR", nil},
	{matchAs[*domain.ErrNumscriptNotFound](), http.StatusNotFound, "NOT_FOUND", nil},
	{matchAs[*domain.ErrNumscriptVersionAlreadyExists](), http.StatusConflict, "CONFLICT", nil},
	{matchAs[*domain.ErrNumscriptInvalidVersion](), http.StatusBadRequest, "VALIDATION", nil},
	{matchAs[*domain.ErrMetadataNotFound](), http.StatusNotFound, "NOT_FOUND", nil},
	{matchAs[*domain.ErrPreparedQueryAlreadyExists](), http.StatusConflict, "CONFLICT", nil},
	{matchAs[*domain.ErrPreparedQueryNotFound](), http.StatusNotFound, "NOT_FOUND", nil},
	{matchAs[*domain.ErrAccountNotMatchingType](), http.StatusBadRequest, "ACCOUNT_NOT_MATCHING_TYPE", nil},
	{matchAs[*domain.ErrAccountTypeNotFound](), http.StatusNotFound, "ACCOUNT_TYPE_NOT_FOUND", nil},
	{matchAs[*domain.ErrAccountTypeAlreadyExists](), http.StatusConflict, "ACCOUNT_TYPE_ALREADY_EXISTS", nil},
	{matchAs[*domain.ErrInvalidPattern](), http.StatusBadRequest, "INVALID_PATTERN", nil},
	{matchAs[*domain.ErrAccountTypeHasAccounts](), http.StatusConflict, "ACCOUNT_TYPE_HAS_ACCOUNTS", nil},
	// Validation sentinel errors
	{matchIs(domain.ErrNumscriptContentRequired), http.StatusBadRequest, "VALIDATION", nil},
	{matchIs(domain.ErrTargetRequired), http.StatusBadRequest, "VALIDATION", nil},
	{matchIs(domain.ErrMetadataKeyRequired), http.StatusBadRequest, "VALIDATION", nil},
	{matchIs(domain.ErrScriptRequired), http.StatusBadRequest, "VALIDATION", nil},
	{matchIs(admission.ErrIdempotencyKeyTooLong), http.StatusBadRequest, "VALIDATION", nil},
	{matchIs(admission.ErrIdempotencyKeyInvalidUTF8), http.StatusBadRequest, "VALIDATION", nil},
}

// handleError handles errors and returns appropriate HTTP responses.
func handleError(w http.ResponseWriter, r *http.Request, err error) {
	for _, m := range httpErrorMappings {
		if m.match(err) {
			if m.headerFn != nil {
				m.headerFn(w)
			}

			writeErrorResponse(w, m.statusCode, m.errorCode, err)

			return
		}
	}

	writeInternalServerError(w, r, err)
}
