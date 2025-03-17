package common

import (
	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"go.opentelemetry.io/otel/trace"
	"net/http"
)

const (
	ErrConflict            = "CONFLICT"
	ErrInsufficientFund    = "INSUFFICIENT_FUND"
	ErrValidation          = "VALIDATION"
	ErrAlreadyRevert       = "ALREADY_REVERT"
	ErrNoPostings          = "NO_POSTINGS"
	ErrCompilationFailed   = "COMPILATION_FAILED"
	ErrMetadataOverride    = "METADATA_OVERRIDE"
	ErrBulkSizeExceeded    = "BULK_SIZE_EXCEEDED"
	ErrLedgerAlreadyExists = "LEDGER_ALREADY_EXISTS"

	ErrInterpreterParse   = "INTERPRETER_PARSE"
	ErrInterpreterRuntime = "INTERPRETER_RUNTIME"

	// v1 only
	ErrScriptCompilationFailed = "COMPILATION_FAILED"
	ErrScriptMetadataOverride  = "METADATA_OVERRIDE"
)

func HandleCommonErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrTooManyClient{}):
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
	default:
		InternalServerError(w, r, err)
	}
}

func InternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	span := trace.SpanFromContext(r.Context())
	if span != nil {
		span.RecordError(err)
	}
	logging.FromContext(r.Context()).Error(err)
	api.WriteErrorResponse(w, http.StatusInternalServerError, api.ErrorInternal, errors.New("Internal error. Consult logs/traces to have more details."))
}
