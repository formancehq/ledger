package common

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
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

// HandleCommonWriteErrors gère spécifiquement les erreurs liées aux opérations d'écriture
// telles que les erreurs d'idempotence
func HandleCommonWriteErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ledgercontroller.ErrIdempotencyKeyConflict{}):
		api.WriteErrorResponse(w, http.StatusConflict, ErrConflict, err)
	case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}):
		api.BadRequest(w, ErrValidation, err)
	case errors.Is(err, ledgercontroller.ErrTransactionReferenceConflict{}):
		api.WriteErrorResponse(w, http.StatusConflict, ErrConflict, err)
	case errors.Is(err, ledgercontroller.ErrNotFound):
		api.NotFound(w, err)
	default:
		HandleCommonErrors(w, r, err)
	}
}

// HandleCommonErrors gère les erreurs communes plus générales
func HandleCommonErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrTooManyClient{}):
		api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
	default:
		InternalServerError(w, r, err)
	}
}

func InternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	otlp.RecordError(r.Context(), err)
	logging.FromContext(r.Context()).Error(err)
	api.WriteErrorResponse(w, http.StatusInternalServerError, api.ErrorInternal, errors.New("Internal error. Consult logs/traces to have more details."))
}
