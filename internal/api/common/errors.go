package common

import (
	"encoding/json"
	"errors"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/ledger"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/platform/postgres"
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

func HandleCommonErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, postgres.ErrTooManyClient{}):
		// Set header and status immediately to prevent status=0 if panic occurs
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		// Write the error response body manually since WriteHeader was already called
		json.NewEncoder(w).Encode(api.ErrorResponse{
			ErrorCode:    api.ErrorInternal,
			ErrorMessage: err.Error(),
		})
	default:
		InternalServerError(w, r, err)
	}
}

func HandleCommonWriteErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ledgercontroller.ErrIdempotencyKeyConflict{}):
		api.WriteErrorResponse(w, http.StatusConflict, ErrConflict, err)
	case errors.Is(err, ledgercontroller.ErrInvalidIdempotencyInput{}):
		api.BadRequest(w, ErrValidation, err)
	case errors.Is(err, ledgercontroller.ErrNotFound):
		api.NotFound(w, err)
	default:
		HandleCommonErrors(w, r, err)
	}
}

func HandleCommonPaginationErrors(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, storagecommon.ErrInvalidQuery{}) ||
		errors.Is(err, ledger.ErrMissingFeature{}) ||
		errors.Is(err, storagecommon.ErrNotPaginatedField{}):
		api.BadRequest(w, ErrValidation, err)
	default:
		HandleCommonErrors(w, r, err)
	}
}

func InternalServerError(w http.ResponseWriter, r *http.Request, err error) {
	// Set header and status immediately to prevent status=0 in logs if a panic occurs
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusInternalServerError)
	
	otlp.RecordError(r.Context(), err)
	logging.FromContext(r.Context()).Error(err)
	
	// Write the error response body manually since WriteHeader was already called
	//nolint:staticcheck
	json.NewEncoder(w).Encode(api.ErrorResponse{
		ErrorCode:    api.ErrorInternal,
		ErrorMessage: errors.New("Internal error. Consult logs/traces to have more details.").Error(),
	})
}
