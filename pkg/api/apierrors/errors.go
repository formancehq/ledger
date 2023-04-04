package apierrors

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/ledger/state"
	"github.com/formancehq/ledger/pkg/machine/vm"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

const (
	ErrInternal                  = "INTERNAL"
	ErrConflict                  = "CONFLICT"
	ErrInsufficientFund          = "INSUFFICIENT_FUND"
	ErrValidation                = "VALIDATION"
	ErrContextCancelled          = "CONTEXT_CANCELLED"
	ErrStore                     = "STORE"
	ErrNotFound                  = "NOT_FOUND"
	ErrScriptCompilationFailed   = "COMPILATION_FAILED"
	ErrScriptNoScript            = "NO_SCRIPT"
	ErrScriptMetadataOverride    = "METADATA_OVERRIDE"
	ScriptErrorInsufficientFund  = "INSUFFICIENT_FUND"
	ScriptErrorCompilationFailed = "COMPILATION_FAILED"
	ScriptErrorNoScript          = "NO_SCRIPT"
	ScriptErrorMetadataOverride  = "METADATA_OVERRIDE"
	ResourceResolutionError      = "RESOURCE_RESOLUTION_ERROR"
)

func ResponseError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, details := coreErrorToErrorCode(err)

	baseError := errors.Cause(err)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if status < 500 {
		err := json.NewEncoder(w).Encode(api.ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: baseError.Error(),
			Details:      details,
		})
		if err != nil {
			panic(err)
		}
	} else {
		logging.FromContext(r.Context()).Errorf("internal server error: %s", err)
	}
}

func coreErrorToErrorCode(err error) (int, string, string) {
	switch {
	case state.IsConflictError(err):
		return http.StatusConflict, ErrConflict, ""
	case
		ledger.IsValidationError(err),
		state.IsPastTransactionError(err),
		runner.IsNoPostingsError(err):
		return http.StatusBadRequest, ErrValidation, ""
	case storage.IsNotFoundError(err):
		return http.StatusNotFound, ErrNotFound, ""
	case runner.IsNoScriptError(err):
		baseError := errors.Cause(err)
		return http.StatusBadRequest, ScriptErrorNoScript, EncodeLink(baseError.Error())
	case vm.IsInsufficientFundError(err):
		baseError := errors.Cause(err)
		return http.StatusBadRequest, ScriptErrorInsufficientFund, EncodeLink(baseError.Error())
	case runner.IsCompilationFailedError(err):
		baseError := errors.Cause(err)
		return http.StatusBadRequest, ScriptErrorCompilationFailed, EncodeLink(baseError.Error())
	case vm.IsMetadataOverrideError(err):
		baseError := errors.Cause(err)
		return http.StatusBadRequest, ScriptErrorMetadataOverride, EncodeLink(baseError.Error())
	case vm.IsResourceResolutionInvalidTypeFromExtSourcesError(err),
		vm.IsResourceResolutionMissingMetadataError(err):
		baseError := errors.Cause(err)
		return http.StatusBadRequest, ResourceResolutionError, EncodeLink(baseError.Error())
	case errors.Is(err, context.Canceled):
		return http.StatusInternalServerError, ErrContextCancelled, ""
	case storage.IsStorageError(err):
		return http.StatusServiceUnavailable, ErrStore, ""
	default:
		return http.StatusInternalServerError, ErrInternal, ""
	}
}

func EncodeLink(errStr string) string {
	if errStr == "" {
		return ""
	}

	errStr = strings.ReplaceAll(errStr, "\n", "\r\n")
	payload, err := json.Marshal(map[string]string{
		"error": errStr,
	})
	if err != nil {
		panic(err)
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	return fmt.Sprintf("https://play.numscript.org/?payload=%v", payloadB64)
}
