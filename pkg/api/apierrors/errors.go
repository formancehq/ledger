package apierrors

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/ledger/pkg/ledger/runner"
	"github.com/formancehq/ledger/pkg/machine"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"
)

const (
	ErrInternal                = "INTERNAL"
	ErrConflict                = "CONFLICT"
	ErrInsufficientFund        = "INSUFFICIENT_FUND"
	ErrValidation              = "VALIDATION"
	ErrContextCancelled        = "CONTEXT_CANCELLED"
	ErrStore                   = "STORE"
	ErrNotFound                = "NOT_FOUND"
	ErrScriptCompilationFailed = "COMPILATION_FAILED"
	ErrScriptNoScript          = "NO_SCRIPT"
	ErrScriptMetadataOverride  = "METADATA_OVERRIDE"
)

func ResponseError(w http.ResponseWriter, r *http.Request, err error) {
	status, code, details := coreErrorToErrorCode(err)

	w.WriteHeader(status)
	if status < 500 {
		err := json.NewEncoder(w).Encode(api.ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: err.Error(),
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
	case runner.IsConflictError(err):
		return http.StatusConflict, ErrConflict, ""
	case runner.IsInsufficientFundError(err):
		return http.StatusBadRequest, ErrInsufficientFund, ""
	case runner.IsValidationError(err):
		return http.StatusBadRequest, ErrValidation, ""
	case runner.IsNotFoundError(err):
		return http.StatusNotFound, ErrNotFound, ""
	case machine.IsScriptErrorWithCode(err, ErrScriptNoScript),
		machine.IsScriptErrorWithCode(err, ErrInsufficientFund),
		machine.IsScriptErrorWithCode(err, ErrScriptCompilationFailed),
		machine.IsScriptErrorWithCode(err, ErrScriptMetadataOverride):
		scriptErr := &machine.ScriptError{}
		_ = errors.As(err, &scriptErr)
		return http.StatusBadRequest, scriptErr.Code, EncodeLink(scriptErr.Message)
	case errors.Is(err, context.Canceled):
		return http.StatusInternalServerError, ErrContextCancelled, ""
	case storage.IsError(err):
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
