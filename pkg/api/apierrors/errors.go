package apierrors

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
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

func ResponseError(c *gin.Context, err error) {
	_ = c.Error(err)
	status, code, details := coreErrorToErrorCode(c, err)

	if status < 500 {
		c.AbortWithStatusJSON(status,
			api.ErrorResponse{
				ErrorCode:    code,
				ErrorMessage: err.Error(),
				Details:      details,

				ErrorCodeDeprecated:    code,
				ErrorMessageDeprecated: err.Error(),
			})
	} else {
		c.AbortWithStatus(status)
	}
}

func coreErrorToErrorCode(c *gin.Context, err error) (int, string, string) {
	switch {
	case ledger.IsConflictError(err):
		return http.StatusConflict, ErrConflict, ""
	case ledger.IsInsufficientFundError(err):
		return http.StatusBadRequest, ErrInsufficientFund, ""
	case ledger.IsValidationError(err):
		return http.StatusBadRequest, ErrValidation, ""
	case ledger.IsNotFoundError(err):
		return http.StatusNotFound, ErrNotFound, ""
	case ledger.IsScriptErrorWithCode(err, ErrScriptNoScript),
		ledger.IsScriptErrorWithCode(err, ErrInsufficientFund),
		ledger.IsScriptErrorWithCode(err, ErrScriptCompilationFailed),
		ledger.IsScriptErrorWithCode(err, ErrScriptMetadataOverride):
		scriptErr := err.(*ledger.ScriptError)
		return http.StatusBadRequest, scriptErr.Code, EncodeLink(scriptErr.Message)
	case errors.Is(err, context.Canceled):
		return http.StatusInternalServerError, ErrContextCancelled, ""
	case storage.IsError(err):
		return http.StatusServiceUnavailable, ErrStore, ""
	default:
		logging.GetLogger(c.Request.Context()).Errorf(
			"unknown API response error: %s", err)
		return http.StatusInternalServerError, ErrInternal, ""
	}
}

func EncodeLink(errStr string) string {
	if errStr == "" {
		return ""
	}

	errStr = strings.ReplaceAll(errStr, "\n", "\r\n")
	payload, err := json.Marshal(gin.H{
		"error": errStr,
	})
	if err != nil {
		panic(err)
	}
	payloadB64 := base64.StdEncoding.EncodeToString(payload)
	return fmt.Sprintf("https://play.numscript.org/?payload=%v", payloadB64)
}
