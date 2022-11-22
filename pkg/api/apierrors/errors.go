package apierrors

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/sharedlogging"
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

	errorCodeKey = "_errorCode"
)

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
		sharedlogging.GetLogger(c.Request.Context()).Errorf("internal errors: %s", err)
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

func ErrorCode(c *gin.Context) string {
	return c.GetString(errorCodeKey)
}

// TODO: update sharedapi.ErrorResponse with new details field
type ErrorResponse struct {
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
	Details      string `json:"details,omitempty"`
}

func ResponseError(c *gin.Context, err error) {
	_ = c.Error(err)
	status, code, details := coreErrorToErrorCode(c, err)
	c.Set(errorCodeKey, code)

	if status < 500 {
		c.AbortWithStatusJSON(status,
			ErrorResponse{
				ErrorCode:    code,
				ErrorMessage: err.Error(),
				Details:      details,
			})
	} else {
		c.AbortWithStatus(status)
	}
}
