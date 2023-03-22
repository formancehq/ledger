package vm

import (
	"fmt"

	"github.com/pkg/errors"
)

const (
	ScriptErrorInsufficientFund  = "INSUFFICIENT_FUND"
	ScriptErrorCompilationFailed = "COMPILATION_FAILED"
	ScriptErrorNoScript          = "NO_SCRIPT"
	ScriptErrorMetadataOverride  = "METADATA_OVERRIDE"
)

type ScriptError struct {
	Code    string
	Message string
}

func (e ScriptError) Error() string {
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

func (e ScriptError) Is(err error) bool {
	eerr, ok := err.(*ScriptError)
	if !ok {
		return false
	}
	return e.Code == eerr.Code
}

func IsScriptErrorWithCode(err error, code string) bool {
	return errors.Is(err, &ScriptError{
		Code: code,
	})
}

func NewScriptError(code string, message string) *ScriptError {
	return &ScriptError{
		Code:    code,
		Message: message,
	}
}
