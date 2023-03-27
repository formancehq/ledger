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

const (
	ResourceResolutionErrorCodeMissingMetadata                = "MISSING_METADATA"
	ResourceResolutionErrorCodeInvalidTypeFromExternalSources = "INVALID_TYPE_FROM_EXTERNAL_SOURCE"
)

type ResourceResolutionError struct {
	Code string
	text string
}

func (e ResourceResolutionError) Error() string {
	return fmt.Sprintf("Error resolving resource: %s", e.text)
}

func (e ResourceResolutionError) Is(err error) bool {
	eerr, ok := err.(ResourceResolutionError)
	if !ok {
		return false
	}
	return e.Code == eerr.Code
}

func newResourceResolutionError(code, text string) ResourceResolutionError {
	return ResourceResolutionError{
		Code: code,
		text: text,
	}
}

func IsResourceResolutionErrorWithCode(err error, code string) bool {
	return errors.Is(err, &ResourceResolutionError{
		Code: code,
	})
}

func IsResourceResolutionError(err error) bool {
	_, ok := err.(ResourceResolutionError)
	return ok
}
