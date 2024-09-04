package machine

import (
	"fmt"

	"github.com/pkg/errors"
)

var (
	ErrScriptFailed            = errors.New("script exited with error code")
	ErrResourcesNotInitialized = errors.New("resources not initialized")
	ErrBalancesNotInitialized  = errors.New("balances not initialized")
	ErrResourceNotFound        = errors.New("resource not found")
)

type ErrInvalidScript struct {
	msg string
}

func (e *ErrInvalidScript) Error() string {
	return e.msg
}

func (e *ErrInvalidScript) Is(err error) bool {
	_, ok := err.(*ErrInvalidScript)
	return ok
}

func NewErrInvalidScript(f string, args ...any) *ErrInvalidScript {
	return &ErrInvalidScript{
		msg: fmt.Sprintf(f, args...),
	}
}

type ErrInsufficientFund struct {
	msg string
}

func (e *ErrInsufficientFund) Error() string {
	return e.msg
}

func (e *ErrInsufficientFund) Is(err error) bool {
	_, ok := err.(*ErrInsufficientFund)
	return ok
}

func NewErrInsufficientFund(f string, args ...any) *ErrInsufficientFund {
	return &ErrInsufficientFund{
		msg: fmt.Sprintf(f, args...),
	}
}

func IsInsufficientFundError(err error) bool {
	return errors.Is(err, &ErrInsufficientFund{})
}

type ErrNegativeAmount struct {
	msg string
}

func (e *ErrNegativeAmount) Error() string {
	return e.msg
}

func (e *ErrNegativeAmount) Is(err error) bool {
	_, ok := err.(*ErrNegativeAmount)
	return ok
}

func NewErrNegativeAmount(f string, args ...any) *ErrNegativeAmount {
	return &ErrNegativeAmount{
		msg: fmt.Sprintf(f, args...),
	}
}

type ErrMissingMetadata struct {
	msg string
}

func (e *ErrMissingMetadata) Error() string {
	return e.msg
}

func (e *ErrMissingMetadata) Is(err error) bool {
	_, ok := err.(*ErrMissingMetadata)
	return ok
}

func NewErrMissingMetadata(f string, args ...any) *ErrMissingMetadata {
	return &ErrMissingMetadata{
		msg: fmt.Sprintf(f, args...),
	}
}

type ErrInvalidVars struct {
	msg string
}

func (e *ErrInvalidVars) Error() string {
	return e.msg
}

func (e *ErrInvalidVars) Is(err error) bool {
	_, ok := err.(*ErrInvalidVars)
	return ok
}

func NewErrInvalidVars(f string, args ...any) *ErrInvalidVars {
	return &ErrInvalidVars{
		msg: fmt.Sprintf(f, args...),
	}
}

type ErrMetadataOverride struct {
	key string
}

func (e *ErrMetadataOverride) Key() string {
	return e.key
}

func (e *ErrMetadataOverride) Error() string {
	return fmt.Sprintf("cannot override metadata '%s'", e.key)
}

func (e *ErrMetadataOverride) Is(err error) bool {
	_, ok := err.(*ErrMetadataOverride)
	return ok
}

func NewErrMetadataOverride(key string) *ErrMetadataOverride {
	return &ErrMetadataOverride{
		key: key,
	}
}

func IsMetadataOverride(err error) bool {
	return errors.Is(err, &ErrMetadataOverride{})
}
