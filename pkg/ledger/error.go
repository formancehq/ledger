package ledger

import (
	"fmt"
	"github.com/pkg/errors"
)

type CommitError struct {
	TXIndex int   `json:"index"`
	Err     error `json:"error"`
}

func (e CommitError) Error() string {
	return errors.Wrapf(e.Err, "processing tx %d", e.TXIndex).Error()
}

func NewCommitError(txIndex int, err error) *CommitError {
	return &CommitError{
		TXIndex: txIndex,
		Err:     err,
	}
}

type InsufficientFundError struct {
	Asset string
}

func (e InsufficientFundError) Error() string {
	return fmt.Sprintf("balance.insufficient.%s", e.Asset)
}

func NewInsufficientFundError(asset string) *InsufficientFundError {
	return &InsufficientFundError{
		Asset: asset,
	}
}

type ValidationError struct {
	Msg string
}

func (v ValidationError) Error() string {
	return v.Msg
}

func NewValidationError(msg string) *ValidationError {
	return &ValidationError{
		Msg: msg,
	}
}
