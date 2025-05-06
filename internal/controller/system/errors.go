package system

import (
	"errors"
	"fmt"
)

var (
	ErrLedgerAlreadyExists          = errors.New("ledger already exists")
	ErrBucketOutdated               = errors.New("bucket is outdated, you need to upgrade it before adding a new ledger")
	ErrExperimentalFeaturesDisabled = errors.New("experimental features are disabled")
	ErrLedgerNotFound               = errors.New("ledger not found")
	ErrBucketMarkedForDeletion      = errors.New("bucket is marked for deletion")
)

type ErrInvalidLedgerConfiguration struct {
	err error
}

func (e ErrInvalidLedgerConfiguration) Error() string {
	return fmt.Sprintf("invalid ledger configuration: %s", e.err)
}

func (e ErrInvalidLedgerConfiguration) Is(err error) bool {
	_, ok := err.(ErrInvalidLedgerConfiguration)
	return ok
}

func newErrInvalidLedgerConfiguration(err error) ErrInvalidLedgerConfiguration {
	return ErrInvalidLedgerConfiguration{
		err: err,
	}
}
