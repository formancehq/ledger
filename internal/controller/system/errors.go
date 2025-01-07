package system

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/internal/replication/controller"
)

var (
	ErrLedgerAlreadyExists          = errors.New("ledger already exists")
	ErrBucketOutdated               = errors.New("bucket is outdated, you need to upgrade it before adding a new ledger")
	ErrExperimentalFeaturesDisabled = errors.New("experimental features are disabled")
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

type ErrConnectorNotFound = controller.ErrConnectorNotFound
type ErrInvalidConnectorConfiguration = controller.ErrInvalidDriverConfiguration
type ErrConnectorUsed = controller.ErrConnectorUsed