package system

import (
	"errors"
	"fmt"
	"github.com/formancehq/ledger/internal/storage/driver"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

var (
	ErrLedgerAlreadyExists          = systemstore.ErrLedgerAlreadyExists
	ErrBucketOutdated               = driver.ErrBucketOutdated
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

// ErrConnectorNotFound denotes an attempt to use a not found connector
type ErrConnectorNotFound string

func (e ErrConnectorNotFound) Error() string {
	return fmt.Sprintf("connector '%s' not found", string(e))
}

func (e ErrConnectorNotFound) Is(err error) bool {
	_, ok := err.(ErrConnectorNotFound)
	return ok
}

func NewErrConnectorNotFound(connectorID string) ErrConnectorNotFound {
	return ErrConnectorNotFound(connectorID)
}

type ErrInvalidDriverConfiguration struct {
	name string
	err  error
}

func (e ErrInvalidDriverConfiguration) Error() string {
	return fmt.Sprintf("driver '%s' invalid: %s", e.name, e.err)
}

func (e ErrInvalidDriverConfiguration) Is(err error) bool {
	_, ok := err.(ErrInvalidDriverConfiguration)
	return ok
}

func (e ErrInvalidDriverConfiguration) Unwrap() error {
	return e.err
}

func NewErrInvalidDriverConfiguration(name string, err error) ErrInvalidDriverConfiguration {
	return ErrInvalidDriverConfiguration{
		name: name,
		err:  err,
	}
}

type ErrConnectorUsed string

func (e ErrConnectorUsed) Error() string {
	return fmt.Sprintf("connector '%s' actually used by an existing pipeline", string(e))
}

func (e ErrConnectorUsed) Is(err error) bool {
	_, ok := err.(ErrConnectorUsed)
	return ok
}

func NewErrConnectorUsed(id string) ErrConnectorUsed {
	return ErrConnectorUsed(id)
}