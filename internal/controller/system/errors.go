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

// ErrExporterNotFound denotes an attempt to use a not found exporter
type ErrExporterNotFound string

func (e ErrExporterNotFound) Error() string {
	return fmt.Sprintf("exporter '%s' not found", string(e))
}

func (e ErrExporterNotFound) Is(err error) bool {
	_, ok := err.(ErrExporterNotFound)
	return ok
}

func NewErrExporterNotFound(exporterID string) ErrExporterNotFound {
	return ErrExporterNotFound(exporterID)
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

type ErrExporterUsed string

func (e ErrExporterUsed) Error() string {
	return fmt.Sprintf("exporter '%s' actually used by an existing pipeline", string(e))
}

func (e ErrExporterUsed) Is(err error) bool {
	_, ok := err.(ErrExporterUsed)
	return ok
}

func NewErrExporterUsed(id string) ErrExporterUsed {
	return ErrExporterUsed(id)
}
