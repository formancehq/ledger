package noop

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

type Driver struct{}

func (driver *Driver) Stop(_ context.Context) error {
	return nil
}

func (driver *Driver) Start(_ context.Context) error {
	return nil
}

func (driver *Driver) ClearData(_ context.Context, _ string) error {
	return nil
}

func (driver *Driver) Accept(_ context.Context, logs ...drivers.LogWithLedger) ([]error, error) {
	return make([]error, len(logs)), nil
}

func NewDriver(_ struct{}, _ logging.Logger) (*Driver, error) {
	return &Driver{}, nil
}

var _ drivers.Driver = (*Driver)(nil)
