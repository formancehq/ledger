package noop

import (
	"context"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

type Connector struct{}

func (connector *Connector) Stop(_ context.Context) error {
	return nil
}

func (connector *Connector) Start(_ context.Context) error {
	return nil
}

func (connector *Connector) ClearData(_ context.Context, _ string) error {
	return nil
}

func (connector *Connector) Accept(_ context.Context, logs ...drivers.LogWithLedger) ([]error, error) {
	return make([]error, len(logs)), nil
}

func NewConnector(_ struct{}, _ logging.Logger) (*Connector, error) {
	return &Connector{}, nil
}

var _ drivers.Driver = (*Connector)(nil)
