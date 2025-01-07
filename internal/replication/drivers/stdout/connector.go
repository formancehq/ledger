package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/go-libs/v2/logging"
	ingester "github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

type Connector struct {
	output io.Writer
}

func (connector *Connector) Stop(_ context.Context) error {
	return nil
}

func (connector *Connector) Start(_ context.Context) error {
	return nil
}

func (connector *Connector) ClearData(_ context.Context, _ string) error {
	return nil
}

func (connector *Connector) Accept(_ context.Context, logs ...ingester.LogWithLedger) ([]error, error) {
	for _, log := range logs {
		data, err := json.MarshalIndent(log, "", "  ")
		if err != nil {
			return nil, err
		}
		_, _ = fmt.Fprintln(connector.output, string(data))
	}

	return make([]error, len(logs)), nil
}

func NewConnector(_ drivers.ServiceConfig, _ struct{}, _ logging.Logger) (*Connector, error) {
	return &Connector{
		output: os.Stdout,
	}, nil
}

var _ drivers.Driver = (*Connector)(nil)
