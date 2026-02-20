package stdout

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger/internal/replication/drivers"
)

type Driver struct {
	output io.Writer
}

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
	for _, log := range logs {
		data, err := json.MarshalIndent(log, "", "  ")
		if err != nil {
			return nil, err
		}
		_, _ = fmt.Fprintln(driver.output, string(data))
	}

	return make([]error, len(logs)), nil
}

func NewDriver(_ struct{}, _ logging.Logger) (*Driver, error) {
	return &Driver{
		output: os.Stdout,
	}, nil
}

var _ drivers.Driver = (*Driver)(nil)
