package stripe

import (
	"time"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/connectors"
	"github.com/formancehq/payments/internal/app/integration"
)

type Loader struct{}

const allowedTasks = 50

func (l *Loader) AllowTasks() int {
	return allowedTasks
}

func (l *Loader) Name() models.ConnectorProvider {
	return Name
}

func (l *Loader) Load(logger logging.Logger, config Config) integration.Connector {
	return newConnector(logger, config)
}

func (l *Loader) ApplyDefaults(cfg Config) Config {
	if cfg.PageSize == 0 {
		cfg.PageSize = 10
	}

	if cfg.PollingPeriod.Duration == 0 {
		cfg.PollingPeriod = connectors.Duration{Duration: 2 * time.Minute}
	}

	return cfg
}

func NewLoader() *Loader {
	return &Loader{}
}
