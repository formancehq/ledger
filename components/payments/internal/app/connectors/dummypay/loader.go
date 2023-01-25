package dummypay

import (
	"time"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/connectors"
	"github.com/formancehq/payments/internal/app/integration"
)

type Loader struct{}

// Name returns the name of the connector.
func (l *Loader) Name() models.ConnectorProvider {
	return Name
}

// AllowTasks returns the amount of tasks that are allowed to be scheduled.
func (l *Loader) AllowTasks() int {
	return 10
}

const (
	// defaultFilePollingPeriod is the default period between file polling.
	defaultFilePollingPeriod = 10 * time.Second

	// defaultFileGenerationPeriod is the default period between file generation.
	defaultFileGenerationPeriod = 5 * time.Second
)

// ApplyDefaults applies default values to the configuration.
func (l *Loader) ApplyDefaults(cfg Config) Config {
	if cfg.FileGenerationPeriod.Duration == 0 {
		cfg.FileGenerationPeriod = connectors.Duration{Duration: defaultFileGenerationPeriod}
	}

	if cfg.FilePollingPeriod.Duration == 0 {
		cfg.FilePollingPeriod = connectors.Duration{Duration: defaultFilePollingPeriod}
	}

	return cfg
}

// Load returns the connector.
func (l *Loader) Load(logger logging.Logger, config Config) integration.Connector {
	return newConnector(logger, config, newFS())
}

// NewLoader creates a new loader.
func NewLoader() *Loader {
	return &Loader{}
}
