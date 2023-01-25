package integration

import (
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/models"
)

type Loader[ConnectorConfig models.ConnectorConfigObject] interface {
	Name() models.ConnectorProvider
	Load(logger logging.Logger, config ConnectorConfig) Connector

	// ApplyDefaults is used to fill default values of the provided configuration object
	ApplyDefaults(t ConnectorConfig) ConnectorConfig

	// AllowTasks define how many task the connector can run
	// If too many tasks are scheduled by the connector,
	// those will be set to pending state and restarted later when some other tasks will be terminated
	AllowTasks() int
}

type LoaderBuilder[ConnectorConfig models.ConnectorConfigObject] struct {
	loadFunction  func(logger logging.Logger, config ConnectorConfig) Connector
	applyDefaults func(t ConnectorConfig) ConnectorConfig
	name          models.ConnectorProvider
	allowedTasks  int
}

func (b *LoaderBuilder[ConnectorConfig]) WithLoad(loadFunction func(logger logging.Logger,
	config ConnectorConfig) Connector,
) *LoaderBuilder[ConnectorConfig] {
	b.loadFunction = loadFunction

	return b
}

func (b *LoaderBuilder[ConnectorConfig]) WithApplyDefaults(
	applyDefaults func(t ConnectorConfig) ConnectorConfig,
) *LoaderBuilder[ConnectorConfig] {
	b.applyDefaults = applyDefaults

	return b
}

func (b *LoaderBuilder[ConnectorConfig]) WithAllowedTasks(v int) *LoaderBuilder[ConnectorConfig] {
	b.allowedTasks = v

	return b
}

func (b *LoaderBuilder[ConnectorConfig]) Build() *BuiltLoader[ConnectorConfig] {
	return &BuiltLoader[ConnectorConfig]{
		loadFunction:  b.loadFunction,
		applyDefaults: b.applyDefaults,
		name:          b.name,
		allowedTasks:  b.allowedTasks,
	}
}

func NewLoaderBuilder[ConnectorConfig models.ConnectorConfigObject](name models.ConnectorProvider,
) *LoaderBuilder[ConnectorConfig] {
	return &LoaderBuilder[ConnectorConfig]{
		name: name,
	}
}

type BuiltLoader[ConnectorConfig models.ConnectorConfigObject] struct {
	loadFunction  func(logger logging.Logger, config ConnectorConfig) Connector
	applyDefaults func(t ConnectorConfig) ConnectorConfig
	name          models.ConnectorProvider
	allowedTasks  int
}

func (b *BuiltLoader[ConnectorConfig]) AllowTasks() int {
	return b.allowedTasks
}

func (b *BuiltLoader[ConnectorConfig]) Name() models.ConnectorProvider {
	return b.name
}

func (b *BuiltLoader[ConnectorConfig]) Load(logger logging.Logger, config ConnectorConfig) Connector {
	if b.loadFunction != nil {
		return b.loadFunction(logger, config)
	}

	return nil
}

func (b *BuiltLoader[ConnectorConfig]) ApplyDefaults(t ConnectorConfig) ConnectorConfig {
	if b.applyDefaults != nil {
		return b.applyDefaults(t)
	}

	return t
}

var _ Loader[models.EmptyConnectorConfig] = &BuiltLoader[models.EmptyConnectorConfig]{}
