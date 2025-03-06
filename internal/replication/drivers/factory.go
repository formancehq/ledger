package drivers

import (
	"context"
	"encoding/json"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/pkg/errors"
)

//go:generate mockgen -source factory.go -destination factory_generated.go -package drivers . Factory
type Factory interface {
	// Create can return following errors:
	// * ErrConnectorNotFound
	Create(ctx context.Context, id string) (Driver, json.RawMessage, error)
}

type DriverFactoryWithBatching struct {
	underlying Factory
	logger     logging.Logger
}

func (c *DriverFactoryWithBatching) Create(ctx context.Context, id string) (Driver, json.RawMessage, error) {
	connector, rawConfig, err := c.underlying.Create(ctx, id)
	if err != nil {
		return nil, nil, err
	}

	type batchingHolder struct {
		Batching Batching `json:"batching"`
	}
	bh := batchingHolder{}
	if err := json.Unmarshal(rawConfig, &bh); err != nil {
		return nil, nil, errors.Wrap(err, "extracting batching config")
	}

	bh.Batching.SetDefaults()
	if err := bh.Batching.Validate(); err != nil {
		return nil, nil, errors.Wrap(err, "validating batching config")
	}

	return newBatcher(connector, bh.Batching, c.logger), rawConfig, nil
}

var _ Factory = (*DriverFactoryWithBatching)(nil)

func NewWithBatchingConnectorFactory(underlying Factory, logger logging.Logger) *DriverFactoryWithBatching {
	return &DriverFactoryWithBatching{
		underlying: underlying,
		logger:     logger,
	}
}
