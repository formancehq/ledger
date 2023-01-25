package stripe

import (
	"context"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/integration"
	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

const Name = models.ConnectorProviderStripe

type Connector struct {
	logger logging.Logger
	cfg    Config
}

func (c *Connector) Install(ctx task.ConnectorContext) error {
	descriptor, err := models.EncodeTaskDescriptor(TaskDescriptor{
		Name: "Main task to periodically fetch transactions",
		Main: true,
	})
	if err != nil {
		return err
	}

	return ctx.Scheduler().Schedule(descriptor, false)
}

func (c *Connector) Uninstall(ctx context.Context) error {
	return nil
}

func (c *Connector) Resolve(descriptor models.TaskDescriptor) task.Task {
	taskDescriptor, err := models.DecodeTaskDescriptor[TaskDescriptor](descriptor)
	if err != nil {
		panic(err)
	}

	if taskDescriptor.Main {
		return MainTask(c.cfg)
	}

	return ConnectedAccountTask(c.cfg, taskDescriptor.Account)
}

var _ integration.Connector = &Connector{}

func newConnector(logger logging.Logger, cfg Config) *Connector {
	return &Connector{
		logger: logger.WithFields(map[string]any{
			"component": "connector",
		}),
		cfg: cfg,
	}
}
