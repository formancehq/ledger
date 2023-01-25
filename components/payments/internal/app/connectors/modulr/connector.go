package modulr

import (
	"context"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/integration"
	"github.com/formancehq/payments/internal/app/task"
)

const Name = models.ConnectorProviderModulr

type Connector struct {
	logger logging.Logger
	cfg    Config
}

func (c *Connector) Install(ctx task.ConnectorContext) error {
	taskDescriptor, err := models.EncodeTaskDescriptor(TaskDescriptor{
		Name: "Fetch accounts from client",
		Key:  taskNameFetchAccounts,
	})
	if err != nil {
		return err
	}

	return ctx.Scheduler().Schedule(taskDescriptor, false)
}

func (c *Connector) Uninstall(ctx context.Context) error {
	return nil
}

func (c *Connector) Resolve(descriptor models.TaskDescriptor) task.Task {
	taskDescriptor, err := models.DecodeTaskDescriptor[TaskDescriptor](descriptor)
	if err != nil {
		return nil
	}

	return resolveTasks(c.logger, c.cfg)(taskDescriptor)
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
