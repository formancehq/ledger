package bankingcircle

import (
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/integration"
	"github.com/formancehq/payments/internal/app/models"
	"github.com/formancehq/payments/internal/app/task"
)

const Name = models.ConnectorProviderBankingCircle

// NewLoader creates a new loader.
func NewLoader() integration.Loader[Config] {
	loader := integration.NewLoaderBuilder[Config](Name).
		WithLoad(func(logger logging.Logger, config Config) integration.Connector {
			return integration.NewConnectorBuilder().
				WithInstall(func(ctx task.ConnectorContext) error {
					taskDescriptor, err := models.EncodeTaskDescriptor(TaskDescriptor{
						Name: "Fetch payments from source",
						Key:  taskNameFetchPayments,
					})
					if err != nil {
						return err
					}

					return ctx.Scheduler().Schedule(taskDescriptor, false)
				}).
				WithResolve(resolveTasks(logger, config)).
				Build()
		}).Build()

	return loader
}
