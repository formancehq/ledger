package currencycloud

import (
	"context"
	"fmt"

	"github.com/formancehq/payments/internal/app/connectors/currencycloud/client"

	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

const (
	taskNameFetchTransactions = "fetch-transactions"
)

// TaskDescriptor is the definition of a task.
type TaskDescriptor struct {
	Name string `json:"name" yaml:"name" bson:"name"`
}

func resolveTasks(logger logging.Logger, config Config) task.Task {
	return func(ctx context.Context, taskDescriptor TaskDescriptor) task.Task {
		currencyCloudClient, err := client.NewClient(ctx, config.LoginID, config.APIKey, config.Endpoint)
		if err != nil {
			return func(ctx context.Context, taskDefinition TaskDescriptor) task.Task {
				return func() error {
					return fmt.Errorf("failed to initiate client: %w", err)
				}
			}
		}

		switch taskDescriptor.Name {
		case taskNameFetchTransactions:
			return taskFetchTransactions(logger, currencyCloudClient, config)
		}

		// This should never happen.
		return func() error {
			return fmt.Errorf("key '%s': %w", taskDescriptor.Name, ErrMissingTask)
		}
	}
}
