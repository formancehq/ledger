package modulr

import (
	"fmt"

	"github.com/formancehq/payments/internal/app/connectors/modulr/client"
	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

const (
	taskNameFetchTransactions = "fetch-transactions"
	taskNameFetchAccounts     = "fetch-accounts"
)

// TaskDescriptor is the definition of a task.
type TaskDescriptor struct {
	Name      string `json:"name" yaml:"name" bson:"name"`
	Key       string `json:"key" yaml:"key" bson:"key"`
	AccountID string `json:"accountID" yaml:"accountID" bson:"accountID"`
}

func resolveTasks(logger logging.Logger, config Config) func(taskDefinition TaskDescriptor) task.Task {
	modulrClient, err := client.NewClient(config.APIKey, config.APISecret, config.Endpoint)
	if err != nil {
		return func(taskDefinition TaskDescriptor) task.Task {
			return func() error {
				return fmt.Errorf("key '%s': %w", taskDefinition.Key, ErrMissingTask)
			}
		}
	}

	return func(taskDefinition TaskDescriptor) task.Task {
		switch taskDefinition.Key {
		case taskNameFetchAccounts:
			return taskFetchAccounts(logger, modulrClient)
		case taskNameFetchTransactions:
			return taskFetchTransactions(logger, modulrClient, taskDefinition.AccountID)
		}

		// This should never happen.
		return func() error {
			return fmt.Errorf("key '%s': %w", taskDefinition.Key, ErrMissingTask)
		}
	}
}
