package bankingcircle

import (
	"fmt"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

const (
	taskNameFetchPayments = "fetch-payments"
)

// TaskDescriptor is the definition of a task.
type TaskDescriptor struct {
	Name string `json:"name" yaml:"name" bson:"name"`
	Key  string `json:"key" yaml:"key" bson:"key"`
}

func resolveTasks(logger logging.Logger, config Config) func(taskDefinition models.TaskDescriptor) task.Task {
	bankingCircleClient, err := newClient(config.Username, config.Password, config.Endpoint, config.AuthorizationEndpoint, logger)
	if err != nil {
		logger.Error(err)

		return nil
	}

	return func(taskDefinition models.TaskDescriptor) task.Task {
		taskDescriptor, err := models.DecodeTaskDescriptor[TaskDescriptor](taskDefinition)
		if err != nil {
			logger.Error(err)

			return nil
		}

		switch taskDescriptor.Key {
		case taskNameFetchPayments:
			return taskFetchPayments(logger, bankingCircleClient)
		}

		// This should never happen.
		return func() error {
			return fmt.Errorf("key '%s': %w", taskDescriptor.Key, ErrMissingTask)
		}
	}
}
