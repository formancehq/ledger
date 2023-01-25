package wise

import (
	"fmt"

	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

const (
	taskNameFetchTransfers = "fetch-transfers"
	taskNameFetchProfiles  = "fetch-profiles"
)

// TaskDescriptor is the definition of a task.
type TaskDescriptor struct {
	Name      string `json:"name" yaml:"name" bson:"name"`
	Key       string `json:"key" yaml:"key" bson:"key"`
	ProfileID uint64 `json:"profileID" yaml:"profileID" bson:"profileID"`
}

func resolveTasks(logger logging.Logger, config Config) func(taskDefinition TaskDescriptor) task.Task {
	client := newClient(config.APIKey)

	return func(taskDefinition TaskDescriptor) task.Task {
		switch taskDefinition.Key {
		case taskNameFetchProfiles:
			return taskFetchProfiles(logger, client)
		case taskNameFetchTransfers:
			return taskFetchTransfers(logger, client, taskDefinition.ProfileID)
		}

		// This should never happen.
		return func() error {
			return fmt.Errorf("key '%s': %w", taskDefinition.Key, ErrMissingTask)
		}
	}
}
