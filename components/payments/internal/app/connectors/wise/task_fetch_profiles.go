package wise

import (
	"context"
	"fmt"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

func taskFetchProfiles(logger logging.Logger, client *client) task.Task {
	return func(
		ctx context.Context,
		scheduler task.Scheduler,
	) error {
		profiles, err := client.getProfiles()
		if err != nil {
			return err
		}

		for _, profile := range profiles {
			logger.Infof(fmt.Sprintf("scheduling fetch-transfers: %d", profile.ID))

			descriptor, err := models.EncodeTaskDescriptor(TaskDescriptor{
				Name:      "Fetch transfers from client by profile",
				Key:       taskNameFetchTransfers,
				ProfileID: profile.ID,
			})
			if err != nil {
				return err
			}

			err = scheduler.Schedule(descriptor, true)
			if err != nil {
				return err
			}
		}

		return nil
	}
}
