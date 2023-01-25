package modulr

import (
	"context"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/connectors/modulr/client"
	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

func taskFetchAccounts(logger logging.Logger, client *client.Client) task.Task {
	return func(
		ctx context.Context,
		scheduler task.Scheduler,
	) error {
		logger.Info(taskNameFetchAccounts)

		accounts, err := client.GetAccounts()
		if err != nil {
			return err
		}

		for _, account := range accounts {
			logger.Infof("scheduling fetch-transactions: %s", account.ID)

			transactionsTask, err := models.EncodeTaskDescriptor(TaskDescriptor{
				Name:      "Fetch transactions from client by account",
				Key:       taskNameFetchTransactions,
				AccountID: account.ID,
			})
			if err != nil {
				return err
			}

			err = scheduler.Schedule(transactionsTask, false)
			if err != nil {
				return err
			}
		}

		return nil
	}
}
