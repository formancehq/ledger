package currencycloud

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/connectors/currencycloud/client"

	"github.com/formancehq/payments/internal/app/ingestion"
	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

func taskFetchTransactions(logger logging.Logger, client *client.Client, config Config) task.Task {
	return func(
		ctx context.Context,
		ingester ingestion.Ingester,
	) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(config.PollingPeriod.Duration()):
				if err := ingestTransactions(ctx, logger, client, ingester); err != nil {
					return err
				}
			}
		}
	}
}

func ingestTransactions(ctx context.Context, logger logging.Logger,
	client *client.Client, ingester ingestion.Ingester,
) error {
	page := 1

	for {
		if page < 0 {
			break
		}

		logger.Info("Fetching transactions")

		transactions, nextPage, err := client.GetTransactions(ctx, page)
		if err != nil {
			return err
		}

		page = nextPage

		batch := ingestion.PaymentBatch{}

		for _, transaction := range transactions {
			logger.Info(transaction)

			var amount float64

			amount, err = strconv.ParseFloat(transaction.Amount, 64)
			if err != nil {
				return fmt.Errorf("failed to parse amount: %w", err)
			}

			var rawData json.RawMessage

			rawData, err = json.Marshal(transaction)
			if err != nil {
				return fmt.Errorf("failed to marshal transaction: %w", err)
			}

			batchElement := ingestion.PaymentBatchElement{
				Payment: &models.Payment{
					Reference: transaction.ID,
					Type:      matchTransactionType(transaction.Type),
					Status:    matchTransactionStatus(transaction.Status),
					Scheme:    models.PaymentSchemeOther,
					Amount:    int64(amount * 100),
					Asset:     models.PaymentAsset(fmt.Sprintf("%s/2", transaction.Currency)),
					RawData:   rawData,
				},
			}

			batch = append(batch, batchElement)
		}

		err = ingester.IngestPayments(ctx, batch, struct{}{})
		if err != nil {
			return err
		}
	}

	return nil
}

func matchTransactionType(transactionType string) models.PaymentType {
	switch transactionType {
	case "credit":
		return models.PaymentTypePayOut
	case "debit":
		return models.PaymentTypePayIn
	}

	return models.PaymentTypeOther
}

func matchTransactionStatus(transactionStatus string) models.PaymentStatus {
	switch transactionStatus {
	case "completed":
		return models.PaymentStatusSucceeded
	case "pending":
		return models.PaymentStatusPending
	case "deleted":
		return models.PaymentStatusFailed
	}

	return models.PaymentStatusOther
}
