package modulr

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/connectors/modulr/client"
	"github.com/formancehq/payments/internal/app/ingestion"
	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

func taskFetchTransactions(logger logging.Logger, client *client.Client, accountID string) task.Task {
	return func(
		ctx context.Context,
		ingester ingestion.Ingester,
	) error {
		logger.Info("Fetching transactions for account", accountID)

		transactions, err := client.GetTransactions(accountID)
		if err != nil {
			return err
		}

		batch := ingestion.PaymentBatch{}

		for _, transaction := range transactions {
			logger.Info(transaction)

			rawData, err := json.Marshal(transaction)
			if err != nil {
				return fmt.Errorf("failed to marshal transaction: %w", err)
			}

			batchElement := ingestion.PaymentBatchElement{
				Payment: &models.Payment{
					Reference: transaction.ID,
					Type:      matchTransactionType(transaction.Type),
					Status:    models.PaymentStatusSucceeded,
					Scheme:    models.PaymentSchemeOther,
					Amount:    int64(transaction.Amount * 100),
					Asset:     models.PaymentAsset(fmt.Sprintf("%s/2", transaction.Account.Currency)),
					RawData:   rawData,
				},
			}

			batch = append(batch, batchElement)
		}

		return ingester.IngestPayments(ctx, batch, struct{}{})
	}
}

func matchTransactionType(transactionType string) models.PaymentType {
	if transactionType == "PI_REV" ||
		transactionType == "PO_REV" ||
		transactionType == "ADHOC" ||
		transactionType == "INT_INTERC" {
		return models.PaymentTypeOther
	}

	if strings.HasPrefix(transactionType, "PI_") {
		return models.PaymentTypePayIn
	}

	if strings.HasPrefix(transactionType, "PO_") {
		return models.PaymentTypePayOut
	}

	return models.PaymentTypeOther
}
