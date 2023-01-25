package bankingcircle

import (
	"context"
	"encoding/json"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/payments/internal/app/ingestion"
	"github.com/formancehq/payments/internal/app/task"

	"github.com/formancehq/go-libs/logging"
)

func taskFetchPayments(logger logging.Logger, client *client) task.Task {
	return func(
		ctx context.Context,
		scheduler task.Scheduler,
		ingester ingestion.Ingester,
	) error {
		paymentsList, err := client.getAllPayments(ctx)
		if err != nil {
			return err
		}

		batch := ingestion.PaymentBatch{}

		for _, paymentEl := range paymentsList {
			logger.Info(paymentEl)

			raw, err := json.Marshal(paymentEl)
			if err != nil {
				return err
			}

			batchElement := ingestion.PaymentBatchElement{
				Payment: &models.Payment{
					Reference: paymentEl.TransactionReference,
					Type:      matchPaymentType(paymentEl.Classification),
					Status:    matchPaymentStatus(paymentEl.Status),
					Scheme:    models.PaymentSchemeOther,
					Amount:    int64(paymentEl.Transfer.Amount.Amount * 100),
					Asset:     models.PaymentAsset(paymentEl.Transfer.Amount.Currency + "/2"),
					RawData:   raw,
				},
			}

			batch = append(batch, batchElement)
		}

		return ingester.IngestPayments(ctx, batch, struct{}{})
	}
}

func matchPaymentStatus(paymentStatus string) models.PaymentStatus {
	switch paymentStatus {
	case "Processed":
		return models.PaymentStatusSucceeded
	// On MissingFunding - the payment is still in progress.
	// If there will be funds available within 10 days - the payment will be processed.
	// Otherwise - it will be cancelled.
	case "PendingProcessing", "MissingFunding":
		return models.PaymentStatusPending
	case "Rejected", "Cancelled", "Reversed", "Returned":
		return models.PaymentStatusFailed
	}

	return models.PaymentStatusOther
}

func matchPaymentType(paymentType string) models.PaymentType {
	switch paymentType {
	case "Incoming":
		return models.PaymentTypePayIn
	case "Outgoing":
		return models.PaymentTypePayOut
	}

	return models.PaymentTypeOther
}
