package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/payments/internal/app/messages"

	"github.com/formancehq/payments/internal/app/models"
)

type PaymentBatchElement struct {
	Payment    *models.Payment
	Adjustment *models.Adjustment
	Metadata   *models.Metadata
	Update     bool
}

type PaymentBatch []PaymentBatchElement

type IngesterFn func(ctx context.Context, batch PaymentBatch, commitState any) error

func (fn IngesterFn) IngestPayments(ctx context.Context, batch PaymentBatch, commitState any) error {
	return fn(ctx, batch, commitState)
}

func (i *DefaultIngester) IngestPayments(ctx context.Context, batch PaymentBatch, commitState any) error {
	startingAt := time.Now()

	i.logger.WithFields(map[string]interface{}{
		"size":       len(batch),
		"startingAt": startingAt,
	}).Debugf("Ingest batch")

	var allPayments []*models.Payment //nolint:prealloc // length is unknown

	for batchIdx := range batch {
		payment := batch[batchIdx].Payment

		if payment == nil {
			continue
		}

		allPayments = append(allPayments, payment)
	}

	if err := i.repo.UpsertPayments(ctx, i.provider, allPayments); err != nil {
		return fmt.Errorf("error upserting payments: %w", err)
	}

	taskDescriptor, err := json.Marshal(i.descriptor)
	if err != nil {
		return fmt.Errorf("error marshaling task descriptor: %w", err)
	}

	taskState, err := json.Marshal(commitState)
	if err != nil {
		return fmt.Errorf("error marshaling task state: %w", err)
	}

	if err = i.repo.UpdateTaskState(ctx, i.provider, taskDescriptor, taskState); err != nil {
		return fmt.Errorf("error updating task state: %w", err)
	}

	for paymentIdx := range allPayments {
		err = i.publisher.Publish(ctx, messages.TopicPayments,
			messages.NewEventSavedPayments(allPayments[paymentIdx], i.provider))
		if err != nil {
			i.logger.Errorf("Publishing message: %w", err)

			continue
		}
	}

	endedAt := time.Now()

	i.logger.WithFields(map[string]interface{}{
		"size":    len(batch),
		"endedAt": endedAt,
		"latency": endedAt.Sub(startingAt).String(),
	}).Debugf("Batch ingested")

	return nil
}
