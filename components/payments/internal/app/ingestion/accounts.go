package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/payments/internal/app/messages"

	"github.com/formancehq/payments/internal/app/models"
)

type AccountBatchElement struct {
	Reference string
	Provider  string
	Type      models.AccountType
}

type AccountBatch []AccountBatchElement

type AccountIngesterFn func(ctx context.Context, batch AccountBatch, commitState any) error

func (fn AccountIngesterFn) IngestAccounts(ctx context.Context, batch AccountBatch, commitState any) error {
	return fn(ctx, batch, commitState)
}

func (i *DefaultIngester) IngestAccounts(ctx context.Context, batch AccountBatch) error {
	startingAt := time.Now()

	i.logger.WithFields(map[string]interface{}{
		"size":       len(batch),
		"startingAt": startingAt,
	}).Debugf("Ingest accounts batch")

	accounts := make([]models.Account, len(batch))

	for batchIdx := range batch {
		accounts[batchIdx] = models.Account{
			Reference: batch[batchIdx].Reference,
			Provider:  batch[batchIdx].Provider,
			Type:      batch[batchIdx].Type,
		}
	}

	if err := i.repo.UpsertAccounts(ctx, i.provider, accounts); err != nil {
		return fmt.Errorf("error upserting accounts: %w", err)
	}

	err := i.publisher.Publish(ctx, messages.TopicPayments,
		messages.NewEventSavedAccounts(accounts))
	if err != nil {
		i.logger.Errorf("Publishing message: %w", err)
	}

	endedAt := time.Now()

	i.logger.WithFields(map[string]interface{}{
		"size":    len(batch),
		"endedAt": endedAt,
		"latency": endedAt.Sub(startingAt).String(),
	}).Debugf("Accounts batch ingested")

	return nil
}
