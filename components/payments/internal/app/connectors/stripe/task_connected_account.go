package stripe

import (
	"context"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/payments/internal/app/ingestion"
	"github.com/formancehq/payments/internal/app/task"
	"github.com/stripe/stripe-go/v72"
)

func ingestBatch(ctx context.Context, logger logging.Logger, ingester ingestion.Ingester,
	bts []*stripe.BalanceTransaction, commitState TimelineState, tail bool,
) error {
	batch := ingestion.PaymentBatch{}

	for i := range bts {
		batchElement, handled := CreateBatchElement(bts[i], !tail)

		if !handled {
			logger.Debugf("Balance transaction type not handled: %s", bts[i].Type)

			continue
		}

		if batchElement.Adjustment == nil && batchElement.Payment == nil {
			continue
		}

		batch = append(batch, batchElement)
	}

	logger.WithFields(map[string]interface{}{
		"state": commitState,
	}).Debugf("updating state")

	err := ingester.IngestPayments(ctx, batch, commitState)
	if err != nil {
		return err
	}

	return nil
}

func ConnectedAccountTask(config Config, account string) func(ctx context.Context, logger logging.Logger,
	ingester ingestion.Ingester, resolver task.StateResolver) error {
	return func(ctx context.Context, logger logging.Logger, ingester ingestion.Ingester,
		resolver task.StateResolver,
	) error {
		logger.Infof("Create new trigger")

		trigger := NewTimelineTrigger(
			logger,
			IngesterFn(func(ctx context.Context, bts []*stripe.BalanceTransaction, commitState TimelineState, tail bool) error {
				return ingestBatch(ctx, logger, ingester, bts, commitState, tail)
			}),
			NewTimeline(NewDefaultClient(config.APIKey).
				ForAccount(account), config.TimelineConfig, task.MustResolveTo(ctx, resolver, TimelineState{})),
		)

		return trigger.Fetch(ctx)
	}
}
