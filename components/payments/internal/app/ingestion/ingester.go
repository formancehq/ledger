package ingestion

import (
	"context"
	"encoding/json"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/publish"
	"github.com/formancehq/payments/internal/app/models"
)

type Ingester interface {
	IngestPayments(ctx context.Context, batch PaymentBatch, commitState any) error
	IngestAccounts(ctx context.Context, batch AccountBatch) error
}

type DefaultIngester struct {
	repo       Repository
	logger     logging.Logger
	provider   models.ConnectorProvider
	descriptor models.TaskDescriptor
	publisher  publish.Publisher
}

type Repository interface {
	UpsertAccounts(ctx context.Context, provider models.ConnectorProvider, accounts []models.Account) error
	UpsertPayments(ctx context.Context, provider models.ConnectorProvider, payments []*models.Payment) error
	UpdateTaskState(ctx context.Context, provider models.ConnectorProvider, descriptor models.TaskDescriptor, state json.RawMessage) error
}

func NewDefaultIngester(
	provider models.ConnectorProvider,
	descriptor models.TaskDescriptor,
	repo Repository,
	logger logging.Logger,
	publisher publish.Publisher,
) *DefaultIngester {
	return &DefaultIngester{
		provider:   provider,
		descriptor: descriptor,
		repo:       repo,
		logger:     logger,
		publisher:  publisher,
	}
}
