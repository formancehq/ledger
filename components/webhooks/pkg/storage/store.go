package storage

import (
	"context"

	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/pkg/errors"
)

var (
	ErrConfigNotFound      = errors.New("config not found")
	ErrConfigNotModified   = errors.New("config not modified")
	ErrWebhookIDNotFound   = errors.New("webhook ID not found")
	ErrAttemptsNotModified = errors.New("attempt not modified")
)

type Store interface {
	FindManyConfigs(ctx context.Context, filter map[string]any) ([]webhooks.Config, error)
	InsertOneConfig(ctx context.Context, cfg webhooks.ConfigUser) (webhooks.Config, error)
	DeleteOneConfig(ctx context.Context, id string) error
	UpdateOneConfigActivation(ctx context.Context, id string, active bool) (webhooks.Config, error)
	UpdateOneConfigSecret(ctx context.Context, id, secret string) (webhooks.Config, error)
	FindAttemptsToRetryByWebhookID(ctx context.Context, webhookID string) ([]webhooks.Attempt, error)
	FindWebhookIDsToRetry(ctx context.Context) (webhookIDs []string, err error)
	UpdateAttemptsStatus(ctx context.Context, webhookID string, status string) ([]webhooks.Attempt, error)
	InsertOneAttempt(ctx context.Context, att webhooks.Attempt) error
	Close(ctx context.Context) error
}
