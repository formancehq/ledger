//go:build ee

package audit

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

// Config holds the audit configuration
type Config struct {
	Topic            string // Audit topic (auto-detected from publisher config)
	AppName          string
	MaxBodySize      int64
	ExcludedPaths    []string
	SensitiveHeaders []string
}

// NewFXModule creates an Fx module for audit using existing publisher
func NewFXModule(cfg Config) fx.Option {
	return fx.Module("audit",
		fx.Provide(func(publisher message.Publisher, logger *zap.Logger) *Client {
			return NewClientWithPublisher(
				publisher,
				cfg.Topic,
				cfg.AppName,
				cfg.MaxBodySize,
				cfg.ExcludedPaths,
				cfg.SensitiveHeaders,
				logger,
			)
		}),
		fx.Invoke(func(lc fx.Lifecycle, client *Client) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return client.Close()
				},
			})
		}),
	)
}
