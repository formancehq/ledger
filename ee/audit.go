//go:build ee

package ee

import (
	"context"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/audit"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

const serviceName = "ledger"

// AuditModule creates the audit Fx module for Enterprise Edition
func AuditModule(cobraCmd *cobra.Command) fx.Option {
	return fx.Module("audit",
		fx.Provide(func(
			publisher message.Publisher,
			logger logging.Logger,
		) (*audit.PublisherClient, error) {
			// Load audit configuration from flags
			auditEnabled, _ := cobraCmd.Flags().GetBool(audit.AuditEnabledFlag)

			if !auditEnabled {
				return nil, nil // Audit disabled, return nil (optional in DI)
			}

			// Load audit settings
			maxBodySize, _ := cobraCmd.Flags().GetInt64(audit.AuditMaxBodySizeFlag)
			excludedPaths, _ := cobraCmd.Flags().GetStringSlice(audit.AuditExcludedPathsFlag)
			sensitiveHeaders, _ := cobraCmd.Flags().GetStringSlice(audit.AuditSensitiveHeadersFlag)

			// Auto-detect audit topic from publisher wildcard mapping
			auditTopic := audit.BuildAuditTopic(cobraCmd)

			// Create client
			client := audit.NewClientWithPublisher(
				publisher,
				auditTopic,
				serviceName,
				maxBodySize,
				excludedPaths,
				sensitiveHeaders,
				logger,
			)

			logger.Infof("Audit logging enabled (topic=%s, max-body-size=%d, excluded-paths=%d)",
				auditTopic, maxBodySize, len(excludedPaths))

			return client, nil
		}),

		// Lifecycle: Close audit client on shutdown
		fx.Invoke(func(lc fx.Lifecycle, client *audit.PublisherClient) {
			if client == nil {
				return // Audit disabled
			}

			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return client.Close()
				},
			})
		}),
	)
}
