//go:build ee

package ee

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/v3/audit"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

// AuditModule creates the audit Fx module for Enterprise Edition
func AuditModule(cobraCmd *cobra.Command, serviceName string) fx.Option {
	return fx.Module("audit",
		fx.Provide(func(
			publisher message.Publisher,
			logger logging.Logger,
		) (*audit.PublisherClient, error) {
			// Load audit configuration from flags
			auditEnabled, err := cobraCmd.Flags().GetBool(audit.AuditEnabledFlag)
			if err != nil {
				return nil, fmt.Errorf("failed to read audit enabled flag: %w", err)
			}
			if !auditEnabled {
				logger.Infof("Audit logging disabled via --%s", audit.AuditEnabledFlag)
				return nil, nil
			}

			// Load audit settings
			maxBodySize, err := cobraCmd.Flags().GetInt64(audit.AuditMaxBodySizeFlag)
			if err != nil {
				return nil, fmt.Errorf("failed to read audit max body size: %w", err)
			}
			excludedPaths, err := cobraCmd.Flags().GetStringSlice(audit.AuditExcludedPathsFlag)
			if err != nil {
				return nil, fmt.Errorf("failed to read audit excluded paths: %w", err)
			}
			sensitiveHeaders, err := cobraCmd.Flags().GetStringSlice(audit.AuditSensitiveHeadersFlag)
			if err != nil {
				return nil, fmt.Errorf("failed to read audit sensitive headers: %w", err)
			}

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
