//go:build ee

package ee

import (
	"github.com/spf13/cobra"
)

// EE Audit flag names
const (
	AuditEnabledFlag          = "audit-enabled"
	AuditMaxBodySizeFlag      = "audit-max-body-size"
	AuditExcludedPathsFlag    = "audit-excluded-paths"
	AuditSensitiveHeadersFlag = "audit-sensitive-headers"
)

// AddFlags adds all EE-specific flags to the command
// Audit reuses existing publisher (--publisher-kafka-enabled or --publisher-nats-enabled)
// Topic is "AUDIT" and can be mapped via --publisher-topic-mapping=AUDIT:your-topic
func AddFlags(cmd *cobra.Command) {
	cmd.Flags().Bool(AuditEnabledFlag, false, "Enable audit logging (EE only, requires publisher)")
	cmd.Flags().Int64(AuditMaxBodySizeFlag, 1024*1024, "Maximum request/response body size to capture (bytes)")
	cmd.Flags().StringSlice(AuditExcludedPathsFlag, []string{"/_healthcheck", "/_/healthcheck"}, "Paths to exclude from audit")
	cmd.Flags().StringSlice(AuditSensitiveHeadersFlag, []string{"Authorization", "Cookie", "X-API-Key"}, "Headers to sanitize in audit logs")
}
