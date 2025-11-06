//go:build ee

package audit

import (
	"strings"

	"github.com/formancehq/go-libs/v3/publish"
	"github.com/spf13/cobra"
)

// BuildAuditTopic constructs the audit topic based on existing publisher wildcard mapping
//
// Examples:
//   - Wildcard "*:stack.ledger" → Returns "stack.audit"
//   - Wildcard "*:example-toto.ledger" → Returns "example-toto.audit"
//   - No wildcard → Returns "AUDIT"
//
// This ensures audit logs are automatically routed to {stack}.audit without manual configuration
func BuildAuditTopic(cmd *cobra.Command) string {
	topicMappings, _ := cmd.Flags().GetStringSlice(publish.PublisherTopicMappingFlag)

	for _, mapping := range topicMappings {
		parts := strings.SplitN(mapping, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Found wildcard mapping like "*:stack.ledger"
		if key == "*" {
			// Extract stack prefix before the last dot
			if dotIndex := strings.LastIndex(value, "."); dotIndex > 0 {
				stackPrefix := value[:dotIndex]
				return stackPrefix + ".audit"
			}
			// Edge case: wildcard without dot (e.g., "*:events")
			return value + ".audit"
		}
	}

	// No wildcard mapping, use default
	return "AUDIT"
}
