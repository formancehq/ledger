//go:build ee

package audit

import (
	"testing"

	"github.com/formancehq/go-libs/v3/publish"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestBuildAuditTopic(t *testing.T) {
	tests := []struct {
		name          string
		topicMappings []string
		expectedTopic string
		description   string
	}{
		{
			name:          "wildcard with stack prefix",
			topicMappings: []string{"*:stack.ledger"},
			expectedTopic: "stack.audit",
			description:   "Should extract stack prefix and append .audit",
		},
		{
			name:          "wildcard with complex stack name",
			topicMappings: []string{"*:example-toto.ledger"},
			expectedTopic: "example-toto.audit",
			description:   "Should handle complex stack names with hyphens",
		},
		{
			name:          "wildcard without dot",
			topicMappings: []string{"*:events"},
			expectedTopic: "events.audit",
			description:   "Should append .audit when no dot exists",
		},
		{
			name:          "specific mapping and wildcard",
			topicMappings: []string{"COMMITTED_TRANSACTIONS:txns", "*:prod.ledger"},
			expectedTopic: "prod.audit",
			description:   "Should use wildcard when both exist",
		},
		{
			name:          "no wildcard mapping",
			topicMappings: []string{"COMMITTED_TRANSACTIONS:txns", "AUDIT:custom"},
			expectedTopic: "AUDIT",
			description:   "Should return default AUDIT when no wildcard",
		},
		{
			name:          "empty mappings",
			topicMappings: []string{},
			expectedTopic: "AUDIT",
			description:   "Should return default AUDIT when no mappings",
		},
		{
			name:          "malformed mapping",
			topicMappings: []string{"invalid-mapping", "*:stack.ledger"},
			expectedTopic: "stack.audit",
			description:   "Should skip malformed and use valid wildcard",
		},
		{
			name:          "wildcard with multiple dots",
			topicMappings: []string{"*:prod.us-east.ledger"},
			expectedTopic: "prod.us-east.audit",
			description:   "Should preserve all dots before last segment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			cmd.Flags().StringSlice(publish.PublisherTopicMappingFlag, tt.topicMappings, "")

			result := BuildAuditTopic(cmd)

			assert.Equal(t, tt.expectedTopic, result, tt.description)
		})
	}
}
