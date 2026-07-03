package events

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

// TestCompleteEventTypes exercises the comma-separated shell completion for the
// --event-types flag: prefix preservation across commas, filtering by the
// segment after the last comma, and exclusion of already-selected types.
func TestCompleteEventTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		toComplete string
		want       []string
	}{
		{
			name:       "empty offers every type",
			toComplete: "",
			want: []string{
				"COMMITTED_TRANSACTION",
				"CREATED_LEDGER",
				"DELETED_LEDGER",
				"DELETED_METADATA",
				"REVERTED_TRANSACTION",
				"SAVED_METADATA",
			},
		},
		{
			name:       "prefix filters to a single match",
			toComplete: "COMM",
			want:       []string{"COMMITTED_TRANSACTION"},
		},
		{
			name:       "lowercase prefix still matches (case-insensitive)",
			toComplete: "comm",
			want:       []string{"COMMITTED_TRANSACTION"},
		},
		{
			name:       "after a comma the prefix is preserved and the selected type is excluded",
			toComplete: "COMMITTED_TRANSACTION,",
			want: []string{
				"COMMITTED_TRANSACTION,CREATED_LEDGER",
				"COMMITTED_TRANSACTION,DELETED_LEDGER",
				"COMMITTED_TRANSACTION,DELETED_METADATA",
				"COMMITTED_TRANSACTION,REVERTED_TRANSACTION",
				"COMMITTED_TRANSACTION,SAVED_METADATA",
			},
		},
		{
			name:       "trailing segment filters remaining candidates",
			toComplete: "COMMITTED_TRANSACTION,DEL",
			want: []string{
				"COMMITTED_TRANSACTION,DELETED_LEDGER",
				"COMMITTED_TRANSACTION,DELETED_METADATA",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, directive := completeEventTypes(nil, nil, tt.toComplete)

			assert.Equal(t, tt.want, got)
			assert.Equal(t, cobra.ShellCompDirectiveNoSpace|cobra.ShellCompDirectiveNoFileComp, directive)
		})
	}
}
