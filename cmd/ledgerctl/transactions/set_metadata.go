package transactions

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewSetMetadataCommand creates the transactions set-metadata command.
func NewSetMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-metadata [transaction-id]",
		Aliases: []string{"set-meta", "sm"},
		Short:   "Set metadata on a transaction",
		Long: `Set metadata on a transaction via gRPC.

Metadata is provided as key=value pairs using the --metadata flag.
Multiple metadata entries can be set at once.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl transactions set-metadata 42 --ledger my-ledger --metadata status=processed
  ledgerctl transactions set-metadata 42 --metadata reason="refund" --metadata ticket=JIRA-123
  ledgerctl tx sm 42 -m status=processed`,
		Args:              cobra.MaximumNArgs(1),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArrayP("metadata", "m", nil, "Metadata key=value pairs (can be repeated)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}
