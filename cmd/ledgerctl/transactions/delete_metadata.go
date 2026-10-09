package transactions

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewDeleteMetadataCommand creates the transactions delete-metadata command.
func NewDeleteMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete-metadata [transaction-id] [key]",
		Aliases: []string{"del-meta", "dm", "rm-meta"},
		Short:   "Delete metadata from a transaction",
		Long: `Delete a metadata key from a transaction via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl transactions delete-metadata 42 status --ledger my-ledger
  ledgerctl transactions delete-metadata 42 reason
  ledgerctl tx dm 42 status`,
		Args:              cobra.MaximumNArgs(2),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}
