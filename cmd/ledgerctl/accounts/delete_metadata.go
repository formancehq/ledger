package accounts

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewDeleteMetadataCommand creates the accounts delete-metadata command.
func NewDeleteMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "delete-metadata [address] [key]",
		Aliases: []string{"del-meta", "dm", "rm-meta"},
		Short:   "Delete metadata from an account",
		Long: `Delete a metadata key from an account via gRPC.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl accounts delete-metadata bank type --ledger my-ledger
  ledgerctl accounts delete-metadata users:alice role
  ledgerctl acc dm bank type`,
		Args:              cobra.MaximumNArgs(2),
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().BoolP("yes", "y", false, "Skip confirmation prompt")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}
