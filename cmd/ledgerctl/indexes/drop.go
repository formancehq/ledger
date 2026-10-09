package indexes

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewDropCommand creates the indexes drop command.
func NewDropCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "drop [flags]",
		Aliases: []string{"d"},
		Short:   "Drop an index from a ledger",
		Long: `Drop an opt-in index from a ledger. This stops the index from being updated
and frees the associated storage.

Index types:
  address              Account→transaction mapping (any role)
  source-address       Source account→transaction mapping
  destination-address  Destination account→transaction mapping
  metadata             Metadata field index (requires --target and --key)
  account-asset        Account asset-presence index (the 'has asset' account filter)

Examples:
  ledgerctl indexes drop --ledger my-ledger --type address
  ledgerctl indexes drop --ledger my-ledger --type metadata --target account --key category
  ledgerctl indexes drop --ledger my-ledger --type account-asset`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("type", "", "Index type: address, source-address, destination-address, metadata")
	cmdutil.RegisterEnumCompletion(cmd, "type", indexTypeOptions...)
	cmd.Flags().String("target", "", "Target type for metadata index: account, transaction, or ledger")
	cmdutil.RegisterEnumCompletion(cmd, "target", cmdutil.TargetTypeOptions()...)
	cmd.Flags().String("key", "", "Metadata key name (for metadata index)")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}
