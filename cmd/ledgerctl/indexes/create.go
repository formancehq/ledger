package indexes

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewCreateCommand creates the indexes create command.
func NewCreateCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "create [flags]",
		Aliases: []string{"c"},
		Short:   "Create an index on a ledger",
		Long: `Create an opt-in index on a ledger.

Index types:
  address              Account→transaction mapping (any role)
  source-address       Source account→transaction mapping
  destination-address  Destination account→transaction mapping
  metadata             Metadata field index (requires --target and --key)
  reference            Transaction reference index (exact-match filter)
  timestamp            Transaction timestamp index (range filter)
  inserted-at          Transaction inserted_at (creation date) index (range filter)
  reverted-at          Transaction reverted_at (revert date) index (range filter)
  log-ledger           Per-ledger log index (enables filtered log listing)
  account-asset        Account asset-presence index (enables the 'has asset' account filter)

Examples:
  ledgerctl indexes create --ledger my-ledger --type address
  ledgerctl indexes create --ledger my-ledger --type source-address
  ledgerctl indexes create --ledger my-ledger --type metadata --target account --key category
  ledgerctl indexes create --ledger my-ledger --type reference
  ledgerctl indexes create --ledger my-ledger --type timestamp
  ledgerctl indexes create --ledger my-ledger --type inserted-at
  ledgerctl indexes create --ledger my-ledger --type reverted-at
  ledgerctl indexes create --ledger my-ledger --type log-ledger
  ledgerctl indexes create --ledger my-ledger --type account-asset`,
		Args:              cobra.NoArgs,
		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().String("type", "", "Index type: address, source-address, destination-address, metadata")
	cmdutil.RegisterEnumCompletion(cmd, "type", indexTypeOptions...)
	cmd.Flags().String("target", "", "Target type for metadata index: account, transaction, or ledger")
	cmdutil.RegisterEnumCompletion(cmd, "target", cmdutil.TargetTypeOptions()...)
	cmd.Flags().String("key", "", "Metadata key name (for metadata index)")
	cmd.Flags().String("idempotency-key", "", "Batch idempotency key, recorded in the audit and included in the signature")
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}
