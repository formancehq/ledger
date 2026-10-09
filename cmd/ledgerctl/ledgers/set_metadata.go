package ledgers

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
)

// NewSetMetadataCommand creates the ledgers set-metadata command.
func NewSetMetadataCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-metadata [flags]",
		Aliases: []string{"set-meta", "sm"},
		Short:   "Set metadata on a ledger",
		Long: `Set metadata key-value pairs on a ledger via gRPC.

Metadata is provided as key=value pairs using the --metadata flag.
Multiple metadata entries can be set at once.

If --ledger is not provided and only one ledger exists, it will be used automatically.

Examples:
  ledgerctl ledgers set-metadata --ledger my-ledger -m environment=production -m team=payments
  ledgerctl ledgers sm --ledger my-ledger -m region=eu-west-1
  ledgerctl ledgers set-metadata  # Interactive mode`,
		Args: cobra.NoArgs,

		ValidArgsFunction: cobra.NoFileCompletions,
	}

	cmd.Flags().String("ledger", "", "Name of the ledger")
	cmd.Flags().StringArrayP("metadata", "m", nil, "Metadata key=value pairs (can be repeated)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}
