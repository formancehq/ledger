package defaults

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "defaults" parent command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "defaults",
		Aliases: []string{"def"},
		Short:   "Manage cluster-scoped LedgerDefaults resources",
	}

	cmd.AddCommand(
		newListCommand(opts),
		newGetCommand(opts),
		newCreateCommand(opts),
		newEditCommand(opts),
		newDeleteCommand(opts),
	)

	return cmd
}
