package agents

import (
	"github.com/spf13/cobra"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "agents" parent command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "agents",
		Aliases: []string{"agent", "ag"},
		Short:   "Manage cluster-scoped LedgerClusterAgent resources",
	}

	cmd.AddCommand(
		newListCommand(opts),
		newGetCommand(opts),
		newCreateCommand(opts),
		newDeleteCommand(opts),
		newGetKeyCommand(opts),
	)

	return cmd
}
