package backup

import (
	"github.com/spf13/cobra"

	"github.com/formance/ledger/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "backup" parent command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "backup",
		Aliases: []string{"backups", "bk"},
		Short:   "Manage LedgerBackup resources and trigger manual backup runs",
	}

	cmd.AddCommand(
		newListCommand(opts),
		newTriggerCommand(opts),
		newRunsCommand(opts),
	)

	return cmd
}
