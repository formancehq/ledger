package config

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "config" parent command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "Manage Ledger configuration",
	}

	cmd.AddCommand(
		newViewCommand(opts),
		newEditCommand(opts),
	)

	return cmd
}
