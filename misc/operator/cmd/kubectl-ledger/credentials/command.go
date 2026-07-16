package credentials

import (
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
)

// NewCommand returns the "credentials" parent command.
func NewCommand(opts *cmdutil.Options) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "credentials",
		Aliases: []string{"credential", "creds", "cred"},
		Short:   "Manage cluster-scoped Credentials resources",
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
