package accounttypes

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the account-types parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "account-types",
		Aliases: []string{"at", "types"},
		Short:   "Manage account types",
		Long:    "Commands for managing account types (pattern-based account validation)",
	}

	cmd.AddCommand(NewAddCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewRemoveCommand())
	cmd.AddCommand(NewSetDefaultEnforcementCommand())
	cmd.AddCommand(NewMigrateCommand())

	return cmd
}
