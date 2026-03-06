package accounts

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the accounts parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "accounts",
		Aliases: []string{"account", "acc", "a"},
		Short:   "Manage accounts",
		Long:    "Commands for managing and viewing accounts in a ledger",
	}

	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewSetMetadataCommand())
	cmd.AddCommand(NewDeleteMetadataCommand())
	cmd.AddCommand(NewAnalyzeCommand())
	cmd.AddCommand(NewAggregateVolumesCommand())

	return cmd
}
