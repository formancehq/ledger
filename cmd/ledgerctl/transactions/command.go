package transactions

import "github.com/spf13/cobra"

// NewCommand creates the transactions parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "transactions",
		Aliases: []string{"transaction", "tx", "t"},
		Short:   "Manage transactions",
		Long:    "Commands for managing and viewing transactions in a ledger",
	}

	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewRevertCommand())
	cmd.AddCommand(NewSetMetadataCommand())
	cmd.AddCommand(NewDeleteMetadataCommand())
	cmd.AddCommand(NewAnalyzeCommand())

	return cmd
}
