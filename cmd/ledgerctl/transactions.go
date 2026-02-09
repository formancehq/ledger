package main

import (
	"github.com/spf13/cobra"
)

// newTransactionsCommand creates the transactions parent command.
func newTransactionsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "transactions",
		Aliases: []string{"transaction", "tx", "t"},
		Short:   "Manage transactions",
		Long:    "Commands for managing and viewing transactions in a ledger",
	}

	cmd.AddCommand(newTransactionsGetCommand())
	cmd.AddCommand(newTransactionsCreateCommand())
	cmd.AddCommand(newTransactionsListCommand())
	cmd.AddCommand(newTransactionsRevertCommand())
	cmd.AddCommand(newTransactionsSetMetadataCommand())
	cmd.AddCommand(newTransactionsDeleteMetadataCommand())

	return cmd
}
