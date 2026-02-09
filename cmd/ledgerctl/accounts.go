package main

import (
	"github.com/spf13/cobra"
)

// newAccountsCommand creates the accounts parent command.
func newAccountsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "accounts",
		Aliases: []string{"account", "acc", "a"},
		Short:   "Manage accounts",
		Long:    "Commands for managing and viewing accounts in a ledger",
	}

	cmd.AddCommand(newAccountsGetCommand())
	cmd.AddCommand(newAccountsSetMetadataCommand())
	cmd.AddCommand(newAccountsDeleteMetadataCommand())

	return cmd
}
