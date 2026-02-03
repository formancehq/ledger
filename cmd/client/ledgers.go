package main

import (
	"github.com/spf13/cobra"
)

// newLedgersCommand creates the ledgers parent command.
func newLedgersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ledgers",
		Aliases: []string{"ledger", "lg"},
		Short:   "Manage ledgers",
		Long:    "Commands for managing ledgers via gRPC",
	}

	cmd.AddCommand(newLedgersListCommand())
	cmd.AddCommand(newLedgersGetCommand())
	cmd.AddCommand(newLedgersCreateCommand())
	cmd.AddCommand(newLedgersDeleteCommand())

	return cmd
}
