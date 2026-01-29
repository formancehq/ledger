package main

import (
	"github.com/spf13/cobra"
)

// newLedgersCommand creates the ledgers parent command.
func newLedgersCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ledgers",
		Short: "Manage ledgers",
		Long:  "Commands for managing ledgers via gRPC",
	}

	cmd.AddCommand(newLedgersListCommand())
	cmd.AddCommand(newLedgersGetCommand())
	cmd.AddCommand(newLedgersCreateCommand())

	return cmd
}
