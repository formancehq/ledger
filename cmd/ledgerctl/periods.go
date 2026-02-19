package main

import (
	"github.com/spf13/cobra"
)

// newPeriodsCommand creates the periods parent command.
func newPeriodsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "periods",
		Aliases: []string{"period", "pd"},
		Short:   "Manage periods",
		Long:    "Commands for managing accounting periods via gRPC",
	}

	cmd.AddCommand(newPeriodsListCommand())
	cmd.AddCommand(newPeriodsCloseCommand())
	cmd.AddCommand(newPeriodsArchiveCommand())

	return cmd
}
