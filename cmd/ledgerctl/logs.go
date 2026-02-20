package main

import (
	"github.com/spf13/cobra"
)

// newLogsCommand creates the logs parent command.
func newLogsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "logs",
		Aliases: []string{"log"},
		Short:   "System log operations",
		Long:    "Commands for viewing system logs via gRPC",
	}

	cmd.AddCommand(newLogsListCommand())

	return cmd
}
