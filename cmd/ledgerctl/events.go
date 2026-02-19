package main

import (
	"github.com/spf13/cobra"
)

// newEventsCommand creates the events parent command.
func newEventsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "events",
		Aliases: []string{"sinks"},
		Short:   "Manage event sinks",
		Long:    "Commands for managing event sinks (NATS, etc.) via gRPC",
	}

	cmd.AddCommand(newEventsListCommand())
	cmd.AddCommand(newEventsAddSinkCommand())
	cmd.AddCommand(newEventsRemoveSinkCommand())

	return cmd
}
