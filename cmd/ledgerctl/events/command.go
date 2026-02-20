package events

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "events",
		Aliases: []string{"sinks"},
		Short:   "Manage event sinks",
		Long:    "Commands for managing event sinks (NATS, ClickHouse) via gRPC",
	}

	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewAddSinkCommand())
	cmd.AddCommand(NewRemoveSinkCommand())

	return cmd
}
