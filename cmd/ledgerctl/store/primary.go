package store

import (
	"github.com/spf13/cobra"
)

// NewPrimaryCommand creates the store primary parent command.
func NewPrimaryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "primary",
		Aliases: []string{"p"},
		Short:   "Primary store operations",
		Long:    "Commands for the primary Pebble store (Raft log / data)",
	}

	cmd.AddCommand(NewPrimaryMetricsCommand())
	cmd.AddCommand(NewPrimaryCompactCommand())

	return cmd
}
