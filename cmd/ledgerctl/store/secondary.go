package store

import (
	"github.com/spf13/cobra"
)

// NewSecondaryCommand creates the store secondary parent command.
func NewSecondaryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "secondary",
		Aliases: []string{"sec"},
		Short:   "Secondary store operations",
		Long:    "Commands for the secondary RocksDB store (read index)",
	}

	cmd.AddCommand(NewSecondaryMetricsCommand())
	cmd.AddCommand(NewSecondaryCompactCommand())

	return cmd
}
