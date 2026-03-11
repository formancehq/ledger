package store

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the store parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "store",
		Aliases: []string{"s"},
		Short:   "Store operations",
		Long:    "Commands for store-level operations via gRPC",
	}

	cmd.AddCommand(NewMetricsCommand())
	cmd.AddCommand(NewCheckCommand())
	cmd.AddCommand(NewBackupCommand())
	cmd.AddCommand(NewBootstrapCommand())
	cmd.AddCommand(NewRebuildIndexesCommand())
	cmd.AddCommand(NewCompactCommand())
	cmd.AddCommand(NewCompactReadIndexCommand())
	cmd.AddCommand(NewReadIndexMetricsCommand())
	cmd.AddCommand(NewCheckpointCommand())

	return cmd
}
