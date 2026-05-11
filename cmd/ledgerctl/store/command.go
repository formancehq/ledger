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

	cmd.AddCommand(NewPrimaryCommand())
	cmd.AddCommand(NewSecondaryCommand())
	cmd.AddCommand(NewCheckCommand())
	cmd.AddCommand(NewBootstrapCommand())
	cmd.AddCommand(NewRebuildIndexesCommand())
	cmd.AddCommand(NewCheckpointCommand())
	cmd.AddCommand(NewDumpCommand())
	cmd.AddCommand(NewBackupCommand())
	cmd.AddCommand(NewIncrementalBackupCommand())

	return cmd
}
