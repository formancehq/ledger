package main

import (
	"github.com/spf13/cobra"
)

// newClusterCommand creates the cluster parent command.
func newClusterCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "cluster",
		Aliases: []string{"cl"},
		Short:   "Manage cluster",
		Long:    "Commands for managing and inspecting the Raft cluster",
	}

	cmd.AddCommand(newClusterStatusCommand())
	cmd.AddCommand(newClusterDiskUsageCommand())
	cmd.AddCommand(newClusterTransferLeaderCommand())

	return cmd
}
