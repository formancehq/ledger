package cluster

import "github.com/spf13/cobra"

// NewCommand creates the cluster parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "cluster",
		Aliases: []string{"cl"},
		Short:   "Manage cluster",
		Long:    "Commands for managing and inspecting the Raft cluster",
	}

	cmd.AddCommand(NewStatusCommand())
	cmd.AddCommand(NewDiskUsageCommand())
	cmd.AddCommand(NewTransferLeaderCommand())
	cmd.AddCommand(NewAddLearnerCommand())
	cmd.AddCommand(NewPromoteLearnerCommand())
	cmd.AddCommand(NewRemoveNodeCommand())
	cmd.AddCommand(NewMaintenanceCommand())

	return cmd
}
