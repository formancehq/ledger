package periods

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "periods",
		Aliases: []string{"period", "pd"},
		Short:   "Manage periods",
		Long:    "Commands for managing accounting periods via gRPC",
	}

	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewCloseCommand())
	cmd.AddCommand(NewArchiveCommand())
	cmd.AddCommand(NewSetScheduleCommand())
	cmd.AddCommand(NewDeleteScheduleCommand())
	cmd.AddCommand(NewGetScheduleCommand())

	return cmd
}
