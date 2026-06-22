package chapters

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "chapters",
		Aliases: []string{"chapter", "ch"},
		Short:   "Manage chapters",
		Long:    "Commands for managing accounting chapters via gRPC",
	}

	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewCloseCommand())
	cmd.AddCommand(NewArchiveCommand())
	cmd.AddCommand(NewSetScheduleCommand())
	cmd.AddCommand(NewDeleteScheduleCommand())
	cmd.AddCommand(NewGetScheduleCommand())

	return cmd
}
