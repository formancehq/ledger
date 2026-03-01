package logs

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "logs",
		Aliases: []string{"log"},
		Short:   "System log operations",
		Long:    "Commands for viewing system logs via gRPC",
	}
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewGetCommand())
	return cmd
}
