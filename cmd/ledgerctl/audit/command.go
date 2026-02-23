package audit

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "audit",
		Aliases: []string{"a"},
		Short:   "Audit log operations",
		Long:    "Commands for viewing the replicated audit log via gRPC",
	}
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewEnableCommand())
	cmd.AddCommand(NewDisableCommand())
	return cmd
}
