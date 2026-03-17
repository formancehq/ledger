package provision

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "provision",
		Short: "Provision ledger scenarios",
		Long:  "Commands for running pre-built provisioning scenarios against a cluster",
	}

	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewRunCommand())

	return cmd
}
