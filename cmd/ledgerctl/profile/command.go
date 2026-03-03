package profile

import "github.com/spf13/cobra"

// NewCommand creates the profile parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "profile",
		Aliases: []string{"profiles", "prof"},
		Short:   "Manage connection profiles",
		Long:    "Commands for managing named connection profiles (server address and TLS settings)",
	}

	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewUseCommand())
	cmd.AddCommand(NewDeleteCommand())
	cmd.AddCommand(NewShowCommand())

	return cmd
}
