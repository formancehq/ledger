package queries

import "github.com/spf13/cobra"

// NewCommand creates the queries parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "queries",
		Aliases: []string{"query", "pq"},
		Short:   "Manage prepared queries",
		Long:    "Commands for creating, listing, updating, deleting, and executing prepared queries",
	}

	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewUpdateCommand())
	cmd.AddCommand(NewDeleteCommand())
	cmd.AddCommand(NewExecuteCommand())

	return cmd
}
