package querycheckpoint

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the query-checkpoint parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "query-checkpoint",
		Aliases: []string{"qcp"},
		Short:   "Query checkpoint operations",
		Long:    "Commands for managing query checkpoints (coordinated snapshots of the main store and read index for point-in-time queries).",
	}

	cmd.AddCommand(newCreateCommand())
	cmd.AddCommand(newDeleteCommand())
	cmd.AddCommand(newListCommand())
	cmd.AddCommand(newInfoCommand())

	return cmd
}
