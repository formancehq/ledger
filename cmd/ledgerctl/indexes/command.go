package indexes

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the indexes parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "indexes",
		Aliases: []string{"index", "idx"},
		Short:   "Manage indexes",
		Long:    "Commands for creating, dropping, listing, and inspecting indexes on ledgers",
	}

	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewDropCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewInspectCommand())

	return cmd
}
