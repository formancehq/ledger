package ledgers

import (
	"github.com/spf13/cobra"
)

// NewCommand creates the ledgers parent command.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "ledgers",
		Aliases: []string{"ledger", "lg"},
		Short:   "Manage ledgers",
		Long:    "Commands for managing ledgers via gRPC",
	}

	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewCreateCommand())
	cmd.AddCommand(NewDeleteCommand())
	cmd.AddCommand(NewSetMetadataTypeCommand())
	cmd.AddCommand(NewRemoveMetadataTypeCommand())
	cmd.AddCommand(NewGetSchemaCommand())

	return cmd
}
