package kv

import "github.com/spf13/cobra"

// NewCommand creates the ledger-scoped KV command group.
func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "kv",
		Short: "Manage opaque ledger KV values",
	}
	cmd.AddCommand(NewGetCommand(), NewPutCommand(), NewDeleteCommand())

	return cmd
}
