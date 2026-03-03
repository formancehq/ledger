package numscripts

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "numscripts",
		Aliases: []string{"numscript", "ns"},
		Short:   "Manage numscript library",
		Long:    "Commands for managing the per-ledger numscript library (save, get, list, delete)",
	}

	cmd.PersistentFlags().String("ledger", "", "Ledger name (interactive selection if omitted)")

	cmd.AddCommand(NewSaveCommand())
	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewDeleteCommand())

	return cmd
}
