package numscripts

import "github.com/spf13/cobra"

func NewCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "numscripts",
		Aliases: []string{"numscript", "ns"},
		Short:   "Manage numscript library",
		Long:    "Commands for executing numscripts and managing the per-ledger library (execute, save, get, list, versions)",
	}

	cmd.AddCommand(NewSaveCommand())
	cmd.AddCommand(NewGetCommand())
	cmd.AddCommand(NewListCommand())
	cmd.AddCommand(NewVersionsCommand())
	cmd.AddCommand(NewExecuteCommand())

	return cmd
}
