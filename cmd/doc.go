package cmd

import (
	"github.com/spf13/cobra"
)

func NewDocCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use: "doc",
	}
	cmd.AddCommand(
		NewDocFlagsCommand(),
		NewDocEventsCommand(),
	)
	return cmd
}
