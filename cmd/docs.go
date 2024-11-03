package cmd

import "github.com/spf13/cobra"

func NewDocsCommand() *cobra.Command {
	ret := &cobra.Command{
		Use: "docs",
	}
	ret.AddCommand(NewDocFlagsCommand())
	ret.AddCommand(NewDocEventsCommand())

	return ret
}
