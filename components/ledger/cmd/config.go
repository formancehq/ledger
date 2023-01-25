package cmd

import "github.com/spf13/cobra"

func NewConfig() *cobra.Command {
	return &cobra.Command{
		Use: "config",
	}
}
