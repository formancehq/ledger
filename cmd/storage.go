package cmd

import "github.com/spf13/cobra"

func NewStorage() *cobra.Command {
	return &cobra.Command{
		Use: "storage",
	}
}
