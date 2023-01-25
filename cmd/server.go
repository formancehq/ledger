package cmd

import "github.com/spf13/cobra"

func NewServer() *cobra.Command {
	return &cobra.Command{
		Use: "server",
	}
}
