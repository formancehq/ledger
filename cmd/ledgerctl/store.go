package main

import (
	"github.com/spf13/cobra"
)

// newStoreCommand creates the store parent command.
func newStoreCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "store",
		Aliases: []string{"s"},
		Short:   "Store operations",
		Long:    "Commands for store-level operations via gRPC",
	}

	cmd.AddCommand(newStoreMetricsCommand())
	cmd.AddCommand(newStoreCheckCommand())

	return cmd
}
