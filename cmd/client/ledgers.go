package main

import (
	"github.com/spf13/cobra"
)

var ledgersCmd = &cobra.Command{
	Use:          "ledgers",
	Short:        "Manage ledgers",
	Long:         "Commands for managing ledgers",
	SilenceUsage: true,
}

func initLedgers() {
	ledgersCmd.AddCommand(ledgersCreateCmd)
	ledgersCmd.AddCommand(ledgersListCmd)
	ledgersCmd.AddCommand(ledgersGetCmd)
}
