package main

import (
	"github.com/formancehq/go-libs/v3/service"
	"github.com/spf13/cobra"
)

func main() {
	service.Execute(newRootCommand())
}

// newRootCommand creates the root command for the ledger client CLI.
func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ledgerctl",
		Short: "Ledger v3 CLI client",
		Long:  "Command-line client for interacting with Ledger v3 servers via gRPC",
	}

	// Add persistent flags for server connection
	rootCmd.PersistentFlags().String("server", "localhost:8888", "gRPC server address")
	rootCmd.PersistentFlags().Bool("insecure", false, "Use insecure connection (no TLS)")

	// Add subcommands
	rootCmd.AddCommand(newLedgersCommand())
	rootCmd.AddCommand(newStoreCommand())

	return rootCmd
}
