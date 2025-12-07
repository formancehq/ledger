package main

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:          "ledger-client",
	Short:        "Client for interacting with Ledger v3 POC Raft cluster",
	Long:         "A CLI client for interacting with the Ledger v3 POC Raft cluster operations",
	SilenceUsage: true,
	Version:      version,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9000", "Server URL (e.g., http://localhost:9000)")
	rootCmd.PersistentFlags().BoolVar(&debugMode, "debug", false, "Enable debug mode to display HTTP requests and responses")

	// Initialize sub-commands
	initBuckets()
	initLedgers()

	// Add commands to root
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(clusterStateCmd)
	rootCmd.AddCommand(bucketsCmd)
	rootCmd.AddCommand(ledgersCmd)
}
