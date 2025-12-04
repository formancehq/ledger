package main

import (
	"fmt"
	"os"

	"openapi"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "ledger-client",
	Short: "Client for interacting with Ledger v3 POC Raft cluster",
	Long:  "A CLI client for interacting with the Ledger v3 POC Raft cluster operations",
}

var (
	serverURL string
)

func init() {
	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9000", "Server URL (e.g., http://localhost:9000)")

	rootCmd.AddCommand(snapshotCmd)
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Create a Raft cluster snapshot",
	Long:  "Forces the creation of a Raft cluster snapshot on the leader node",
	RunE:  runSnapshot,
}

func runSnapshot(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Create SDK instance with custom server URL
	sdk := openapi.New(
		openapi.WithServerURL(serverURL),
	)

	// Call the snapshot endpoint
	res, err := sdk.Cluster.CreateSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("snapshot failed: %w", err)
	}

	if res.SnapshotResponse != nil && res.SnapshotResponse.Data != nil && res.SnapshotResponse.Data.Message != nil {
		fmt.Println(*res.SnapshotResponse.Data.Message)
	} else {
		fmt.Println("Snapshot created successfully")
	}

	return nil
}
