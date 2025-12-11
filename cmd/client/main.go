package main

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	version = "dev"
)

func main() {
	// Load .env file if it exists (ignore errors if file doesn't exist)
	_ = godotenv.Load()

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:          "ledger-poc-client",
	Short:        "Client for interacting with Ledger v3 POC Raft cluster",
	Long:         "A CLI client for interacting with the Ledger v3 POC Raft cluster operations",
	SilenceUsage: true,
	Version:      version,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// Read values from Viper and set them to the variables
		// Viper automatically reads from flags, env vars, and config files
		// Env vars have the highest priority
		serverURL = viper.GetString("server")
		debugMode = viper.GetBool("debug")
	},
}

func init() {
	// Configure Viper
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	rootCmd.PersistentFlags().StringVar(&serverURL, "server", "http://localhost:9000", "Server URL (e.g., http://localhost:9000)")
	rootCmd.PersistentFlags().BoolVar(&debugMode, "debug", false, "Enable debug mode to display HTTP requests and responses")

	// Bind flags to Viper (for automatic env var mapping)
	_ = viper.BindPFlag("server", rootCmd.PersistentFlags().Lookup("server"))
	_ = viper.BindPFlag("debug", rootCmd.PersistentFlags().Lookup("debug"))

	// Initialize sub-commands
	initBuckets()
	initLedgers()

	// Add commands to root
	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(clusterStateCmd)
	rootCmd.AddCommand(bucketsCmd)
	rootCmd.AddCommand(ledgersCmd)
}
