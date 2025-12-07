package main

import (
	"fmt"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger-v3-poc/internal/application"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	rootCmd := newRootCommand()
	service.Execute(rootCmd)
}

func newRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ledger-v3-poc",
		Short: "Ledger v3 POC with Raft cluster",
		Long:  "A proof of concept for Ledger v3 with Raft consensus cluster",
		RunE:  runServer,
	}

	// Add standard service flags
	service.AddFlags(rootCmd.Flags())

	// Add OpenTelemetry flags from go-libs
	otlp.AddFlags(rootCmd.Flags())
	otlptraces.AddFlags(rootCmd.Flags())

	// Add application-specific flags
	rootCmd.Flags().Uint64("node-id", 0, "Numeric node ID for this instance (must be non-zero)")
	rootCmd.Flags().String("bind-addr", "127.0.0.1:8888", "Address to bind to")
	rootCmd.Flags().String("advertise-addr", "", "Address to advertise to other nodes (defaults to bind-addr)")
	rootCmd.Flags().String("data-dir", "./data", "Data directory for Raft")
	rootCmd.Flags().StringSlice("peers", []string{}, "Initial peer list (comma-separated, format: <id>/<address>, e.g., \"1/node-1:8888,2/node-2:8888\")")
	rootCmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster (only set on the first node)")
	rootCmd.Flags().Int("grpc-port", 8000, "gRPC server port (for leader)")
	rootCmd.Flags().Int("http-port", 9000, "HTTP server port")
	rootCmd.Flags().String("storage-type", "sqlite", "Storage type: 'sqlite' or 'file'")
	rootCmd.Flags().String("sqlite-dsn", "file:./data/ledger.db?cache=shared&mode=rwc", "SQLite DSN connection string (required when storage-type is 'sqlite')")
	rootCmd.Flags().String("storage-file-path", "./data/logs.jsonl", "Path to log file (required when storage-type is 'file')")
	rootCmd.Flags().Uint64("snapshot-threshold", 0, "Number of logs before triggering a snapshot (0 = use Raft default)")
	rootCmd.Flags().Duration("snapshot-interval", 0, "Minimum interval between snapshots (0 = use Raft default, e.g., 30s)")

	return rootCmd
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	// Set default service name if not provided via flags
	serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
	if serviceName == "" {
		// Set default service name based on node ID
		defaultServiceName := fmt.Sprintf("ledger-v3-poc-node-%d", cfg.NodeID)
		if err := cmd.Flags().Set(otlp.OtelServiceNameFlag, defaultServiceName); err != nil {
			return fmt.Errorf("setting default service name: %w", err)
		}
	}

	// Create fx application options
	opts := []fx.Option{
		// Provide configuration
		fx.Provide(func() *config.Config {
			return cfg
		}),
		// Add OpenTelemetry modules from go-libs (using flags)
		otlp.FXModuleFromFlags(cmd, otlp.WithServiceVersion(fmt.Sprintf("%s-%s", version, commit))),
		otlptraces.FXModuleFromFlags(cmd),
		// Provide application module
		application.Module(),
	}

	// Create service app
	app := service.New(os.Stdout, opts...)

	// Run the application (handles startup, signal handling, and graceful shutdown)
	return app.Run(cmd)
}

func loadConfig(cmd *cobra.Command) (*config.Config, error) {
	cfg := &config.Config{}

	// Helper function to get string value from flag (env vars are bound automatically by service.BindEnvToCommand)
	getString := func(flagName, defaultValue string) string {
		if val, _ := cmd.Flags().GetString(flagName); val != "" {
			return val
		}
		return defaultValue
	}

	// Helper function to get uint64 value from flag
	getUint64 := func(flagName string, defaultValue uint64) uint64 {
		if val, _ := cmd.Flags().GetUint64(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	// Helper function to get int value from flag
	getInt := func(flagName string, defaultValue int) int {
		if val, _ := cmd.Flags().GetInt(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	// Helper function to get bool value from flag
	getBool := func(flagName string, defaultValue bool) bool {
		if val, _ := cmd.Flags().GetBool(flagName); val {
			return val
		}
		return defaultValue
	}

	// Helper function to get string slice from flag
	getStringSlice := func(flagName string) []string {
		if val, _ := cmd.Flags().GetStringSlice(flagName); len(val) > 0 {
			return val
		}
		return []string{}
	}

	// Helper function to get duration from flag
	getDuration := func(flagName string, defaultValue time.Duration) time.Duration {
		if val, _ := cmd.Flags().GetDuration(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	cfg.NodeID = getUint64("node-id", 0)
	cfg.BindAddr = getString("bind-addr", "127.0.0.1:8888")
	cfg.AdvertiseAddr = getString("advertise-addr", "")
	cfg.DataDir = getString("data-dir", "./data")
	cfg.Peers = getStringSlice("peers")
	cfg.Debug = getBool("debug", false)
	cfg.Bootstrap = getBool("bootstrap", false)
	cfg.GRPCPort = getInt("grpc-port", 8000)
	cfg.HTTPPort = getInt("http-port", 9000)
	cfg.StorageType = getString("storage-type", "sqlite")
	cfg.SQLiteDSN = getString("sqlite-dsn", "file:./data/ledger.db?cache=shared&mode=rwc")
	cfg.StorageFilePath = getString("storage-file-path", "./data/logs.jsonl")
	cfg.SnapshotThreshold = getUint64("snapshot-threshold", 0)
	cfg.SnapshotInterval = getDuration("snapshot-interval", 0)

	return cfg, nil
}
