package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/formancehq/ledger-v3-poc/internal/application"
	"github.com/formancehq/ledger-v3-poc/internal/config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
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
	Use:   "ledger-v3-poc",
	Short: "Ledger v3 POC with Raft cluster",
	Long:  "A proof of concept for Ledger v3 with Raft consensus cluster",
	RunE:  runServer,
}

func init() {
	rootCmd.Flags().String("config", "", "Path to configuration file (supports yaml, json, toml)")
	rootCmd.Flags().Uint64("node-id", 0, "Numeric node ID for this instance (must be non-zero)")
	rootCmd.Flags().String("bind-addr", "127.0.0.1:8888", "Address to bind to")
	rootCmd.Flags().String("advertise-addr", "", "Address to advertise to other nodes (defaults to bind-addr)")
	rootCmd.Flags().String("data-dir", "./data", "Data directory for Raft")
	rootCmd.Flags().StringSlice("peers", []string{}, "Initial peer list (comma-separated, format: <id>/<address>, e.g., \"1/node-1:8888,2/node-2:8888\")")
	rootCmd.Flags().Bool("debug", false, "Enable debug logging")
	rootCmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster (only set on the first node)")
	rootCmd.Flags().Int("grpc-port", 8000, "gRPC server port (for leader)")
	rootCmd.Flags().Int("http-port", 9000, "HTTP server port")
	rootCmd.Flags().String("storage-type", "sqlite", "Storage type: 'sqlite' or 'file'")
	rootCmd.Flags().String("sqlite-dsn", "file:./data/ledger.db?cache=shared&mode=rwc", "SQLite DSN connection string (required when storage-type is 'sqlite')")
	rootCmd.Flags().String("storage-file-path", "./data/logs.jsonl", "Path to log file (required when storage-type is 'file')")
	rootCmd.Flags().Uint64("snapshot-threshold", 0, "Number of logs before triggering a snapshot (0 = use Raft default)")
	rootCmd.Flags().Duration("snapshot-interval", 0, "Minimum interval between snapshots (0 = use Raft default, e.g., 30s)")
}

func runServer(cmd *cobra.Command, args []string) error {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	var logger *zap.Logger
	if cfg.Debug {
		logger, err = zap.NewDevelopment()
	} else {
		logger, err = zap.NewProduction()
	}
	if err != nil {
		return fmt.Errorf("creating logger: %w", err)
	}
	defer func() {
		// Sync can fail in Docker containers when using /dev/stderr
		// Ignore the error to avoid panics
		_ = logger.Sync()
	}()

	logger.Info("Starting Ledger v3 POC",
		zap.String("version", version),
		zap.String("commit", commit),
		zap.Uint64("node-id", cfg.NodeID),
		zap.String("bind-addr", cfg.BindAddr),
		zap.String("advertise-addr", cfg.AdvertiseAddr),
	)

	// Get context from Cobra
	ctx := cmd.Context()

	// Create application
	app := application.New(cfg, logger)

	// Start application
	if err := app.Start(ctx); err != nil {
		return fmt.Errorf("starting application: %w", err)
	}

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	<-sigChan
	logger.Info("Shutting down...")

	return app.Shutdown()
}

func loadConfig(cmd *cobra.Command) (*config.Config, error) {
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Check if config file is specified
	configFile, _ := cmd.Flags().GetString("config")
	if configFile != "" {
		// Use explicit config file (Viper will auto-detect format from extension)
		viper.SetConfigFile(configFile)
		if err := viper.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("reading config file: %w", err)
		}
	} else {
		// Look for config file in common locations
		// Viper will try multiple formats automatically (yaml, json, toml, etc.)
		viper.SetConfigName("config")
		viper.AddConfigPath(".")
		viper.AddConfigPath("./config")
		viper.AddConfigPath("$HOME/.ledger-v3-poc")
		// Config file is optional, ignore error if not found
		_ = viper.ReadInConfig()
	}

	// Bind flags to viper (flags override config file and env vars)
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return nil, fmt.Errorf("binding flags: %w", err)
	}

	cfg := &config.Config{
		NodeID:            viper.GetUint64("node-id"),
		BindAddr:          viper.GetString("network.bind-addr"),
		AdvertiseAddr:     viper.GetString("advertise-addr"),
		DataDir:           viper.GetString("network.data-dir"),
		Peers:             viper.GetStringSlice("peers"),
		Debug:             viper.GetBool("logging.debug"),
		Bootstrap:         viper.GetBool("bootstrap"),
		GRPCPort:          viper.GetInt("server.grpc-port"),
		HTTPPort:          viper.GetInt("server.http-port"),
		StorageType:       viper.GetString("storage.type"),
		SQLiteDSN:         viper.GetString("storage.sqlite.dsn"),
		StorageFilePath:   viper.GetString("storage.file.path"),
		SnapshotThreshold: viper.GetUint64("snapshot-threshold"),  // Can be set via flag or config file
		SnapshotInterval:  viper.GetDuration("snapshot-interval"), // Can be set via flag or config file
	}

	// Also check hierarchical config keys if flags weren't set
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = viper.GetUint64("raft.snapshot.threshold")
	}
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = viper.GetDuration("raft.snapshot.interval")
	}

	return cfg, nil
}
