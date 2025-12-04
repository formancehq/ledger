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
	rootCmd.Flags().String("node-id", "", "Node ID for this instance")
	rootCmd.Flags().String("bind-addr", "127.0.0.1:8888", "Address to bind to")
	rootCmd.Flags().String("advertise-addr", "", "Address to advertise to other nodes (defaults to bind-addr)")
	rootCmd.Flags().String("data-dir", "./data", "Data directory for Raft")
	rootCmd.Flags().StringSlice("peers", []string{}, "Initial peer addresses (comma-separated)")
	rootCmd.Flags().Bool("debug", false, "Enable debug logging")
	rootCmd.Flags().Bool("bootstrap", false, "Bootstrap the cluster (only set on the first node)")
	rootCmd.Flags().Int("grpc-port", 8000, "gRPC server port (for leader)")
	rootCmd.Flags().Int("http-port", 9000, "HTTP server port")
	rootCmd.Flags().String("sqlite-dsn", "file:./data/ledger.db?cache=shared&mode=rwc", "SQLite DSN connection string")
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
		if err := logger.Sync(); err != nil {
			panic(err)
		}
	}()

	logger.Info("Starting Ledger v3 POC",
		zap.String("version", version),
		zap.String("commit", commit),
		zap.String("node-id", cfg.NodeID),
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

	// Bind flags to viper
	if err := viper.BindPFlags(cmd.Flags()); err != nil {
		return nil, fmt.Errorf("binding flags: %w", err)
	}

	cfg := &config.Config{
		NodeID:        viper.GetString("node-id"),
		BindAddr:      viper.GetString("bind-addr"),
		AdvertiseAddr: viper.GetString("advertise-addr"),
		DataDir:       viper.GetString("data-dir"),
		Peers:         viper.GetStringSlice("peers"),
		Debug:         viper.GetBool("debug"),
		Bootstrap:     viper.GetBool("bootstrap"),
		GRPCPort:      viper.GetInt("grpc-port"),
		HTTPPort:      viper.GetInt("http-port"),
		SQLiteDSN:     viper.GetString("sqlite-dsn"),
	}

	return cfg, nil
}
