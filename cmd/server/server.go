package server

import (
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger-v3-poc/internal/application"
	"github.com/formancehq/ledger-v3-poc/internal/pyroscope"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/tracesampling"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"
)

var (
	version = "dev"
	commit  = "unknown"
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ledger-v3-poc",
		Short: "Ledger v3 POC with Raft cluster",
		Long:  "A proof of concept for Ledger v3 with Raft consensus cluster",
	}

	rootCmd.AddCommand(NewRunCommand())

	return rootCmd
}

// NewRunCommand returns the run command for the ledger server.
// This is exported for use in integration tests.
func NewRunCommand() *cobra.Command {
	runCmd := &cobra.Command{
		Use:   "run",
		Short: "Run the ledger server",
		Long:  "Start the Ledger v3 POC server with Raft consensus cluster",
		RunE:  runServer,
	}

	// Add standard service flags
	service.AddFlags(runCmd.Flags())

	// Add OpenTelemetry flags from go-libs
	otlp.AddFlags(runCmd.Flags())
	otlptraces.AddFlags(runCmd.Flags())
	otlpmetrics.AddFlags(runCmd.Flags())
	addOtlpLogsFlags(runCmd.Flags())

	// Add Pyroscope profiling flags
	addPyroscopeFlags(runCmd.Flags())

	// Add trace sampling flags
	addTraceSamplingFlags(runCmd.Flags())

	// Add application-specific flags
	runCmd.Flags().Uint64("node-id", 0, "Numeric node ID for this instance (must be non-zero)")
	runCmd.Flags().String("bind-addr", "0.0.0.0:8888", "Address to bind to (grpc)")
	runCmd.Flags().String("advertise-addr", "", "Address to advertise to other nodes (defaults to bind-addr)")
	runCmd.Flags().String("wal-dir", "./wal", "WAL directory for Raft")
	runCmd.Flags().String("data-dir", "./data", "Data directory for application storage")
	runCmd.Flags().StringSlice("peers", []string{}, "Initial peer list (comma-separated, format: <id>/<address>, e.g., \"1/node-1:8888,2/node-2:8888\")")
	runCmd.Flags().Int("http-port", 9000, "HTTP server port")
	runCmd.Flags().Uint64("snapshot-threshold", 5000, "Number of logs before triggering a snapshot (0 = use Raft default)")
	runCmd.Flags().Duration("snapshot-interval", 0, "Minimum interval between snapshots (0 = use Raft default, e.g., 30s)")
	runCmd.Flags().Int("raft-election-tick", 10, "Election timeout in ticks (0 = use default 10)")
	runCmd.Flags().Int("raft-heartbeat-tick", 1, "Heartbeat interval in ticks (0 = use default 1)")
	runCmd.Flags().Uint64("raft-max-size-per-msg", 0, "Maximum size per message in bytes (0 = use default 1MB)")
	runCmd.Flags().Int("raft-max-inflight-msgs", 0, "Maximum number of in-flight messages (0 = use default 256)")
	runCmd.Flags().Duration("raft-tick-interval", 100*time.Millisecond, "Interval between Raft ticks (0 = use default 100ms)")
	runCmd.Flags().Uint64("raft-compaction-margin", 1000, "Minimum log entries between snapshots (0 = use default 1000)")
	runCmd.Flags().Int("raft-propose-queue-capacity", 0, "Capacity of the propose queue (0 = use default 100)")
	runCmd.Flags().IntSlice("raft-transport-reception-queues", []int{}, "Comma-separated list of reception queue capacities per priority (e.g., \"10,512,512,512,128\")")
	runCmd.Flags().IntSlice("raft-transport-send-queues", []int{}, "Comma-separated list of send queue capacities per priority (e.g., \"10,512,512,512,128\")")
	runCmd.Flags().String("storage-type", "", "Define storage type (sqlite-mattn, sqlite-modern, pebble)")

	return runCmd
}

func runServer(cmd *cobra.Command, _ []string) error {
	cfg, err := LoadConfig(cmd)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}
	fmt.Println(string(data))

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	// Set default service name if not provided via flags
	serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
	if serviceName == "" {
		// Set default service name based on node ID
		defaultServiceName := fmt.Sprintf("ledger-v3-poc-node-%d", cfg.RaftConfig.NodeID)
		if err := cmd.Flags().Set(otlp.OtelServiceNameFlag, defaultServiceName); err != nil {
			return fmt.Errorf("setting default service name: %w", err)
		}
	}

	logger, err := loggerFromFlags(cmd, map[string]any{
		"node-id": cfg.RaftConfig.NodeID,
	})
	if err != nil {
		return fmt.Errorf("creating logger: %w", err)
	}

	logger.Infof("GOMAXPROCS=%d NumCPU=%d", runtime.GOMAXPROCS(0), runtime.NumCPU())

	// Configure Pyroscope profiling
	pyroscopeCfg := pyroscopeConfigFromFlags(cmd)
	if pyroscopeCfg.Enabled && pyroscopeCfg.ApplicationName == "" {
		// Use the service name as the default application name
		serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
		pyroscopeCfg.ApplicationName = serviceName
	}
	// Add node ID as a tag
	if pyroscopeCfg.Enabled {
		if pyroscopeCfg.Tags == nil {
			pyroscopeCfg.Tags = make(map[string]string)
		}
		pyroscopeCfg.Tags["node_id"] = fmt.Sprintf("%d", cfg.RaftConfig.NodeID)
	}

	// Configure trace sampling
	traceSamplingCfg := traceSamplingConfigFromFlags(cmd)

	// Create fx application options
	opts := []fx.Option{
		// Provide configuration
		fx.Supply(*cfg),
		// Add OpenTelemetry modules from go-libs (using flags)
		otlp.FXModuleFromFlags(cmd, otlp.WithServiceVersion(fmt.Sprintf("%s-%s", version, commit))),
		otlptraces.FXModuleFromFlags(cmd),
		otlpmetrics.FXModuleFromFlags(cmd),
		// Add trace sampling module (wraps exporter with error-aware sampling)
		tracesampling.Module(traceSamplingCfg),
		// Add Pyroscope profiling module
		pyroscope.Module(pyroscopeCfg),
		// Provide application module
		application.Module(),
	}

	// Run the application (handles startup, signal handling, and graceful shutdown)
	return service.NewWithLogger(logger, opts...).Run(cmd)
}

func LoadConfig(cmd *cobra.Command) (*application.Config, error) {
	cfg := &application.Config{}

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

	// Helper function to get int slice from flag
	getIntSlice := func(flagName string) []int {
		if val, _ := cmd.Flags().GetIntSlice(flagName); len(val) > 0 {
			return val
		}
		return []int{}
	}

	// Helper function to get duration from flag
	getDuration := func(flagName string, defaultValue time.Duration) time.Duration {
		if val, _ := cmd.Flags().GetDuration(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	cfg.Debug = getBool("debug", false)
	cfg.HTTPPort = getInt("http-port", 9000)
	cfg.RaftConfig.NodeID = getUint64("node-id", 0)
	cfg.RaftConfig.BindAddr = getString("bind-addr", "127.0.0.1:8888")
	cfg.RaftConfig.AdvertiseAddr = getString("advertise-addr", "")
	cfg.RaftConfig.WalDir = getString("wal-dir", "./wal")
	cfg.DataDir = getString("data-dir", "./data")
	cfg.RaftConfig.Peers = make([]raft.Peer, 0)
	for _, peer := range getStringSlice("peers") {
		parts := strings.SplitN(peer, "/", 2)

		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %w", err)
		}

		cfg.RaftConfig.Peers = append(cfg.RaftConfig.Peers, raft.Peer{
			ID:      id,
			Address: parts[1],
		})
	}
	cfg.RaftConfig.SnapshotThreshold = getUint64("snapshot-threshold", 0)
	cfg.RaftConfig.SnapshotInterval = getDuration("snapshot-interval", 0)
	cfg.RaftConfig.ElectionTick = getInt("raft-election-tick", 0)
	cfg.RaftConfig.HeartbeatTick = getInt("raft-heartbeat-tick", 0)
	cfg.RaftConfig.MaxSizePerMsg = getUint64("raft-max-size-per-msg", 0)
	cfg.RaftConfig.MaxInflightMsgs = getInt("raft-max-inflight-msgs", 0)
	cfg.RaftConfig.TickInterval = getDuration("raft-tick-interval", 0)
	cfg.RaftConfig.CompactionMargin = getUint64("raft-compaction-margin", 1000)
	cfg.RaftConfig.ProposeQueueCapacity = getInt("raft-propose-queue-capacity", 0)
	cfg.StorageType = getString("storage-type", "pebble")

	// Parse transport reception queues
	// Default values based on commented code in transport.go: [10, 512, 512, 512, 128]
	receptionQueues := getIntSlice("raft-transport-reception-queues")
	if len(receptionQueues) > 0 {
		cfg.TransportConfig.Reception = receptionQueues
	} else {
		// Default values: [10, 512, 512, 512, 128] for priorities 0-4
		cfg.TransportConfig.Reception = []int{10, 512, 512}
	}

	// Parse transport send queues
	// Default values based on commented code in transport.go: [10, 512, 512, 512, 128]
	sendQueues := getIntSlice("raft-transport-send-queues")
	if len(sendQueues) > 0 {
		cfg.TransportConfig.Send = sendQueues
	} else {
		// Default values: [10, 512, 512, 512, 128] for priorities 0-4
		cfg.TransportConfig.Send = []int{10, 512, 512}
	}

	if cfg.RaftConfig.AdvertiseAddr == "" {
		cfg.RaftConfig.AdvertiseAddr = cfg.RaftConfig.BindAddr
	}

	return cfg, nil
}
