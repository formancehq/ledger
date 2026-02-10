package server

import (
	"context"
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
	"github.com/formancehq/ledger-v3-poc/internal/monitoring/pyroscope"
	"github.com/formancehq/ledger-v3-poc/internal/monitoring/tracesampling"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
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
	runCmd.Flags().String("bind-addr", "0.0.0.0:7777", "Address to bind for Raft transport (internal inter-node communication)")
	runCmd.Flags().String("advertise-addr", "", "Address to advertise to other nodes (defaults to bind-addr)")
	runCmd.Flags().Int("grpc-port", 8888, "gRPC port for service API (external client-facing)")
	runCmd.Flags().String("wal-dir", "./wal", "WAL directory for Raft")
	runCmd.Flags().String("data-dir", "./data", "Data directory for application storage")
	runCmd.Flags().StringSlice("peers", []string{}, "Initial peer list (comma-separated, format: <id>/<raftAddress>/<serviceAddress>, e.g., \"1/node-1:7777/node-1:8888,2/node-2:7777/node-2:8888\")")
	runCmd.Flags().Int("http-port", 9000, "HTTP server port")
	runCmd.Flags().Uint64("snapshot-threshold", 5000, "Number of logs before triggering a snapshot (0 = use Raft default)")
	// todo: remove
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

	// Pebble storage configuration flags
	runCmd.Flags().Uint64("pebble-memtable-size", 0, "Pebble memtable size in bytes (default: 256MB)")
	runCmd.Flags().Int("pebble-memtable-stop-writes-threshold", 0, "Pebble memtable count before stopping writes (default: 6)")
	runCmd.Flags().Int("pebble-l0-compaction-threshold", 0, "Pebble L0 file count to trigger compaction (default: 64)")
	runCmd.Flags().Int("pebble-l0-stop-writes-threshold", 0, "Pebble L0 file count before stopping writes (default: 256)")
	runCmd.Flags().Int64("pebble-lbase-max-bytes", 0, "Pebble L1 max size in bytes (default: 2GB)")
	runCmd.Flags().Int64("pebble-cache-size", 0, "Pebble block cache size in bytes (default: 1GB)")
	runCmd.Flags().Int64("pebble-target-file-size", 0, "Pebble SST file target size in bytes (default: 256MB)")
	runCmd.Flags().Int("pebble-bytes-per-sync", 0, "Pebble bytes written before sync during flush/compaction (default: 1MB)")
	runCmd.Flags().Int("pebble-wal-bytes-per-sync", 0, "Pebble WAL bytes written before sync (default: 1MB)")
	runCmd.Flags().Int("pebble-max-concurrent-compactions", 0, "Pebble max concurrent compactions (default: 2)")
	runCmd.Flags().Duration("pebble-wal-min-sync-interval", 0, "Pebble minimum interval between WAL syncs (default: 0, immediate sync)")
	runCmd.Flags().Bool("pebble-disable-wal", false, "Pebble disable WAL (WARNING: risks data loss)")
	runCmd.Flags().Uint64("cache-rotation-threshold", 1000, "Cache rotation threshold (0 = use default 1000)")

	// Health check configuration flags
	runCmd.Flags().Duration("health-check-interval", 30*time.Second, "Interval between health checks (default: 30s)")
	runCmd.Flags().Float64("health-wal-threshold", 0.8, "WAL volume usage threshold (0.0-1.0, default: 0.8 = 80%)")
	runCmd.Flags().Float64("health-data-threshold", 0.8, "Data volume usage threshold (0.0-1.0, default: 0.8 = 80%)")

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

	defer func() {
		switch loggerProvider := global.GetLoggerProvider().(type) {
		case *sdklog.LoggerProvider:
			if err := loggerProvider.ForceFlush(context.Background()); err != nil {
				logger.Errorf("Failed to flush logs: %v", err)
			}
			if err := loggerProvider.Shutdown(context.Background()); err != nil {
				logger.Errorf("Failed to shutdown logs: %v", err)
			}
		default:
			logger.Errorf("Unknown logger provider type: %T", loggerProvider)
		}
	}()

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
	cfg.GRPCPort = getInt("grpc-port", 8888)
	cfg.RaftConfig.NodeID = getUint64("node-id", 0)
	cfg.RaftConfig.BindAddr = getString("bind-addr", "127.0.0.1:8888")
	cfg.RaftConfig.AdvertiseAddr = getString("advertise-addr", "")
	cfg.RaftConfig.WalDir = getString("wal-dir", "./wal")
	cfg.DataDir = getString("data-dir", "./data")
	cfg.RaftConfig.Peers = make([]node.Peer, 0)
	for _, peer := range getStringSlice("peers") {
		// Format: <id>/<raftAddress>/<serviceAddress>
		parts := strings.Split(peer, "/")
		if len(parts) != 3 {
			return nil, fmt.Errorf("invalid peer format: expected <id>/<raftAddress>/<serviceAddress>, got %q", peer)
		}

		id, err := strconv.ParseUint(parts[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid peer ID: %w", err)
		}

		cfg.RaftConfig.Peers = append(cfg.RaftConfig.Peers, node.Peer{
			ID:             id,
			Address:        parts[1],
			ServiceAddress: parts[2],
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

	// Load Pebble configuration with defaults
	cfg.PebbleConfig = loadPebbleConfig(cmd)

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

	cfg.RaftConfig.RotationThreshold = getUint64("cache-rotation-threshold", 0)

	// Health check configuration
	cfg.HealthConfig.Interval = getDuration("health-check-interval", 30*time.Second)
	cfg.HealthConfig.WALThreshold, _ = cmd.Flags().GetFloat64("health-wal-threshold")
	cfg.HealthConfig.DataThreshold, _ = cmd.Flags().GetFloat64("health-data-threshold")

	return cfg, nil
}

// loadPebbleConfig loads Pebble configuration from command flags with defaults.
func loadPebbleConfig(cmd *cobra.Command) data.Config {
	cfg := data.DefaultConfig()

	// Helper to get uint64 with default
	getUint64 := func(flagName string, defaultValue uint64) uint64 {
		if val, _ := cmd.Flags().GetUint64(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	// Helper to get int64 with default
	getInt64 := func(flagName string, defaultValue int64) int64 {
		if val, _ := cmd.Flags().GetInt64(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	// Helper to get int with default
	getInt := func(flagName string, defaultValue int) int {
		if val, _ := cmd.Flags().GetInt(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	// Helper to get duration with default
	getDuration := func(flagName string, defaultValue time.Duration) time.Duration {
		if val, _ := cmd.Flags().GetDuration(flagName); val != 0 {
			return val
		}
		return defaultValue
	}

	cfg.MemTableSize = getUint64("pebble-memtable-size", cfg.MemTableSize)
	cfg.MemTableStopWritesThreshold = getInt("pebble-memtable-stop-writes-threshold", cfg.MemTableStopWritesThreshold)
	cfg.L0CompactionThreshold = getInt("pebble-l0-compaction-threshold", cfg.L0CompactionThreshold)
	cfg.L0StopWritesThreshold = getInt("pebble-l0-stop-writes-threshold", cfg.L0StopWritesThreshold)
	cfg.LBaseMaxBytes = getInt64("pebble-lbase-max-bytes", cfg.LBaseMaxBytes)
	cfg.CacheSize = getInt64("pebble-cache-size", cfg.CacheSize)
	cfg.TargetFileSize = getInt64("pebble-target-file-size", cfg.TargetFileSize)
	cfg.BytesPerSync = getInt("pebble-bytes-per-sync", cfg.BytesPerSync)
	cfg.WALBytesPerSync = getInt("pebble-wal-bytes-per-sync", cfg.WALBytesPerSync)
	cfg.MaxConcurrentCompactions = getInt("pebble-max-concurrent-compactions", cfg.MaxConcurrentCompactions)
	cfg.WALMinSyncInterval = getDuration("pebble-wal-min-sync-interval", cfg.WALMinSyncInterval)

	// Bool flag: explicitly check if set
	if disableWAL, _ := cmd.Flags().GetBool("pebble-disable-wal"); disableWAL {
		cfg.DisableWAL = true
	}

	return cfg
}
