package server

import (
	"context"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"time"

	"github.com/formancehq/go-libs/v3/auth"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/service"
	"github.com/formancehq/ledger-v3-poc/internal/bootstrap"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/pyroscope"
	"github.com/formancehq/ledger-v3-poc/internal/infra/monitoring/tracesampling"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/fx"
	"google.golang.org/grpc"
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

	// Add authentication flags from go-libs
	auth.AddFlags(runCmd.Flags())

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
	runCmd.Flags().Bool("bootstrap", false, "Initialize a new single-node cluster (mutually exclusive with --join)")
	runCmd.Flags().Uint64("learner-promotion-threshold", 100, "Max log entry lag before auto-promoting a caught-up learner to voter (0 = disable)")
	runCmd.Flags().Int("http-port", 9000, "HTTP server port")
	runCmd.Flags().Uint64("snapshot-threshold", 5000, "Number of logs before triggering a snapshot (0 = use Raft default)")

	runCmd.Flags().Int("raft-election-tick", 10, "Election timeout in ticks (0 = use default 10)")
	runCmd.Flags().Int("raft-heartbeat-tick", 1, "Heartbeat interval in ticks (0 = use default 1)")
	runCmd.Flags().Uint64("raft-max-size-per-msg", 0, "Maximum size per message in bytes (0 = use default 1MB)")
	runCmd.Flags().Int("raft-max-inflight-msgs", 0, "Maximum number of in-flight messages (0 = use default 256)")
	runCmd.Flags().Duration("raft-tick-interval", 100*time.Millisecond, "Interval between Raft ticks (0 = use default 100ms)")
	runCmd.Flags().Uint64("raft-compaction-margin", 1000, "Minimum log entries between snapshots (0 = use default 1000)")
	runCmd.Flags().Int("raft-propose-queue-capacity", 0, "Capacity of the propose queue (0 = use default 100)")
	runCmd.Flags().IntSlice("raft-transport-reception-queues", []int{}, "Comma-separated list of reception queue capacities per priority (e.g., \"10,512,512,512,128\")")
	runCmd.Flags().IntSlice("raft-transport-send-queues", []int{}, "Comma-separated list of send queue capacities per priority (e.g., \"10,512,512,512,128\")")
	runCmd.Flags().Int("raft-transport-buffer-size", 0, "Per-peer send buffer capacity in bytes (0 = use default 10MB)")
	runCmd.Flags().Duration("raft-processing-tick-interval", 0, "Interval for processing committed entries (0 = tick-interval/10)")
	runCmd.Flags().Int("raft-replay-batch-size", 0, "Number of entries per batch during spool replay (0 = use default 1000)")
	runCmd.Flags().Bool("grpc-compression", false, "Enable gzip compression on gRPC calls")

	// Pebble storage configuration flags
	runCmd.Flags().Uint64("pebble-memtable-size", 0, "Pebble memtable size in bytes (default: 256MB)")
	runCmd.Flags().Int("pebble-memtable-stop-writes-threshold", 0, "Pebble memtable count before stopping writes (default: 6)")
	runCmd.Flags().Int("pebble-l0-compaction-threshold", 0, "Pebble L0 file count to trigger compaction (default: 4)")
	runCmd.Flags().Int("pebble-l0-stop-writes-threshold", 0, "Pebble L0 file count before stopping writes (default: 16)")
	runCmd.Flags().Int64("pebble-lbase-max-bytes", 0, "Pebble L1 max size in bytes (default: 2GB)")
	runCmd.Flags().Int64("pebble-cache-size", 0, "Pebble block cache size in bytes (default: 1GB)")
	runCmd.Flags().Int64("pebble-target-file-size", 0, "Pebble SST file target size in bytes (default: 256MB)")
	runCmd.Flags().Int("pebble-bytes-per-sync", 0, "Pebble bytes written before sync during flush/compaction (default: 1MB)")
	runCmd.Flags().Int("pebble-wal-bytes-per-sync", 0, "Pebble WAL bytes written before sync (default: 1MB)")
	runCmd.Flags().Int("pebble-max-concurrent-compactions", 0, "Pebble max concurrent compactions (default: 2)")
	runCmd.Flags().Duration("pebble-wal-min-sync-interval", 0, "Pebble minimum interval between WAL syncs (default: 0, immediate sync)")
	runCmd.Flags().Bool("pebble-disable-wal", false, "Pebble disable WAL (WARNING: risks data loss)")
	runCmd.Flags().Uint64("pebble-incremental-compact-threshold", 0, "New log entries before triggering incremental compaction (default: 100000)")
	runCmd.Flags().Uint64("cache-rotation-threshold", 1000, "Cache rotation threshold (0 = use default 1000)")
	runCmd.Flags().Int("numscript-cache-size", 1024, "Maximum number of parsed Numscript programs to cache (LRU eviction)")
	runCmd.Flags().Int("mirror-max-batch-size", 500, "Maximum allowed batch size for mirror sync (server-side cap on user-configured batch size)")

	// Health check configuration flags
	runCmd.Flags().Duration("health-check-interval", 30*time.Second, "Interval between health checks (default: 30s)")
	runCmd.Flags().Float64("health-wal-threshold", 0.8, "WAL volume usage threshold (0.0-1.0, default: 0.8 = 80%)")
	runCmd.Flags().Float64("health-data-threshold", 0.8, "Data volume usage threshold (0.0-1.0, default: 0.8 = 80%)")
	runCmd.Flags().Duration("health-clock-skew-threshold", 500*time.Millisecond, "Maximum allowed clock skew between nodes (0 to disable)")
	runCmd.Flags().String("cluster-id", "", "Cluster ID for inter-node communication validation")

	// Admission metrics (disabled by default to avoid contention under high concurrency)
	runCmd.Flags().Bool("admission-metrics", false, "Enable admission metrics (histograms/counters in the admission hot path)")

	// Receipt signing key for JWT transaction receipts
	runCmd.Flags().String("receipt-signing-key", "", "HMAC key for signing JWT transaction receipts (empty = disabled)")

	// Response signing key for Ed25519 response signatures
	runCmd.Flags().String("response-signing-key", "", "Path to Ed25519 seed file for response signing (empty = disabled)")

	// Cold storage configuration
	runCmd.Flags().String("cold-storage-driver", "filesystem", "Cold storage driver for period archival (filesystem, s3)")
	runCmd.Flags().String("cold-storage-path", "", "Base path for cold storage (default: <data-dir>/cold-storage)")
	runCmd.Flags().String("cold-storage-bucket-id", "", "Shared namespace prefix for archives (default: cluster-id)")
	runCmd.Flags().String("cold-storage-s3-bucket", "", "S3 bucket name (required when driver=s3)")
	runCmd.Flags().String("cold-storage-s3-region", "", "AWS region for S3")
	runCmd.Flags().String("cold-storage-s3-endpoint", "", "Custom S3 endpoint (for MinIO)")

	// TLS configuration flags
	runCmd.Flags().String("tls-cert-file", "", "Path to TLS certificate file (PEM)")
	runCmd.Flags().String("tls-key-file", "", "Path to TLS private key file (PEM)")
	runCmd.Flags().String("tls-ca-cert-file", "", "Path to CA certificate file (PEM) for client verification")

	// Join mode: join an existing cluster as a learner node
	runCmd.Flags().String("join", "", "Service address of an existing cluster member to join as a learner (e.g., \"node-1:8888\")")

	// Restore mode: start in restore mode to accept backup upload
	runCmd.Flags().Bool("restore", false, "Start in restore mode (accepts backup upload, no Raft)")

	// Ed25519 authentication keys
	runCmd.Flags().String("auth-ed25519-keys", "", "Path to JSON file with Ed25519 public keys and scopes for authentication")

	// Scope mapping: virtual → granular scope expansion
	runCmd.Flags().String("auth-scope-mapping-file", "", "Path to JSON file mapping virtual scopes (e.g. ledger:read) to granular scopes")

	// Configuration safety
	runCmd.Flags().Bool("unsafe-skip-config-validation", false, "Skip startup configuration safety checks (DANGEROUS: allows node-id/cluster-id changes)")

	// Read index configuration
	runCmd.Flags().String("read-index-dir", "", "Directory for the read index bbolt database (default: <data-dir>/read-indexes/)")
	runCmd.Flags().Bool("read-index-no-freelist-sync", false, "Skip bbolt freelist serialization on commit (faster bulk writes, slower reopen)")
	runCmd.Flags().Int("read-index-batch-size", 0, "Number of log entries per bbolt write transaction (0 = default 1000)")
	runCmd.Flags().Duration("read-index-freelist-sync-interval", 5*time.Minute, "Periodic freelist sync interval when no-freelist-sync is enabled (0 to disable)")
	runCmd.Flags().Int("read-index-initial-mmap-size", 0, "Initial mmap size for the bbolt read index in bytes (0 = default 1 GiB)")

	// Query profiling
	runCmd.Flags().Duration("query-profile-threshold", 10*time.Millisecond, "Log and emit OTel attributes for queries exceeding this duration (0 to disable)")

	// gRPC slow threshold
	runCmd.Flags().Duration("grpc-slow-threshold", time.Second, "Duration above which a gRPC call is logged as slow")

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

	memlimit := debug.SetMemoryLimit(-1)
	if memlimit == math.MaxInt64 {
		logger.Infof("GOMAXPROCS=%d NumCPU=%d GOMEMLIMIT=off", runtime.GOMAXPROCS(0), runtime.NumCPU())
	} else {
		logger.Infof("GOMAXPROCS=%d NumCPU=%d GOMEMLIMIT=%dMiB", runtime.GOMAXPROCS(0), runtime.NumCPU(), memlimit/(1024*1024))
	}

	logMemoryEstimate(logger, cfg, memlimit)

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

	// Select the application module based on mode
	var appModule fx.Option
	if cfg.Restore {
		appModule = bootstrap.RestoreModule()
	} else {
		appModule = bootstrap.Module()
	}

	// Build authentication module.
	// Only use go-libs OIDC module when an issuer is configured; otherwise
	// skip OIDC discovery to avoid a crash on empty issuer URL.
	// Ed25519-only auth works without OIDC (the KeySet parameter in
	// buildAuthConfig is optional).
	var authModule fx.Option
	if cfg.AuthConfig.Issuer != "" {
		authModule = auth.FXModuleFromFlags(cmd)
	} else {
		authModule = fx.Module("auth")
	}

	// Create fx application options
	opts := []fx.Option{
		// Provide configuration
		fx.Supply(*cfg),
		// Add authentication module (OIDC discovery when issuer is configured)
		authModule,
		// Add OpenTelemetry modules from go-libs (using flags)
		otlp.FXModuleFromFlags(cmd, otlp.WithServiceVersion(fmt.Sprintf("%s-%s", version, commit))),
		otlptraces.FXModuleFromFlags(cmd),
		otlpmetrics.FXModuleFromFlags(cmd),
		// Add trace sampling module (wraps exporter with error-aware sampling)
		tracesampling.Module(traceSamplingCfg),
		// Add Pyroscope profiling module
		pyroscope.Module(pyroscopeCfg),
		// Provide application module
		appModule,
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

func LoadConfig(cmd *cobra.Command) (*bootstrap.Config, error) {
	cfg := &bootstrap.Config{}

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
		if cmd.Flags().Changed(flagName) {
			val, _ := cmd.Flags().GetBool(flagName)
			return val
		}
		return defaultValue
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
	cfg.RaftConfig.Bootstrap = getBool("bootstrap", false)
	cfg.RaftConfig.AutoPromoteThreshold = getUint64("learner-promotion-threshold", 100)
	cfg.RaftConfig.SnapshotThreshold = getUint64("snapshot-threshold", 0)

	cfg.RaftConfig.ElectionTick = getInt("raft-election-tick", 0)
	cfg.RaftConfig.HeartbeatTick = getInt("raft-heartbeat-tick", 0)
	cfg.RaftConfig.MaxSizePerMsg = getUint64("raft-max-size-per-msg", 0)
	cfg.RaftConfig.MaxInflightMsgs = getInt("raft-max-inflight-msgs", 0)
	cfg.RaftConfig.TickInterval = getDuration("raft-tick-interval", 0)
	cfg.RaftConfig.CompactionMargin = getUint64("raft-compaction-margin", 1000)
	cfg.RaftConfig.ProposeQueueCapacity = getInt("raft-propose-queue-capacity", 0)
	cfg.RaftConfig.TransportBufferSize = getInt("raft-transport-buffer-size", 0)
	cfg.RaftConfig.ProcessingTickInterval = getDuration("raft-processing-tick-interval", 0)
	cfg.RaftConfig.ReplayBatchSize = getInt("raft-replay-batch-size", 0)
	cfg.PoolConfig.Compression = getBool("grpc-compression", false)

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
	cfg.NumscriptCacheSize = getInt("numscript-cache-size", 1024)
	cfg.MirrorMaxBatchSize = getInt("mirror-max-batch-size", 500)

	// Health check configuration
	cfg.HealthConfig.Interval = getDuration("health-check-interval", 30*time.Second)
	cfg.HealthConfig.WALThreshold, _ = cmd.Flags().GetFloat64("health-wal-threshold")
	cfg.HealthConfig.DataThreshold, _ = cmd.Flags().GetFloat64("health-data-threshold")
	cfg.HealthConfig.ClockSkewThreshold = getDuration("health-clock-skew-threshold", 500*time.Millisecond)

	cfg.ClusterID = getString("cluster-id", "")

	// Admission metrics
	cfg.AdmissionMetrics = getBool("admission-metrics", false)

	// Receipt signing key
	cfg.ReceiptSigningKey = getString("receipt-signing-key", "")

	// Response signing key
	cfg.ResponseSigningKeyFile = getString("response-signing-key", "")

	// Cold storage configuration
	cfg.ColdStorageConfig.Driver = getString("cold-storage-driver", "filesystem")
	cfg.ColdStorageConfig.BasePath = getString("cold-storage-path", "")
	cfg.ColdStorageConfig.BucketID = getString("cold-storage-bucket-id", "")
	cfg.ColdStorageConfig.S3Bucket = getString("cold-storage-s3-bucket", "")
	cfg.ColdStorageConfig.S3Region = getString("cold-storage-s3-region", "")
	cfg.ColdStorageConfig.S3Endpoint = getString("cold-storage-s3-endpoint", "")

	// TLS configuration
	tlsCert := getString("tls-cert-file", "")
	tlsKey := getString("tls-key-file", "")
	tlsCA := getString("tls-ca-cert-file", "")
	if (tlsCert == "") != (tlsKey == "") {
		return nil, fmt.Errorf("--tls-cert-file and --tls-key-file must be provided together")
	}
	cfg.TLSConfig = bootstrap.TLSConfig{
		Enabled:  tlsCert != "" && tlsKey != "",
		CertFile: tlsCert,
		KeyFile:  tlsKey,
		CAFile:   tlsCA,
	}

	// Restore mode
	cfg.Restore = getBool("restore", false)

	// Authentication configuration
	ed25519KeysFile := getString("auth-ed25519-keys", "")
	scopeMappingFile := getString("auth-scope-mapping-file", "")
	scopeMappingJSON := os.Getenv("AUTH_SCOPE_MAPPING") // env var only (used by operator)
	cfg.AuthConfig = bootstrap.AuthFlagConfig{
		Enabled:          getBool(auth.AuthEnabledFlag, false),
		Issuer:           getString(auth.AuthIssuerFlag, ""),
		Service:          getString(auth.AuthServiceFlag, "ledger"),
		Ed25519KeysFile:  ed25519KeysFile,
		ScopeMappingFile: scopeMappingFile,
		ScopeMappingJSON: scopeMappingJSON,
	}
	// Auto-enable auth when Ed25519 keys are configured.
	if ed25519KeysFile != "" {
		cfg.AuthConfig.Enabled = true
	}

	// Configuration safety
	cfg.UnsafeSkipConfigValidation = getBool("unsafe-skip-config-validation", false)

	// Read index configuration
	cfg.ReadIndexConfig = bootstrap.ReadIndexConfig{
		Dir:                  getString("read-index-dir", ""),
		NoFreelistSync:       getBool("read-index-no-freelist-sync", false),
		BatchSize:            getInt("read-index-batch-size", 0),
		FreelistSyncInterval: getDuration("read-index-freelist-sync-interval", 5*time.Minute),
		InitialMmapSize:      getInt("read-index-initial-mmap-size", 0),
	}

	// Query profiling
	cfg.QueryProfileThreshold = getDuration("query-profile-threshold", 10*time.Millisecond)

	// gRPC slow threshold
	cfg.GRPCSlowThreshold = getDuration("grpc-slow-threshold", time.Second)

	// Join mode: discover peers from an existing cluster member
	joinAddr := getString("join", "")
	if cfg.Restore {
		if cfg.RaftConfig.Bootstrap {
			return nil, fmt.Errorf("--restore and --bootstrap are mutually exclusive")
		}
		if joinAddr != "" {
			return nil, fmt.Errorf("--restore and --join are mutually exclusive")
		}
	}

	if joinAddr != "" {
		if cfg.RaftConfig.Bootstrap {
			return nil, fmt.Errorf("--join and --bootstrap are mutually exclusive")
		}

		fmt.Printf("Join mode: discovering peers from cluster via %s\n", joinAddr)
		peers, err := discoverPeersFromClusterWithRetry(joinAddr, cfg.TLSConfig)
		if err != nil {
			return nil, fmt.Errorf("discovering peers from cluster: %w", err)
		}
		for _, p := range peers {
			fmt.Printf("  Discovered peer: id=%d raft=%s service=%s\n", p.ID, p.Address, p.ServiceAddress)
		}
		cfg.RaftConfig.Peers = peers
	}

	return cfg, nil
}

// discoverPeersFromClusterWithRetry retries peer discovery with exponential backoff
// for up to 60 seconds, allowing the bootstrap node time to start.
func discoverPeersFromClusterWithRetry(serviceAddr string, tlsCfg bootstrap.TLSConfig) ([]node.Peer, error) {
	var (
		lastErr  error
		delay    = 500 * time.Millisecond
		deadline = time.After(60 * time.Second)
	)

	for {
		peers, err := discoverPeersFromCluster(serviceAddr, tlsCfg)
		if err == nil {
			return peers, nil
		}
		lastErr = err

		select {
		case <-deadline:
			return nil, fmt.Errorf("timed out after 60s discovering peers from %s: %w", serviceAddr, lastErr)
		case <-time.After(delay):
			if delay < 5*time.Second {
				delay = delay * 2
			}
		}
	}
}

// discoverPeersFromCluster connects to an existing cluster member and discovers
// all voter nodes and their addresses via GetClusterState.
func discoverPeersFromCluster(serviceAddr string, tlsCfg bootstrap.TLSConfig) ([]node.Peer, error) {
	creds, err := bootstrap.ClientTransportCredentials(tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("loading TLS credentials for peer discovery: %w", err)
	}

	conn, err := grpc.NewClient(serviceAddr,
		grpc.WithTransportCredentials(creds),
	)
	if err != nil {
		return nil, fmt.Errorf("connecting to cluster member %s: %w", serviceAddr, err)
	}
	defer func() { _ = conn.Close() }()

	client := clusterpb.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	state, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return nil, fmt.Errorf("getting cluster state from %s: %w", serviceAddr, err)
	}

	var peers []node.Peer
	for _, nodeInfo := range state.Nodes {
		if nodeInfo.RaftAddress == "" || nodeInfo.ServiceAddress == "" {
			continue
		}
		peers = append(peers, node.Peer{
			ID:             uint64(nodeInfo.Id),
			Address:        nodeInfo.RaftAddress,
			ServiceAddress: nodeInfo.ServiceAddress,
		})
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers with addresses found in cluster state from %s", serviceAddr)
	}

	return peers, nil
}

const (
	// Fixed memory estimates for components not directly configurable.
	goRuntimeEstimate  int64 = 200 << 20 // 200 MiB — GC, stacks, goroutines

	// FSM cache heuristics: each raft entry touches ~30 unique cache keys
	// across 9 AttributeCache instances, averaging ~300 bytes per entry.
	// Total = 2 generations * RotationThreshold * keysPerEntry * bytesPerKey.
	fsmCacheKeysPerEntry  int64 = 30
	fsmCacheBytesPerKey   int64 = 300
	fsmCacheGenerations   int64 = 2
)

func logMemoryEstimate(logger logging.Logger, cfg *bootstrap.Config, memlimit int64) {
	mib := func(b int64) int64 { return b / (1 << 20) }

	pebbleCache := int64(cfg.PebbleConfig.CacheSize)
	memtables := int64(cfg.PebbleConfig.MemTableSize) * int64(cfg.PebbleConfig.MemTableStopWritesThreshold)

	bboltMmapEstimate := int64(cfg.ReadIndexConfig.InitialMmapSize)
	if bboltMmapEstimate == 0 {
		bboltMmapEstimate = 1 << 30 // default 1 GiB
	}

	transportBuf := int64(cfg.RaftConfig.TransportBufferSize)
	if transportBuf == 0 {
		transportBuf = 10 * 1024 * 1024 // default 10 MiB
	}
	peerCount := int64(len(cfg.RaftConfig.Peers))
	if peerCount == 0 {
		peerCount = 1
	}
	transportTotal := transportBuf * peerCount

	rotationThreshold := int64(cfg.RaftConfig.RotationThreshold)
	if rotationThreshold == 0 {
		rotationThreshold = 1000
	}
	fsmCache := fsmCacheGenerations * rotationThreshold * fsmCacheKeysPerEntry * fsmCacheBytesPerKey

	total := pebbleCache + memtables + bboltMmapEstimate + transportTotal + fsmCache + goRuntimeEstimate

	logger.Infof(
		"Memory estimate: pebbleCache=%dMiB memtables=%dMiB bboltMmap=%dMiB transport=%dMiB fsmCache=%dMiB goRuntime=%dMiB total=%dMiB",
		mib(pebbleCache), mib(memtables), mib(bboltMmapEstimate),
		mib(transportTotal), mib(fsmCache), mib(goRuntimeEstimate), mib(total),
	)

	if memlimit != math.MaxInt64 && total > memlimit {
		logger.Errorf(
			"WARNING: estimated memory usage (%dMiB) exceeds GOMEMLIMIT (%dMiB) — risk of OOM. Consider increasing memory limits or reducing pebble-cache-size / pebble-memtable-size.",
			mib(total), mib(memlimit),
		)
	}
}

// loadPebbleConfig loads Pebble configuration from command flags with defaults.
func loadPebbleConfig(cmd *cobra.Command) dal.Config {
	cfg := dal.DefaultConfig()

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
	cfg.IncrementalCompactThreshold = getUint64("pebble-incremental-compact-threshold", cfg.IncrementalCompactThreshold)

	// Bool flag: explicitly check if set
	if disableWAL, _ := cmd.Flags().GetBool("pebble-disable-wal"); disableWAL {
		cfg.DisableWAL = true
	}

	return cfg
}
