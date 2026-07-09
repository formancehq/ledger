package server

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.uber.org/fx"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	auth "github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/go-libs/v5/pkg/fx/observefx"
	otlp "github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	otlpmetrics "github.com/formancehq/go-libs/v5/pkg/observe/metrics"
	otlptraces "github.com/formancehq/go-libs/v5/pkg/observe/traces"
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/flightrecorder"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/pyroscope"
	"github.com/formancehq/ledger/v3/internal/infra/monitoring/tracesampling"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/bytesize"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func NewRootCommand() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "ledger",
		Short: "Ledger v3 with Raft cluster",
		Long:  "Ledger v3 with Raft consensus cluster",
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
	runCmd.Flags().Int("raft-election-tick", 10, "Election timeout in ticks (0 = use default 10)")
	runCmd.Flags().Int("raft-heartbeat-tick", 1, "Heartbeat interval in ticks (0 = use default 1)")
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "raft-max-size-per-msg", 0, "Maximum size per message (0 = use default 1Mi)")
	runCmd.Flags().Int("raft-max-inflight-msgs", 0, "Maximum number of in-flight messages (0 = use default 256)")
	runCmd.Flags().Duration("raft-tick-interval", 100*time.Millisecond, "Interval between Raft ticks (0 = use default 100ms)")
	runCmd.Flags().Uint64("raft-compaction-margin", 1000, "Minimum log entries between snapshots (0 = use default 1000)")
	runCmd.Flags().Duration("maintenance-interval", 30*time.Second, "Interval for background WAL snapshot + Pebble checkpoint (0 = use default 30s)")
	runCmd.Flags().Int("raft-propose-queue-capacity", 0, "Capacity of the propose queue (0 = use default 100)")
	runCmd.Flags().IntSlice("raft-transport-reception-queues", []int{}, "Comma-separated list of reception queue capacities per priority (e.g., \"10,512,512,512,128\")")
	runCmd.Flags().IntSlice("raft-transport-send-queues", []int{}, "Comma-separated list of send queue capacities per priority (e.g., \"10,512,512,512,128\")")
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "raft-transport-buffer-size", 0, "Per-peer send buffer capacity (0 = use default 10Mi)")
	runCmd.Flags().Duration("raft-processing-tick-interval", 0, "Interval for processing committed entries (0 = tick-interval/10)")
	runCmd.Flags().Int("raft-replay-batch-size", 0, "Number of entries per batch during spool replay (0 = use default 1000)")
	runCmd.Flags().Bool("grpc-compression", false, "Enable gzip compression on gRPC calls")

	// Pebble storage configuration flags (common flags shared with read index)
	registerPebbleFlags(runCmd, "pebble", dal.DefaultConfig().Config)
	// DAL-specific Pebble flags
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "pebble-wal-bytes-per-sync", 0, "Pebble WAL bytes written before sync (default: 1Mi)")
	runCmd.Flags().Duration("pebble-wal-min-sync-interval", 0, "Pebble minimum interval between WAL syncs (default: 0, immediate sync)")
	runCmd.Flags().Bool("pebble-disable-wal", false, "Pebble disable WAL (WARNING: risks data loss)")
	runCmd.Flags().Int("pebble-max-checkpoints", dal.DefaultConfig().MaxCheckpoints, "Maximum number of Pebble checkpoints to keep (default: 10)")
	runCmd.Flags().String("pebble-wal-failover-dir", "", "Secondary WAL directory for automatic failover on primary disk latency spikes (disabled if empty)")
	// Value separation flags
	runCmd.Flags().Bool("pebble-value-separation", false, "Enable value separation (large values stored in blob files)")
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "pebble-value-separation-min-size", 256, "Minimum value size for separation (default: 256)")
	runCmd.Flags().Int("pebble-value-separation-max-depth", 4, "Max blob reference depth per SSTable (default: 4)")
	runCmd.Flags().Duration("pebble-value-separation-rewrite-age", time.Hour, "Minimum blob file age before rewrite (default: 1h)")
	runCmd.Flags().Float64("pebble-value-separation-garbage-ratio", 0.20, "Blob garbage ratio before rewrite (default: 0.20)")

	runCmd.Flags().Uint64("cache-rotation-threshold", 1000, "Cache rotation threshold (0 = use default 1000)")
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "spool-segment-max-bytes", 0, "Maximum spool segment size before rotation/sealing (0 = use default 256Mi)")
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "backup-max-segment-bytes", 0, "Maximum incremental-backup export segment size before splitting into a new segment (0 = use default 4Gi)")
	runCmd.Flags().Int("numscript-cache-size", 1024, "Maximum number of parsed Numscript programs to cache (LRU eviction)")
	runCmd.Flags().Int("mirror-max-batch-size", 500, "Maximum allowed batch size for mirror sync (server-side cap on user-configured batch size)")
	runCmd.Flags().Int("max-execution-plan-size", 4096, "Maximum number of AttributePlan entries an ExecutionPlan may carry; admission rejects proposals beyond this (0 = unlimited)")

	// Health check configuration flags
	runCmd.Flags().Duration("health-check-interval", 30*time.Second, "Interval between health checks (default: 30s)")
	runCmd.Flags().Float64("health-wal-threshold", 0.8, "WAL volume usage threshold (0.0-1.0, default: 0.8 = 80%)")
	runCmd.Flags().Float64("health-data-threshold", 0.8, "Data volume usage threshold (0.0-1.0, default: 0.8 = 80%)")
	runCmd.Flags().Float64("health-wal-resume-threshold", 0.75, "WAL volume usage resume threshold for hysteresis (must satisfy 0 < resume < health-wal-threshold; when unset, derived as 0.9375 x the block threshold)")
	runCmd.Flags().Float64("health-data-resume-threshold", 0.75, "Data volume usage resume threshold for hysteresis (must satisfy 0 < resume < health-data-threshold; when unset, derived as 0.9375 x the block threshold)")
	runCmd.Flags().Duration("health-clock-skew-threshold", 500*time.Millisecond, "Maximum allowed clock skew between nodes (0 to disable)")
	runCmd.Flags().String("cluster-id", "", "Cluster ID for inter-node communication validation")

	// Admission metrics (disabled by default to avoid contention under high concurrency)
	runCmd.Flags().Bool("admission-metrics", false, "Enable admission metrics (histograms/counters in the admission hot path)")

	// Naming convention for metrics emitted by the server. "otel" preserves
	// dot-notation names (the OpenTelemetry default); "prom" prefixes every
	// metric the server emits with "ledger_" and converts dots to underscores
	// so the names are unambiguous after an OTLP→Prometheus collector that
	// sanitizes "." into "_". OpenTelemetry semantic-convention
	// auto-instrumentation (go.*, process.*, system.*, http.*) targets the
	// global MeterProvider, bypasses the ledger factory, and is therefore
	// never touched by this flag.
	runCmd.Flags().String("metrics-naming", "otel", "Application metrics naming convention (otel|prom)")

	// Receipt signing key for JWT transaction receipts
	runCmd.Flags().String("receipt-signing-key", "", "HMAC key for signing JWT transaction receipts (empty = disabled)")

	// Response signing key for Ed25519 response signatures
	runCmd.Flags().String("response-signing-key", "", "Path to Ed25519 seed file for response signing (empty = disabled)")

	// Cold storage configuration
	runCmd.Flags().String("cold-storage-driver", "none", "Cold storage driver for chapter archival (none, filesystem, s3)")
	runCmd.Flags().String("cold-storage-path", "", "Base path for cold storage (default: <data-dir>/cold-storage)")
	runCmd.Flags().String("cold-storage-bucket-id", "", "Shared namespace prefix for archives (default: cluster-id)")
	runCmd.Flags().String("cold-storage-s3-bucket", "", "S3 bucket name (required when driver=s3)")
	runCmd.Flags().String("cold-storage-s3-region", "", "AWS region for S3")
	runCmd.Flags().String("cold-storage-s3-endpoint", "", "Custom S3 endpoint (for MinIO)")
	runCmd.Flags().String("cold-cache-dir", "", "Directory for cold storage read cache (default: <data-dir>/cold-cache). Use a separate volume to avoid filling the data disk.")

	// TLS configuration flags
	runCmd.Flags().String("tls-mode", "disabled", "TLS mode for inter-node and service gRPC: disabled, optional (accepts both TLS and plaintext; used as a transitional state during a TLS toggle), or required (TLS only)")
	runCmd.Flags().String("tls-cert-file", "", "Path to TLS certificate file (PEM). Required when --tls-mode is optional or required.")
	runCmd.Flags().String("tls-key-file", "", "Path to TLS private key file (PEM). Required when --tls-mode is optional or required.")
	runCmd.Flags().String("tls-ca-cert-file", "", "Path to CA certificate file (PEM) for client verification")
	runCmd.Flags().Bool("tls-require-client-cert", false, "Reject inbound connections that do not present a CA-signed client certificate. Defaults to false (VerifyClientCertIfGiven), preserving the prior posture where missing client certs fall through to cluster-secret/JWT auth. Requires --tls-ca-cert-file.")

	// Join mode: join an existing cluster as a learner node
	runCmd.Flags().String("join", "", "Raft transport address of an existing cluster member to join as a learner (e.g., \"node-1:7777\")")

	// Restore mode: start in restore mode to accept backup upload
	runCmd.Flags().Bool("restore", false, "Start in restore mode (accepts backup upload, no Raft)")
	runCmd.Flags().String("restore-listen", "127.0.0.1", "Bind host for restore-mode gRPC + HTTP servers. The restore RPCs are not authenticated; default keeps them off the public network. Set to \"0.0.0.0\" or a specific interface to expose, but only behind TLS + firewalling.")
	runCmd.Flags().Int("restore-download-parallelism", 16, "Maximum number of concurrent S3 file downloads during an async restore. Clamped to [1, 64].")

	// Shared secret for inter-node authentication
	runCmd.Flags().String("cluster-secret", "", "Shared secret for inter-node gRPC authentication. Requires TLS (--tls-mode != disabled) — the secret would otherwise be sent in plaintext.")

	// Ed25519 authentication keys
	runCmd.Flags().String("auth-ed25519-keys", "", "Path to JSON file with Ed25519 public keys and scopes for authentication")

	// Scope mapping: virtual → granular scope expansion
	runCmd.Flags().String("auth-scope-mapping-file", "", "Path to JSON file mapping virtual scopes (e.g. ledger:read) to granular scopes")
	runCmd.Flags().String("auth-anonymous-scopes", "", "Comma-separated granular scopes granted to requests without a bearer token (e.g. \"*:read\" for writes-only mode). Wildcards: *:read, *:write")

	// OIDC discovery + JWKS HTTP timeout, applied in buildAuthConfig: a context
	// deadline bounds oidc.Discover and the keyset's http.Client.Timeout bounds
	// JWKS fetches, so both share one operator-controlled ceiling. 0 leaves them
	// unbounded for operators that need it.
	runCmd.Flags().Duration("auth-discovery-timeout", 10*time.Second, "Bound the HTTP calls used for OIDC discovery and JWKS fetches (0 = unbounded)")

	// Idempotency TTL and eviction
	runCmd.Flags().Duration("idempotency-ttl", 24*time.Hour, "Idempotency key time-to-live (0 = never expire)")
	runCmd.Flags().Duration("idempotency-eviction-interval", 60*time.Second, "How often the leader proposes idempotency eviction")

	// Snapshot sync configuration
	runCmd.Flags().Duration("snapshot-session-ttl", 5*time.Minute, "Server-side session TTL for snapshot sync (reaper cleans up expired sessions)")
	runCmd.Flags().Int("snapshot-parallelism", 4, "Number of parallel file fetch workers during snapshot sync")
	runCmd.Flags().Int("snapshot-retry-count", 5, "Session-level retry attempts for snapshot sync on transient errors")
	runCmd.Flags().Int("snapshot-file-retry-count", 3, "Per-file retry attempts during snapshot sync on transient stream errors")

	// Configuration safety
	runCmd.Flags().Bool("unsafe-skip-config-validation", false, "Skip startup configuration safety checks (DANGEROUS: allows node-id/cluster-id changes)")

	// Sentinel mode (runtime consistency checks)
	runCmd.Flags().Bool("sentinel-mode", false, "Enable sentinel mode: runtime volume consistency assertions (monotonicity, delta/posting cross-check, post-commit cache/Pebble verification)")

	// Bloom filter per-attribute-type configuration
	registerBloomFlags(runCmd)

	// Hash algorithm for log chain integrity
	runCmd.Flags().String("hash-algorithm", "blake3", "Hash algorithm for log chain (blake3 or xxh3)")

	// Read index configuration
	runCmd.Flags().String("read-index-dir", "", "Directory for the Pebble read index (default: <data-dir>/read-indexes/)")
	runCmd.Flags().Int("read-index-batch-size", 0, "Number of log entries per Pebble batch commit (0 = default 1000)")
	registerPebbleFlags(runCmd, "read-index", readstore.DefaultConfig())

	// Audit index configuration
	runCmd.Flags().Int("audit-index-batch-size", 0, "Audit entries per Pebble batch commit (0 = default 1000)")
	runCmd.Flags().Uint64("audit-index-rebuild-threshold", 0, "Drop+rebuild the audit index on boot when the cursor is this far behind (0 = never)")
	runCmd.Flags().Bool("disable-audit-index", false, "Disable the audit secondary index worker")

	// Query profiling
	runCmd.Flags().Duration("query-profile-threshold", 10*time.Millisecond, "Log and emit OTel attributes for queries exceeding this duration (0 to disable)")

	// gRPC slow threshold
	runCmd.Flags().Duration("grpc-slow-threshold", time.Second, "Duration above which a gRPC call is logged as slow")

	// Flight recorder flags
	runCmd.Flags().Bool("flight-recorder-enabled", false, "Enable the runtime flight recorder (continuous trace buffering)")
	runCmd.Flags().Duration("flight-recorder-min-age", 5*time.Second, "Minimum duration of trace data retained in the flight recorder buffer")
	bytesize.ByteSizeVar(runCmd, new(bytesize.ByteSize), "flight-recorder-max-bytes", 10<<20, "Maximum memory for the flight recorder buffer (default: 10Mi)")

	return runCmd
}

func runServer(cmd *cobra.Command, _ []string) error {
	cfg, err := LoadConfig(cmd.Context(), cmd)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}

	cmd.Println(string(data))

	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating config: %w", err)
	}

	// Set default service name if not provided via flags
	serviceName, _ := cmd.Flags().GetString(otlp.OtelServiceNameFlag)
	if serviceName == "" {
		// Set default service name based on node ID
		defaultServiceName := fmt.Sprintf("ledger-node-%d", cfg.RaftConfig.NodeID)

		err := cmd.Flags().Set(otlp.OtelServiceNameFlag, defaultServiceName)
		if err != nil {
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

		pyroscopeCfg.Tags["node_id"] = strconv.FormatUint(cfg.RaftConfig.NodeID, 10)
		pyroscopeCfg.Tags["cluster_id"] = cfg.ClusterID
	}

	// Configure trace sampling
	traceSamplingCfg := traceSamplingConfigFromFlags(cmd)

	// Configure flight recorder
	frEnabled, _ := cmd.Flags().GetBool("flight-recorder-enabled")
	frMinAge, _ := cmd.Flags().GetDuration("flight-recorder-min-age")
	frMaxBytes := bytesize.Get(cmd, "flight-recorder-max-bytes").Int()
	flightRecorderCfg := flightrecorder.Config{
		Enabled:  frEnabled,
		MinAge:   frMinAge,
		MaxBytes: frMaxBytes,
	}

	// Select the application module based on mode
	var appModule fx.Option
	if cfg.Restore {
		appModule = bootstrap.RestoreModule()
	} else {
		appModule = bootstrap.Module()
	}

	info := version.Get()

	// Auth (OIDC discovery + JWKS reads, bounded by OIDCDiscoveryTimeout) is built in
	// bootstrap.buildAuthConfig; there is no auth fx module. The go-libs authnfx JWT
	// module is intentionally not wired in: nothing in this service consumes its
	// map[string]oidc.KeySet or Authenticator, so its providers would never run.
	opts := []fx.Option{
		// Provide configuration
		fx.Supply(*cfg),
		// Provide build metadata for version reporting
		fx.Supply(info),
		// Add OpenTelemetry modules from go-libs (using flags)
		observefx.ResourceModuleFromFlags(cmd, otlp.WithServiceVersion(fmt.Sprintf("%s-%s", info.Version, info.Commit))),
		observefx.TracesModuleFromFlags(cmd),
		observefx.MetricsModuleFromFlags(cmd),
		// Add trace sampling module (wraps exporter with error-aware sampling)
		tracesampling.Module(traceSamplingCfg),
		// Add Pyroscope profiling module
		pyroscope.Module(pyroscopeCfg),
		// Add flight recorder module
		flightrecorder.Module(flightRecorderCfg),
		// Provide application module
		appModule,
		// Cold storage module (conditional on driver)
		bootstrap.ColdStorageModule(cfg.ColdStorageConfig.Driver),
	}

	defer func() {
		switch loggerProvider := global.GetLoggerProvider().(type) {
		case *sdklog.LoggerProvider:
			err := loggerProvider.ForceFlush(context.Background())
			if err != nil {
				logger.Errorf("Failed to flush logs: %v", err)
			}

			err = loggerProvider.Shutdown(context.Background())
			if err != nil {
				logger.Errorf("Failed to shutdown logs: %v", err)
			}
		default:
			logger.Errorf("Unknown logger provider type: %T", loggerProvider)
		}
	}()

	// Run the application (handles startup, signal handling, and graceful shutdown)
	return service.NewWithLogger(logger, opts...).Run(cmd)
}

func LoadConfig(ctx context.Context, cmd *cobra.Command) (*bootstrap.Config, error) {
	cfg := &bootstrap.Config{}

	// Helper function to get string value from flag (env vars are bound automatically by service.BindEnvToCommand)
	getString := func(flagName, defaultValue string) string {
		if val, _ := cmd.Flags().GetString(flagName); val != "" {
			return val
		}

		return defaultValue
	}

	// Helper function to get uint64 value from flag.
	//
	// When the flag is registered with cobra (the common case here),
	// cmd.Flags().GetUint64 already returns the registered default if the
	// user didn't set the flag, and the user's value otherwise — including
	// an explicit zero, which several flags document as a meaningful
	// sentinel ("0 = disable" / "0 = never expire"). The previous
	// `val != 0` shortcut silently substituted the local `defaultValue`
	// for any zero, making those sentinels impossible to set via the CLI
	// (#324). An earlier attempt routed the not-set branch through the
	// local `defaultValue`, but that put two competing defaults in play
	// (cobra's registered one and the call-site fallback) and where the
	// two disagreed — e.g. cobra default 1000 vs call-site 0 for
	// --cache-rotation-threshold — the not-set path silently produced an
	// invalid runtime value (here, a divide-by-zero in cache/generation.go).
	//
	// Trust cobra as the single source of truth for the registered case;
	// keep `defaultValue` as a safety net only for tests / call sites that
	// never registered the flag.
	getUint64 := func(flagName string, defaultValue uint64) uint64 {
		if cmd.Flags().Lookup(flagName) == nil {
			return defaultValue
		}

		val, _ := cmd.Flags().GetUint64(flagName)

		return val
	}

	// Helper function to get int value from flag. Same single-source-of-
	// truth rationale as getUint64.
	getInt := func(flagName string, defaultValue int) int {
		if cmd.Flags().Lookup(flagName) == nil {
			return defaultValue
		}

		val, _ := cmd.Flags().GetInt(flagName)

		return val
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

	// Helper function to get duration from flag. Same single-source-of-
	// truth rationale as getUint64 — flags like --idempotency-ttl,
	// --health-clock-skew-threshold and --query-profile-threshold document
	// 0 as a meaningful sentinel (disable / never expire) and MUST forward
	// that value unchanged when the user sets it. The previous `val != 0`
	// shortcut silently snapped --idempotency-ttl=0 back to 24h, so the
	// eviction scheduler then deleted idempotency keys the operator wanted
	// kept indefinitely and client retries created duplicate transactions
	// (#324).
	getDuration := func(flagName string, defaultValue time.Duration) time.Duration {
		if cmd.Flags().Lookup(flagName) == nil {
			return defaultValue
		}

		val, _ := cmd.Flags().GetDuration(flagName)

		return val
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
	cfg.RaftConfig.MaintenanceInterval = getDuration("maintenance-interval", 30*time.Second)

	cfg.RaftConfig.ElectionTick = getInt("raft-election-tick", 0)
	cfg.RaftConfig.HeartbeatTick = getInt("raft-heartbeat-tick", 0)
	cfg.RaftConfig.MaxSizePerMsg = bytesize.Get(cmd, "raft-max-size-per-msg").Uint64()
	cfg.RaftConfig.MaxInflightMsgs = getInt("raft-max-inflight-msgs", 0)
	cfg.RaftConfig.TickInterval = getDuration("raft-tick-interval", 0)
	cfg.RaftConfig.CompactionMargin = getUint64("raft-compaction-margin", 1000)
	cfg.RaftConfig.ProposeQueueCapacity = getInt("raft-propose-queue-capacity", 0)
	cfg.RaftConfig.TransportBufferSize = bytesize.Get(cmd, "raft-transport-buffer-size").Int()
	cfg.RaftConfig.ProcessingTickInterval = getDuration("raft-processing-tick-interval", 0)
	cfg.RaftConfig.ReplayBatchSize = getInt("raft-replay-batch-size", 0)
	cfg.PoolConfig.Compression = getBool("grpc-compression", false)

	// Load Pebble configuration with defaults
	cfg.PebbleConfig = loadPebbleConfig(cmd)

	// Parse transport reception queues.
	// Priority-0 is heartbeats + high-priority raft traffic (votes, MsgApp
	// leader→follower during ConfChange bursts). Under AsyncStorageWrites
	// each Ready cycle produces MORE rawNode.Step calls on the orchestrate
	// goroutine (one per MsgStorageAppendResp / MsgStorageApplyResp instead
	// of the single Advance() call in sync mode), so orchestrate spends
	// more time between select turns and the per-peer high-priority recv
	// queue can saturate faster. Antithesis 2h k8s run 81691b87... hit
	// this: 47 "Channel full" events in 15s on priority-0 during a
	// 3→7→5 scale-up/down burst. Bumped from 10 to 128 to absorb the burst
	// without changing the code path.
	receptionQueues := getIntSlice("raft-transport-reception-queues")
	if len(receptionQueues) > 0 {
		cfg.TransportConfig.Reception = receptionQueues
	} else {
		cfg.TransportConfig.Reception = []int{128, 512, 512}
	}

	// Parse transport send queues.
	// Symmetric bump on priority-0 send: same rationale — a stressed
	// receiver rejects with peer-respond-with-error, which cascades to
	// the sender's per-peer send buffer if it isn't sized to absorb the
	// re-transmit / retry pressure.
	sendQueues := getIntSlice("raft-transport-send-queues")
	if len(sendQueues) > 0 {
		cfg.TransportConfig.Send = sendQueues
	} else {
		cfg.TransportConfig.Send = []int{128, 512, 512}
	}

	if cfg.RaftConfig.AdvertiseAddr == "" {
		cfg.RaftConfig.AdvertiseAddr = cfg.RaftConfig.BindAddr
	}

	cfg.RaftConfig.RotationThreshold = getUint64("cache-rotation-threshold", 0)
	cfg.SpoolSegmentMaxBytes = bytesize.Get(cmd, "spool-segment-max-bytes").Int64()
	cfg.BackupMaxSegmentBytes = bytesize.Get(cmd, "backup-max-segment-bytes").Int64()
	cfg.NumscriptCacheSize = getInt("numscript-cache-size", 1024)
	cfg.MirrorMaxBatchSize = getInt("mirror-max-batch-size", 500)
	cfg.MaxExecutionPlanSize = getInt("max-execution-plan-size", 4096)

	// Health check configuration
	cfg.HealthConfig.Interval = getDuration("health-check-interval", 30*time.Second)
	cfg.HealthConfig.WALThreshold, _ = cmd.Flags().GetFloat64("health-wal-threshold")
	cfg.HealthConfig.DataThreshold, _ = cmd.Flags().GetFloat64("health-data-threshold")

	// resumeThreshold honors an explicitly-set resume flag, otherwise derives the
	// resume (low-water) mark from the block (high-water) threshold. A fixed
	// default rejects startup whenever an operator lowered the block threshold
	// below it (config validation requires 0 < resume < block); deriving from
	// block keeps the gap valid for any block in (0,1]. The 0.9375 factor
	// reproduces the shipped 0.75/0.8 default exactly.
	resumeThreshold := func(flagName string, block float64) float64 {
		if cmd.Flags().Changed(flagName) {
			val, _ := cmd.Flags().GetFloat64(flagName)

			return val
		}

		return block * 0.9375
	}
	cfg.HealthConfig.WALResumeThreshold = resumeThreshold("health-wal-resume-threshold", cfg.HealthConfig.WALThreshold)
	cfg.HealthConfig.DataResumeThreshold = resumeThreshold("health-data-resume-threshold", cfg.HealthConfig.DataThreshold)
	cfg.HealthConfig.ClockSkewThreshold = getDuration("health-clock-skew-threshold", 500*time.Millisecond)

	cfg.ClusterID = getString("cluster-id", "")

	// Admission metrics
	cfg.AdmissionMetrics = getBool("admission-metrics", false)

	// Metrics naming convention
	cfg.MetricsNaming = getString("metrics-naming", "otel")

	// Receipt signing key
	cfg.ReceiptSigningKey = getString("receipt-signing-key", "")

	// Response signing key
	cfg.ResponseSigningKeyFile = getString("response-signing-key", "")

	// Cold storage configuration
	cfg.ColdStorageConfig.Driver = getString("cold-storage-driver", "none")
	cfg.ColdStorageConfig.BasePath = getString("cold-storage-path", "")
	cfg.ColdStorageConfig.BucketID = getString("cold-storage-bucket-id", "")
	cfg.ColdStorageConfig.S3Bucket = getString("cold-storage-s3-bucket", "")
	cfg.ColdStorageConfig.S3Region = getString("cold-storage-s3-region", "")
	cfg.ColdStorageConfig.S3Endpoint = getString("cold-storage-s3-endpoint", "")
	cfg.ColdStorageConfig.CacheDir = getString("cold-cache-dir", "")

	// TLS configuration
	tlsMode := bootstrap.TLSMode(getString("tls-mode", string(bootstrap.TLSModeDisabled)))
	tlsCert := getString("tls-cert-file", "")
	tlsKey := getString("tls-key-file", "")
	tlsCA := getString("tls-ca-cert-file", "")

	if (tlsCert == "") != (tlsKey == "") {
		return nil, errors.New("--tls-cert-file and --tls-key-file must be provided together")
	}

	cfg.TLSConfig = bootstrap.TLSConfig{
		Mode:              tlsMode,
		CertFile:          tlsCert,
		KeyFile:           tlsKey,
		CAFile:            tlsCA,
		RequireClientCert: getBool("tls-require-client-cert", false),
	}

	// Restore mode
	cfg.Restore = getBool("restore", false)
	cfg.RestoreListen = getString("restore-listen", "127.0.0.1")
	cfg.RestoreDownloadParallelism = getInt("restore-download-parallelism", 16)

	// Cluster secret for inter-node authentication
	cfg.ClusterSecret = getString("cluster-secret", "")

	// Authentication configuration
	ed25519KeysFile := getString("auth-ed25519-keys", "")
	scopeMappingFile := getString("auth-scope-mapping-file", "")
	scopeMappingJSON := os.Getenv("AUTH_SCOPE_MAPPING") // env var only (used by operator)
	cfg.AuthConfig = bootstrap.AuthFlagConfig{
		Enabled:              getBool(auth.AuthEnabledFlag, false),
		Issuer:               getString(auth.AuthIssuerFlag, ""),
		Service:              getString(auth.AuthServiceFlag, "ledger"),
		Ed25519KeysFile:      ed25519KeysFile,
		ScopeMappingFile:     scopeMappingFile,
		ScopeMappingJSON:     scopeMappingJSON,
		AnonymousScopes:      getString("auth-anonymous-scopes", ""),
		OIDCDiscoveryTimeout: getDuration("auth-discovery-timeout", 10*time.Second),
	}
	// Idempotency TTL
	cfg.IdempotencyTTL = getDuration("idempotency-ttl", 24*time.Hour)
	cfg.IdempotencyEvictionInterval = getDuration("idempotency-eviction-interval", 60*time.Second)

	// Snapshot sync configuration
	cfg.SnapshotSyncConfig = bootstrap.SnapshotSyncConfig{
		SessionTTL:     getDuration("snapshot-session-ttl", 5*time.Minute),
		Parallelism:    getInt("snapshot-parallelism", 4),
		RetryCount:     getInt("snapshot-retry-count", 5),
		FileRetryCount: getInt("snapshot-file-retry-count", 3),
	}

	// Configuration safety
	cfg.UnsafeSkipConfigValidation = getBool("unsafe-skip-config-validation", false)

	// Sentinel mode
	cfg.SentinelMode = getBool("sentinel-mode", false)

	// Background checkpoint interval
	// Bloom filter per-type config
	cfg.BloomConfig = &commonpb.ClusterConfig{}
	loadBloomConfig(cmd, cfg.BloomConfig)

	// Hash algorithm for log chain
	switch getString("hash-algorithm", "blake3") {
	case "xxh3":
		cfg.BloomConfig.HashAlgorithm = commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3
	default:
		cfg.BloomConfig.HashAlgorithm = commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3
	}

	// Read index configuration
	cfg.ReadIndexConfig = bootstrap.ReadIndexConfig{
		Dir:          getString("read-index-dir", ""),
		BatchSize:    getInt("read-index-batch-size", 0),
		PebbleConfig: loadReadIndexPebbleConfig(cmd),
	}

	// Audit index configuration
	auditBatchSize, _ := cmd.Flags().GetInt("audit-index-batch-size")
	auditRebuildThreshold, _ := cmd.Flags().GetUint64("audit-index-rebuild-threshold")
	auditDisabled, _ := cmd.Flags().GetBool("disable-audit-index")
	cfg.AuditIndexConfig = bootstrap.AuditIndexConfig{
		BatchSize:        auditBatchSize,
		RebuildThreshold: auditRebuildThreshold,
		Disabled:         auditDisabled,
	}

	// Query profiling
	cfg.QueryProfileThreshold = getDuration("query-profile-threshold", 10*time.Millisecond)

	// gRPC slow threshold
	cfg.GRPCSlowThreshold = getDuration("grpc-slow-threshold", time.Second)

	// Join mode: discover peers from an existing cluster member
	joinAddr := getString("join", "")

	if cfg.Restore {
		if cfg.RaftConfig.Bootstrap {
			return nil, errors.New("--restore and --bootstrap are mutually exclusive")
		}

		if joinAddr != "" {
			return nil, errors.New("--restore and --join are mutually exclusive")
		}
	}

	if joinAddr != "" {
		if cfg.RaftConfig.Bootstrap {
			return nil, errors.New("--join and --bootstrap are mutually exclusive")
		}

		cmd.Printf("Join mode: discovering peers from cluster via %s\n", joinAddr)

		peers, err := discoverPeersFromClusterWithRetry(ctx, joinAddr, cfg.TLSConfig, cfg.ClusterID, cfg.ClusterSecret)
		if err != nil {
			return nil, fmt.Errorf("discovering peers from cluster: %w", err)
		}

		for _, p := range peers {
			cmd.Printf("  Discovered peer: id=%d raft=%s service=%s\n", p.ID, p.Address, p.ServiceAddress)
		}

		cfg.RaftConfig.Peers = peers
	}

	return cfg, nil
}

// discoverPeersFromClusterWithRetry retries peer discovery with exponential backoff
// indefinitely until peers are found or the context is cancelled (e.g. SIGTERM).
func discoverPeersFromClusterWithRetry(ctx context.Context, raftAddr string, tlsCfg bootstrap.TLSConfig, clusterID, clusterSecret string) ([]node.Peer, error) {
	delay := 500 * time.Millisecond

	for {
		peers, err := discoverPeersFromCluster(raftAddr, tlsCfg, clusterID, clusterSecret)
		if err == nil {
			return peers, nil
		}

		// A cluster-secret mismatch is a hard configuration error, never
		// transient: retrying with the same (mis)configuration would spin
		// until the deadline and then surface an opaque "context deadline
		// exceeded". Fail fast with an actionable message instead. EN-1080.
		if st, ok := status.FromError(err); ok && st.Code() == codes.Unauthenticated {
			return nil, &bootstrap.JoinAuthError{
				PeerAddress: raftAddr,
				HasSecret:   clusterSecret != "",
				Detail:      st.Message(),
			}
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("discovering peers from %s: %w: %w", raftAddr, ctx.Err(), err)
		case <-time.After(delay):
			if delay < 5*time.Second {
				delay *= 2
			}
		}
	}
}

// discoverPeersFromCluster connects to an existing cluster member's
// RaftServer and discovers all voter nodes and their addresses via
// ClusterBootstrapService.GetPeers. The call is authenticated by the
// cluster-id metadata (and, when configured, the cluster-secret bearer
// — preserved for compatibility with the existing RaftServer auth
// pipeline) — not by a user JWT.
func discoverPeersFromCluster(raftAddr string, tlsCfg bootstrap.TLSConfig, clusterID, clusterSecret string) ([]node.Peer, error) {
	creds, _, err := bootstrap.ClientTransportCredentials(tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("loading TLS credentials for peer discovery: %w", err)
	}

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}
	if clusterSecret != "" {
		opts = append(opts, transport.BearerTokenDialOption(clusterSecret))
	}

	conn, err := grpc.NewClient(raftAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("connecting to cluster member %s: %w", raftAddr, err)
	}

	defer func() { _ = conn.Close() }()

	client := clusterbootstrappb.NewClusterBootstrapServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if clusterID != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, node.MetadataKeyClusterID, clusterID)
	}

	resp, err := client.GetPeers(ctx, &clusterbootstrappb.GetPeersRequest{})
	if err != nil {
		return nil, fmt.Errorf("getting peers from %s: %w", raftAddr, err)
	}

	peers := make([]node.Peer, 0, len(resp.GetPeers()))

	for _, p := range resp.GetPeers() {
		if p.GetRaftAddress() == "" || p.GetServiceAddress() == "" {
			continue
		}

		peers = append(peers, node.Peer{
			ID:             p.GetId(),
			Address:        p.GetRaftAddress(),
			ServiceAddress: p.GetServiceAddress(),
		})
	}

	if len(peers) == 0 {
		return nil, fmt.Errorf("no peers with addresses found from %s", raftAddr)
	}

	return peers, nil
}

const (
	// Fixed memory estimates for components not directly configurable.
	goRuntimeEstimate int64 = 200 << 20 // 200 MiB — GC, stacks, goroutines

	// FSM cache heuristics: each raft entry touches ~30 unique cache keys
	// across 9 AttributeCache instances, averaging ~300 bytes per entry.
	// Total = 2 generations * RotationThreshold * keysPerEntry * bytesPerKey.
	fsmCacheKeysPerEntry int64 = 30
	fsmCacheBytesPerKey  int64 = 300
	fsmCacheGenerations  int64 = 2
)

func logMemoryEstimate(logger logging.Logger, cfg *bootstrap.Config, memlimit int64) {
	mib := func(b int64) int64 { return b / (1 << 20) }

	pebbleCache := cfg.PebbleConfig.CacheSize
	memtables := int64(cfg.PebbleConfig.MemTableSize) * int64(cfg.PebbleConfig.MemTableStopWritesThreshold)

	readIndexCache := cfg.ReadIndexConfig.PebbleConfig.CacheSize
	readIndexMemtables := int64(cfg.ReadIndexConfig.PebbleConfig.MemTableSize) * int64(cfg.ReadIndexConfig.PebbleConfig.MemTableStopWritesThreshold)

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

	// Bloom filter memory: m = -n * ln(p) / (ln2)^2 bits per filter.
	var bloomTotal int64
	for _, tc := range []*commonpb.BloomTypeConfig{
		cfg.BloomConfig.GetBloomVolumes(), cfg.BloomConfig.GetBloomMetadata(),
		cfg.BloomConfig.GetBloomReferences(), cfg.BloomConfig.GetBloomLedgers(),
		cfg.BloomConfig.GetBloomBoundaries(), cfg.BloomConfig.GetBloomTransactions(),
		cfg.BloomConfig.GetBloomSinkConfigs(), cfg.BloomConfig.GetBloomNumscriptVersions(),
		cfg.BloomConfig.GetBloomNumscriptContents(),
		cfg.BloomConfig.GetBloomLedgerMetadata(),
		cfg.BloomConfig.GetBloomPreparedQueries(),
		cfg.BloomConfig.GetBloomIndexes(),
	} {
		if tc.GetExpectedKeys() > 0 && tc.GetFpRate() > 0 {
			bits := -float64(tc.GetExpectedKeys()) * math.Log(tc.GetFpRate()) / (math.Ln2 * math.Ln2)
			bloomTotal += int64(bits) / 8
		}
	}

	total := pebbleCache + memtables + readIndexCache + readIndexMemtables + transportTotal + fsmCache + bloomTotal + goRuntimeEstimate

	logger.Infof(
		"Memory estimate: pebbleCache=%dMiB memtables=%dMiB readIndexCache=%dMiB readIndexMemtables=%dMiB transport=%dMiB fsmCache=%dMiB bloom=%dMiB goRuntime=%dMiB total=%dMiB",
		mib(pebbleCache), mib(memtables),
		mib(readIndexCache), mib(readIndexMemtables),
		mib(transportTotal), mib(fsmCache), mib(bloomTotal), mib(goRuntimeEstimate), mib(total),
	)

	if memlimit != math.MaxInt64 && total > memlimit {
		logger.Errorf(
			"WARNING: estimated memory usage (%dMiB) exceeds GOMEMLIMIT (%dMiB) — risk of OOM. Consider increasing memory limits or reducing pebble-cache-size / pebble-memtable-size.",
			mib(total), mib(memlimit),
		)
	}
}

// registerPebbleFlags registers the common Pebble flags with the given prefix.
// Flag names are "{prefix}-memtable-size", "{prefix}-cache-size", etc.
func registerPebbleFlags(cmd *cobra.Command, prefix string, defaults pebblecfg.Config) {
	p := prefix + "-"
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), p+"memtable-size", 0, fmt.Sprintf("Pebble memtable size (default: %s)", bytesize.ByteSize(defaults.MemTableSize)))
	cmd.Flags().Int(p+"memtable-stop-writes-threshold", 0, fmt.Sprintf("Pebble memtable count before stopping writes (default: %d)", defaults.MemTableStopWritesThreshold))
	cmd.Flags().Int(p+"l0-compaction-threshold", 0, fmt.Sprintf("Pebble L0 file count to trigger compaction (default: %d)", defaults.L0CompactionThreshold))
	cmd.Flags().Int(p+"l0-stop-writes-threshold", 0, fmt.Sprintf("Pebble L0 file count before stopping writes (default: %d)", defaults.L0StopWritesThreshold))
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), p+"lbase-max-bytes", 0, fmt.Sprintf("Pebble L1 max size (default: %s)", bytesize.ByteSize(defaults.LBaseMaxBytes)))
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), p+"cache-size", 0, fmt.Sprintf("Pebble block cache size (default: %s)", bytesize.ByteSize(defaults.CacheSize)))
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), p+"target-file-size", 0, fmt.Sprintf("Pebble SST file target size (default: %s)", bytesize.ByteSize(defaults.TargetFileSize)))
	bytesize.ByteSizeVar(cmd, new(bytesize.ByteSize), p+"bytes-per-sync", 0, fmt.Sprintf("Pebble bytes written before sync (default: %s)", bytesize.ByteSize(defaults.BytesPerSync)))
	cmd.Flags().Int(p+"max-concurrent-compactions", 0, fmt.Sprintf("Pebble max concurrent compactions (default: %d)", defaults.MaxConcurrentCompactions))
	cmd.Flags().String(p+"compression", "", fmt.Sprintf("Pebble per-level compression L0-L6, comma-separated (none|snappy|zstd|fastest|fast|balanced|good|default) (default: %s)", defaults.Compression))
}

// loadBasePebbleConfig loads the common Pebble config from flags with the given prefix.
func loadBasePebbleConfig(cmd *cobra.Command, prefix string, defaults pebblecfg.Config) pebblecfg.Config {
	p := prefix + "-"

	getByteSize := func(flag string, def int64) int64 {
		if val := bytesize.Get(cmd, flag); val != 0 {
			return val.Int64()
		}

		return def
	}

	getInt := func(flag string, def int) int {
		if val, _ := cmd.Flags().GetInt(flag); val != 0 {
			return val
		}

		return def
	}

	compression := defaults.Compression
	if s, _ := cmd.Flags().GetString(p + "compression"); s != "" {
		parsed, err := pebblecfg.ParseLevelCompression(s)
		if err != nil {
			panic(fmt.Sprintf("invalid %scompression flag: %v", p, err))
		}
		compression = parsed
	}

	return pebblecfg.Config{
		MemTableSize:                uint64(getByteSize(p+"memtable-size", int64(defaults.MemTableSize))),
		MemTableStopWritesThreshold: getInt(p+"memtable-stop-writes-threshold", defaults.MemTableStopWritesThreshold),
		L0CompactionThreshold:       getInt(p+"l0-compaction-threshold", defaults.L0CompactionThreshold),
		L0StopWritesThreshold:       getInt(p+"l0-stop-writes-threshold", defaults.L0StopWritesThreshold),
		LBaseMaxBytes:               getByteSize(p+"lbase-max-bytes", defaults.LBaseMaxBytes),
		CacheSize:                   getByteSize(p+"cache-size", defaults.CacheSize),
		TargetFileSize:              getByteSize(p+"target-file-size", defaults.TargetFileSize),
		BytesPerSync:                int(getByteSize(p+"bytes-per-sync", int64(defaults.BytesPerSync))),
		MaxConcurrentCompactions:    getInt(p+"max-concurrent-compactions", defaults.MaxConcurrentCompactions),
		Compression:                 compression,
	}
}

// loadPebbleConfig loads Pebble configuration from command flags with defaults.
func loadPebbleConfig(cmd *cobra.Command) dal.Config {
	cfg := dal.DefaultConfig()
	cfg.Config = loadBasePebbleConfig(cmd, "pebble", cfg.Config)

	getDuration := func(flag string, def time.Duration) time.Duration {
		if val, _ := cmd.Flags().GetDuration(flag); val != 0 {
			return val
		}

		return def
	}

	getInt := func(flag string, def int) int {
		if val, _ := cmd.Flags().GetInt(flag); val != 0 {
			return val
		}

		return def
	}

	getByteSize := func(flag string, def int) int {
		if val := bytesize.Get(cmd, flag); val != 0 {
			return val.Int()
		}

		return def
	}

	cfg.WALBytesPerSync = getByteSize("pebble-wal-bytes-per-sync", cfg.WALBytesPerSync)
	cfg.WALMinSyncInterval = getDuration("pebble-wal-min-sync-interval", cfg.WALMinSyncInterval)
	cfg.MaxCheckpoints = getInt("pebble-max-checkpoints", cfg.MaxCheckpoints)

	if disableWAL, _ := cmd.Flags().GetBool("pebble-disable-wal"); disableWAL {
		cfg.DisableWAL = true
	}

	if dir, _ := cmd.Flags().GetString("pebble-wal-failover-dir"); dir != "" {
		cfg.WALFailoverDir = dir
	}

	// Value separation
	if enabled, _ := cmd.Flags().GetBool("pebble-value-separation"); enabled {
		cfg.ValueSeparation.Enabled = true
	}

	cfg.ValueSeparation.MinimumSize = getByteSize("pebble-value-separation-min-size", cfg.ValueSeparation.MinimumSize)
	cfg.ValueSeparation.MaxBlobReferenceDepth = getInt("pebble-value-separation-max-depth", cfg.ValueSeparation.MaxBlobReferenceDepth)
	cfg.ValueSeparation.RewriteMinimumAge = getDuration("pebble-value-separation-rewrite-age", cfg.ValueSeparation.RewriteMinimumAge)

	if ratio, _ := cmd.Flags().GetFloat64("pebble-value-separation-garbage-ratio"); ratio != 0 {
		cfg.ValueSeparation.TargetGarbageRatio = ratio
	}

	return cfg
}

// loadReadIndexPebbleConfig loads Pebble configuration for the read index from command flags.
func loadReadIndexPebbleConfig(cmd *cobra.Command) readstore.Config {
	return loadBasePebbleConfig(cmd, "read-index", readstore.DefaultConfig())
}

// bloomFlagNames lists per-attribute-type names for bloom filter flag registration.
var bloomFlagNames = []string{
	"volumes",
	"metadata",
	"references",
	"ledgers",
	"boundaries",
	"transactions",
	"sink-configs",
	"numscript-versions",
	"numscript-contents",
	"ledger-metadata",
	"prepared-queries",
	"indexes",
}

// registerBloomFlags registers per-attribute-type bloom filter flags.
func registerBloomFlags(cmd *cobra.Command) {
	for _, name := range bloomFlagNames {
		cmd.Flags().Uint(
			fmt.Sprintf("bloom-%s-expected-keys", name), 0,
			fmt.Sprintf("Expected unique keys for %s bloom filter (0 = disable this type)", name),
		)
		cmd.Flags().Float64(
			fmt.Sprintf("bloom-%s-fp-rate", name), 0,
			fmt.Sprintf("False positive rate for %s bloom filter (0 = use 0.01 when enabled)", name),
		)
	}
}

// loadBloomConfig builds bloom filter configuration from per-type CLI flags
// and writes them into a ClusterConfig proto.
func loadBloomConfig(cmd *cobra.Command, cfg *commonpb.ClusterConfig) {
	load := func(name string) *commonpb.BloomTypeConfig {
		expectedKeys, _ := cmd.Flags().GetUint(fmt.Sprintf("bloom-%s-expected-keys", name))
		fpRate, _ := cmd.Flags().GetFloat64(fmt.Sprintf("bloom-%s-fp-rate", name))

		if expectedKeys == 0 {
			return nil
		}

		// FPRate must be > 0 when filter is enabled.
		if fpRate == 0 {
			fpRate = 0.01
		}

		return &commonpb.BloomTypeConfig{
			ExpectedKeys: uint64(expectedKeys),
			FpRate:       fpRate,
		}
	}

	cfg.BloomVolumes = load("volumes")
	cfg.BloomMetadata = load("metadata")
	cfg.BloomReferences = load("references")
	cfg.BloomLedgers = load("ledgers")
	cfg.BloomBoundaries = load("boundaries")
	cfg.BloomTransactions = load("transactions")
	cfg.BloomSinkConfigs = load("sink-configs")
	cfg.BloomNumscriptVersions = load("numscript-versions")
	cfg.BloomNumscriptContents = load("numscript-contents")
	cfg.BloomLedgerMetadata = load("ledger-metadata")
	cfg.BloomPreparedQueries = load("prepared-queries")
	cfg.BloomIndexes = load("indexes")
}
