package bootstrap

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
)

// AuthFlagConfig captures the authentication flag values without runtime objects.
type AuthFlagConfig struct {
	Enabled          bool
	Issuer           string
	Service          string
	Ed25519KeysFile  string
	ScopeMappingFile string // path to JSON file mapping virtual scopes to granular scopes
	ScopeMappingJSON string // raw JSON string mapping (used by operator, env var AUTH_SCOPE_MAPPING)
	// AnonymousScopes is a CSV of granular scopes (or "*:read" / "*:write"
	// wildcards) granted to requests that arrive without a bearer token.
	// Use "*:read" to enable the "writes-only" auth mode: reads are public,
	// writes require a valid token. Empty (default) preserves the historical
	// strict behavior where every request must authenticate.
	AnonymousScopes string
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
	CAFile   string
}

type HealthConfig struct {
	Interval           time.Duration
	WALThreshold       float64
	DataThreshold      float64
	ClockSkewThreshold time.Duration
}

// ReadIndexConfig holds configuration for the Pebble read index store.
type ReadIndexConfig struct {
	Dir          string           // empty = default (<data-dir>/read-indexes/)
	BatchSize    int              // log entries per Pebble batch commit (0 = default 1000)
	PebbleConfig pebblecfg.Config // Pebble tunables for the read index
}

// SnapshotSyncConfig holds configuration for the session-based snapshot sync protocol.
type SnapshotSyncConfig struct {
	SessionTTL     time.Duration // Server-side session lifetime before reaper cleanup
	Parallelism    int           // Number of parallel file fetch workers on the client
	RetryCount     int           // Session-level retry attempts on transient errors
	FileRetryCount int           // Per-file retry attempts on transient stream errors
}

type Config struct {
	RaftConfig                  node.NodeConfig
	Debug                       bool
	HTTPPort                    int
	GRPCPort                    int
	TransportConfig             node.TransportConfig
	DataDir                     string
	PebbleConfig                dal.Config
	HealthConfig                HealthConfig
	ClusterID                   string
	AdmissionMetrics            bool
	ReceiptSigningKey           string
	ResponseSigningKeyFile      string
	ColdStorageConfig           coldstorage.Config
	PoolConfig                  transport.PoolConfig
	TLSConfig                   TLSConfig
	AuthConfig                  AuthFlagConfig
	ClusterSecret               string
	Restore                     bool
	NumscriptCacheSize          int
	MirrorMaxBatchSize          int
	UnsafeSkipConfigValidation  bool
	SentinelMode                bool
	ReadIndexConfig             ReadIndexConfig
	QueryProfileThreshold       time.Duration
	GRPCSlowThreshold           time.Duration
	BloomConfig                 *commonpb.ClusterConfig
	IdempotencyTTL              time.Duration
	IdempotencyEvictionInterval time.Duration
	SnapshotSyncConfig          SnapshotSyncConfig
}

func (c Config) Validate() error {
	if c.ClusterID == "" {
		return errors.New("--cluster-id is required")
	}

	// Reject cluster-secret without TLS — the secret would be sent in plaintext.
	if c.ClusterSecret != "" && !c.TLSConfig.Enabled {
		return errors.New("--cluster-secret requires TLS (configure --tls-cert-file and --tls-key-file); the secret would be sent in plaintext otherwise")
	}

	if err := c.validateAuthConfig(); err != nil {
		return err
	}

	return c.RaftConfig.Validate()
}

// validateAuthConfig enforces authentication configuration invariants.
// It prevents unsafe combinations where credentials are configured but
// authentication is disabled, or where authentication is enabled without
// the required OIDC issuer.
func (c Config) validateAuthConfig() error {
	auth := c.AuthConfig

	if auth.Enabled {
		if auth.Issuer == "" && auth.Ed25519KeysFile == "" {
			return errors.New("--auth-enabled requires either --auth-issuer (OIDC) or --auth-ed25519-keys (Ed25519)")
		}

		return nil
	}

	// Auth is disabled — warn about credentials that will be ignored.
	var unused []string
	if auth.Issuer != "" {
		unused = append(unused, "--auth-issuer")
	}

	if auth.Ed25519KeysFile != "" {
		unused = append(unused, "--auth-ed25519-keys")
	}

	if auth.ScopeMappingFile != "" {
		unused = append(unused, "--auth-scope-mapping-file")
	}

	if auth.ScopeMappingJSON != "" {
		unused = append(unused, "AUTH_SCOPE_MAPPING")
	}

	if len(unused) > 0 {
		return fmt.Errorf("authentication is disabled but the following flags are set: %v; either enable auth with --auth-enabled or remove these flags", unused)
	}

	return nil
}

// ServiceAdvertiseAddr returns the routable gRPC service address for this node.
// It derives the hostname from the Raft advertise address and uses the gRPC port,
// so that other nodes can reach this node's service API.
func (c Config) ServiceAdvertiseAddr() string {
	host, _, err := net.SplitHostPort(c.RaftConfig.AdvertiseAddr)
	if err != nil {
		host = c.RaftConfig.AdvertiseAddr
	}

	return fmt.Sprintf("%s:%d", host, c.GRPCPort)
}
