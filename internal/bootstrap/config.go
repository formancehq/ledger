package bootstrap

import (
	"encoding/json"
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
	// OIDCDiscoveryTimeout bounds the OIDC discovery + JWKS HTTP calls made
	// during startup. A slow or blackholed issuer would otherwise hang the
	// process indefinitely (the go-libs path injects http.DefaultClient with
	// no Timeout, and the local fallback calls oidc.Discover with
	// context.Background()). 0 keeps the legacy unbounded behavior; the
	// default (set in cmd/server/server.go) is 10s.
	OIDCDiscoveryTimeout time.Duration
}

// TLSMode controls the TLS posture of gRPC servers and the inter-node dialer.
//
//   - disabled: plaintext only.
//   - optional: server accepts both TLS and plaintext (cmux dual-listener);
//     client probes TLS first then falls back to plaintext per-peer. Used as
//     a transitional state by the operator during a TLS toggle so that pods
//     mid-rolling-update can still talk to peers on the other side of the
//     transition.
//   - required: TLS only (strict).
type TLSMode string

const (
	TLSModeDisabled TLSMode = "disabled"
	TLSModeOptional TLSMode = "optional"
	TLSModeRequired TLSMode = "required"
)

// Valid reports whether m is one of the recognized TLS modes.
func (m TLSMode) Valid() bool {
	switch m {
	case TLSModeDisabled, TLSModeOptional, TLSModeRequired:
		return true
	}

	return false
}

// AllowsTLS reports whether the mode permits TLS connections (server side)
// or attempts them (client side).
func (m TLSMode) AllowsTLS() bool {
	return m == TLSModeOptional || m == TLSModeRequired
}

// AllowsPlaintext reports whether the mode permits plaintext connections.
func (m TLSMode) AllowsPlaintext() bool {
	return m == TLSModeDisabled || m == TLSModeOptional
}

type TLSConfig struct {
	Mode     TLSMode
	CertFile string
	KeyFile  string
	CAFile   string
	// RequireClientCert switches inbound mTLS from VerifyClientCertIfGiven
	// (the default — a missing client cert is accepted, authentication then
	// relies on cluster-secret / JWT) to RequireAndVerifyClientCert, where a
	// peer that fails to present a CA-signed certificate is rejected at the
	// TLS layer before any application code runs. Only honored when CAFile
	// is set — required by config validation.
	RequireClientCert bool
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

// Validate enforces that snapshot sync flags name workable values. The
// snapshot fetcher uses retryCount as `for attempt := range f.retryCount`
// (zero means no attempts → the node never catches up), and parallelism is
// passed to errgroup.SetLimit where 0 means "serial" and negative means
// "unlimited"; both extremes are operational failure modes.
func (c SnapshotSyncConfig) Validate() error {
	if c.Parallelism < 1 || c.Parallelism > 1024 {
		return fmt.Errorf("--snapshot-parallelism must be in [1, 1024] (got %d)", c.Parallelism)
	}

	if c.RetryCount < 1 || c.RetryCount > 100 {
		return fmt.Errorf("--snapshot-retry-count must be in [1, 100] (got %d)", c.RetryCount)
	}

	if c.FileRetryCount < 1 || c.FileRetryCount > 100 {
		return fmt.Errorf("--snapshot-file-retry-count must be in [1, 100] (got %d)", c.FileRetryCount)
	}

	if c.SessionTTL <= 0 {
		return fmt.Errorf("--snapshot-session-ttl must be > 0 (got %s)", c.SessionTTL)
	}

	return nil
}

type Config struct {
	RaftConfig             node.NodeConfig
	Debug                  bool
	HTTPPort               int
	GRPCPort               int
	TransportConfig        node.TransportConfig
	DataDir                string
	PebbleConfig           dal.Config
	HealthConfig           HealthConfig
	ClusterID              string
	AdmissionMetrics       bool
	ReceiptSigningKey      string
	ResponseSigningKeyFile string
	ColdStorageConfig      coldstorage.Config
	PoolConfig             transport.PoolConfig
	TLSConfig              TLSConfig
	AuthConfig             AuthFlagConfig
	ClusterSecret          string
	Restore                bool
	// RestoreListen is the bind host for restore-mode servers (gRPC + HTTP).
	// Defaults (via EffectiveRestoreListen) to "127.0.0.1" so the destructive
	// restore RPCs are not exposed on the public network unless the operator
	// opts in. Set to "0.0.0.0" or a specific external interface to allow
	// remote calls (requires TLS + upstream firewalling — restore RPCs are
	// not authenticated today).
	RestoreListen string
	// RestoreDownloadParallelism caps the number of concurrent S3 file downloads
	// during an async restore. 0 means use the default (16). The server clamps
	// the effective value to [1, 64].
	RestoreDownloadParallelism int
	// SpoolSegmentMaxBytes caps the size of a spool segment before rotation
	// (sealing). 0 means use the spool default (256Mi).
	SpoolSegmentMaxBytes int64
	NumscriptCacheSize   int
	MirrorMaxBatchSize   int
	// MaxExecutionPlanSize caps the number of AttributePlan entries an
	// ExecutionPlan may carry. 0 disables the cap. See plan.Builder.
	MaxExecutionPlanSize        int
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

// EffectiveRestoreListen returns the bind host for restore mode, falling
// back to 127.0.0.1 when the flag is unset. Used by both the gRPC and HTTP
// restore listeners so the two stay in sync.
func (c Config) EffectiveRestoreListen() string {
	if c.RestoreListen == "" {
		return "127.0.0.1"
	}

	return c.RestoreListen
}

// RedactedSecretPlaceholder replaces inline secret values when the config is
// rendered (e.g. printed at startup), so cluster secrets and signing keys never
// leak into terminals, CI logs, or support bundles.
const RedactedSecretPlaceholder = "***REDACTED***"

// redactedCopy returns a copy of the config with inline secrets blanked, as a
// type that carries no Marshal methods — so json/yaml render it as plain fields
// without recursing back into MarshalJSON/MarshalYAML. A non-empty secret
// becomes the placeholder; an empty one is left empty to preserve "is it
// configured?" visibility. New secret fields must be added here (single source
// of truth) so both encoders stay redacted.
func (c Config) redactedCopy() configAlias {
	redacted := configAlias(c)

	if redacted.ReceiptSigningKey != "" {
		redacted.ReceiptSigningKey = RedactedSecretPlaceholder
	}

	if redacted.ClusterSecret != "" {
		redacted.ClusterSecret = RedactedSecretPlaceholder
	}

	// PoolConfig.AuthToken carries the inter-node bearer token (set to the
	// cluster secret); redact it too even though it is populated after startup.
	if redacted.PoolConfig.AuthToken != "" {
		redacted.PoolConfig.AuthToken = RedactedSecretPlaceholder
	}

	return redacted
}

// configAlias has Config's fields but none of its methods, breaking the
// marshal recursion in redactedCopy.
type configAlias Config

// MarshalYAML redacts inline secret fields before the config is serialized.
// Config is only marshaled for display, so redacting at the type level
// guarantees secrets are never rendered regardless of the call site or format.
func (c Config) MarshalYAML() (any, error) {
	return c.redactedCopy(), nil
}

// MarshalJSON mirrors MarshalYAML so JSON output (e.g. a future --output json)
// is redacted too — encoding/json does not honor MarshalYAML.
func (c Config) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.redactedCopy())
}

func (c Config) Validate() error {
	if c.ClusterID == "" {
		return errors.New("--cluster-id is required")
	}

	if err := c.validateTLSConfig(); err != nil {
		return err
	}

	// Reject cluster-secret without TLS — the secret would be sent in plaintext.
	if c.ClusterSecret != "" && c.TLSConfig.Mode == TLSModeDisabled {
		return errors.New("--cluster-secret requires TLS (set --tls-mode to optional or required and provide --tls-cert-file / --tls-key-file); the secret would be sent in plaintext otherwise")
	}

	if err := c.validateAuthConfig(); err != nil {
		return err
	}

	if err := c.RaftConfig.Validate(); err != nil {
		return err
	}

	if err := c.TransportConfig.Validate(); err != nil {
		return err
	}

	return c.SnapshotSyncConfig.Validate()
}

// validateTLSConfig enforces TLS configuration invariants.
func (c Config) validateTLSConfig() error {
	if !c.TLSConfig.Mode.Valid() {
		return fmt.Errorf("--tls-mode must be one of %q, %q, %q (got %q)",
			TLSModeDisabled, TLSModeOptional, TLSModeRequired, c.TLSConfig.Mode)
	}

	if c.TLSConfig.Mode == TLSModeDisabled {
		return nil
	}

	if c.TLSConfig.CertFile == "" || c.TLSConfig.KeyFile == "" {
		return fmt.Errorf("--tls-mode=%s requires --tls-cert-file and --tls-key-file", c.TLSConfig.Mode)
	}

	if c.TLSConfig.RequireClientCert && c.TLSConfig.CAFile == "" {
		return errors.New("--tls-require-client-cert requires --tls-ca-file (cannot verify peer certs without a CA)")
	}

	return nil
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
