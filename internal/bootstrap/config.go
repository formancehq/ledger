package bootstrap

import (
	"fmt"
	"net"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/infra/transport"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// AuthFlagConfig captures the authentication flag values without runtime objects.
type AuthFlagConfig struct {
	Enabled          bool
	Issuer           string
	Service          string
	Ed25519KeysFile  string
	ScopeMappingFile string // path to JSON file mapping virtual scopes to granular scopes
	ScopeMappingJSON string // raw JSON string mapping (used by operator, env var AUTH_SCOPE_MAPPING)
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

// ReadIndexConfig holds configuration for the bbolt read index store.
type ReadIndexConfig struct {
	Dir string // empty = default (<data-dir>/read-indexes/)
}

type Config struct {
	RaftConfig                 node.NodeConfig
	Debug                      bool
	HTTPPort                   int
	GRPCPort                   int
	TransportConfig            node.TransportConfig
	DataDir                    string
	PebbleConfig               dal.Config
	HealthConfig               HealthConfig
	ClusterID                  string
	AdmissionMetrics           bool
	ReceiptSigningKey          string
	ResponseSigningKeyFile     string
	ColdStorageConfig          coldstorage.Config
	PoolConfig                 transport.PoolConfig
	TLSConfig                  TLSConfig
	AuthConfig                 AuthFlagConfig
	Restore                    bool
	NumscriptCacheSize         int
	MirrorMaxBatchSize         int
	UnsafeSkipConfigValidation bool
	ReadIndexConfig            ReadIndexConfig
	QueryProfileThreshold      time.Duration
}

func (c Config) Validate() error {
	if c.ClusterID == "" {
		return fmt.Errorf("--cluster-id is required")
	}
	return c.RaftConfig.Validate()
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
