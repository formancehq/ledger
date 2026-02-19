package application

import (
	"fmt"
	"net"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/service/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

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

type Config struct {
	RaftConfig        node.NodeConfig
	Debug             bool
	HTTPPort          int
	GRPCPort          int
	TransportConfig   node.TransportConfig
	DataDir           string
	PebbleConfig      data.Config
	HealthConfig      HealthConfig
	ClusterID         string
	AuditEnabled      bool
	AdmissionMetrics  bool
	ReceiptSigningKey string
	ColdStorageConfig coldstorage.Config
	TLSConfig          TLSConfig
	Restore            bool
	NumscriptCacheSize int
}

func (c Config) Validate() error {
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
