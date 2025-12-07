package http

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.etcd.io/etcd/raft/v3"
)

type Server struct {
	logger        logging.Logger
	ledgerService service.Ledger
	cluster       ClusterClient
}

// todo: use in place of ClusterClient
type LeaderClient interface {
	Snapshot() error
	IsHealthy() bool
	GetClusterState() (*ClusterState, error)
	CreateLedger(bucketName, ledgerName string, metadata metadata.Metadata) error
	GetLedger(bucketName, ledgerName string) (service.LedgerInfo, bool, error)
	GetLedgerByName(ledgerName string) (service.LedgerInfo, string, bool, error)
	FindBucketForLedger(ledgerName string) (string, error)
	GetAllLedgers(bucketName string) (map[string]service.LedgerInfo, error)
	CreateBucket(name, driver string, config map[string]interface{}) error
	DeleteBucket(name string) error
	CreateBucketSnapshot(bucketName string) error
	GetAllBuckets() map[string]service.BucketInfo
	GetBucket(name string) (service.BucketInfo, bool)
	GetBucketWithRaftState(name string) (*BucketWithRaftState, error)
}

// ClusterClient is an interface for cluster operations
type ClusterClient interface {
	Snapshot() error
	IsHealthy() bool
	GetClusterState() (*ClusterState, error)
	CreateLedger(bucketName, ledgerName string, metadata metadata.Metadata) error
	GetLedger(bucketName, ledgerName string) (service.LedgerInfo, bool, error)
	GetLedgerByName(ledgerName string) (service.LedgerInfo, string, bool, error)
	FindBucketForLedger(ledgerName string) (string, error)
	GetAllLedgers(bucketName string) (map[string]service.LedgerInfo, error)
	CreateBucket(name, driver string, config map[string]interface{}) error
	DeleteBucket(name string) error
	CreateBucketSnapshot(bucketName string) error
	GetAllBuckets() map[string]service.BucketInfo
	GetBucket(name string) (service.BucketInfo, bool)
	GetBucketWithRaftState(name string) (*BucketWithRaftState, error)
	GetLeaderGRPCClient() service.SystemServiceClient
	GetLeaderLedgerGRPCClient() service.LedgerServiceClient
	GetRaft() *raft.RawNode
}

// ClusterState represents the state of the Raft cluster
type ClusterState struct {
	State     string     `json:"state"`     // Leader, Follower, Candidate, Shutdown
	Leader    string     `json:"leader"`    // ID of the current leader (empty if no leader)
	Nodes     []NodeInfo `json:"nodes"`     // List of all nodes in the cluster
	LocalNode string     `json:"localNode"` // ID of the local node
}

// NodeInfo represents information about a node in the cluster
type NodeInfo struct {
	ID       string `json:"id"`       // Node ID
	Address  string `json:"address"`  // Node address
	Suffrage string `json:"suffrage"` // Voter or Nonvoter
}

// BucketWithRaftState represents a bucket with its Raft cluster state
type BucketWithRaftState struct {
	service.BucketInfo
	RaftState *ClusterState `json:"raftState"`
}

// NewServer creates a new server instance (used by handlers)
func NewServer(logger logging.Logger, ledgerService service.Ledger, cluster ClusterClient) *Server {
	return &Server{
		logger:        logger,
		ledgerService: ledgerService,
		cluster:       cluster,
	}
}
