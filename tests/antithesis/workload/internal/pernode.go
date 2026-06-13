package internal

import (
	"context"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// metadataKeyConsistency mirrors internal/adapter/grpc/consistency.go: the
// server reads the read-consistency level from this incoming-metadata key.
const metadataKeyConsistency = "x-consistency"

// consistencyStale mirrors internal/adapter/grpc/consistency.go ConsistencyStale.
// A stale read is served directly from the receiving node's local FSM-owned
// Pebble store without a ReadIndex barrier or leader forwarding, so the read is
// attributable to exactly the node that received the RPC. This is what makes
// per-node connections (one single-target conn per address) the only way to
// compare the same data across all nodes.
const consistencyStale = "stale"

// PerNodeConn is a single-target gRPC connection to one ledger node. Unlike the
// shared round-robin connection from NewGRPCConn, a PerNodeConn always reaches
// the same node, so a stale read served from it is attributable to that node.
type PerNodeConn struct {
	// Addr is the dial target (e.g. "ledger-0:8888"), always populated.
	Addr string
	// NodeID is the Raft node ID resolved once via GetClusterState. Zero if it
	// could not be resolved (node down at dial time); Addr is still usable.
	NodeID uint32

	conn    *grpc.ClientConn
	Bucket  servicepb.BucketServiceClient
	Cluster clusterpb.ClusterServiceClient
}

// Close releases the underlying gRPC connection.
func (p *PerNodeConn) Close() error {
	return p.conn.Close()
}

// PerNodeConns is a set of single-target connections, one per node address.
type PerNodeConns []*PerNodeConn

// Close releases every connection, ignoring individual close errors.
func (conns PerNodeConns) Close() {
	for _, c := range conns {
		_ = c.conn.Close()
	}
}

// nodeAddresses parses LEDGER_PER_NODE_GRPC_ADDR (a comma-separated list such as
// "ledger-0:8888,ledger-1:8888,ledger-2:8888") into individual addresses.
// Empty entries are dropped. Falls back to LEDGER_GRPC_ADDR, then the
// single-node default.
func nodeAddresses() []string {
	target := os.Getenv("LEDGER_PER_NODE_GRPC_ADDR")
	if target == "" {
		target = os.Getenv("LEDGER_GRPC_ADDR")
	}
	if target == "" {
		target = "localhost:15100"
	}

	var addrs []string
	for addr := range strings.SplitSeq(target, ",") {
		addr = strings.TrimSpace(addr)
		if addr != "" {
			addrs = append(addrs, addr)
		}
	}

	return addrs
}

// DialPerNode opens one single-target gRPC connection per node address in
// LEDGER_PER_NODE_GRPC_ADDR. Each connection uses the same insecure credentials
// and UNAVAILABLE retry interceptors as NewGRPCConn but dials a SINGLE target
// (no round-robin), so reads are attributable to the node that received them.
//
// Dialing is error-tolerant: grpc.NewClient is lazy and never fails on an
// unreachable address, so a node that is down is simply skipped at read time
// (its RPCs return UNAVAILABLE, which callers treat as transient). The caller
// must Close the returned set.
//
// Each connection's NodeID is best-effort resolved once from the local node's
// GetClusterState; if the node is unreachable at dial time the NodeID stays 0
// and the connection is still returned (keyed by Addr).
func DialPerNode(ctx context.Context) (PerNodeConns, error) {
	addrs := nodeAddresses()

	serviceConfig := `{
		"methodConfig": [{
			"name": [{}],
			"retryPolicy": {
				"MaxAttempts": 50,
				"InitialBackoff": "0.2s",
				"MaxBackoff": "2s",
				"BackoffMultiplier": 1.5,
				"RetryableStatusCodes": ["UNAVAILABLE"]
			}
		}]
	}`

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
		grpc.WithUnaryInterceptor(retryUnaryInterceptor),
		grpc.WithStreamInterceptor(retryStreamInterceptor),
	}

	var conns PerNodeConns
	for _, addr := range addrs {
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			// A malformed target fails synchronously; skip it rather than
			// aborting the whole set (other nodes may still be dialable).
			continue
		}

		conns = append(conns, &PerNodeConn{
			Addr:    addr,
			conn:    conn,
			Bucket:  servicepb.NewBucketServiceClient(conn),
			Cluster: clusterpb.NewClusterServiceClient(conn),
		})
	}

	// Resolve each connection's Raft node ID by matching its dial address
	// against the cluster's advertised service addresses. GetClusterState{0}
	// routes to the leader, whose Nodes list carries (id, service_address) for
	// every peer; ServiceAdvertiseAddr is "<raft-host>:<grpc-port>", which is
	// exactly the form of the LEDGER_GRPC_ADDR entries we dial. NodeID is
	// required to poll a node's OWN applied index (GetClusterState{NodeId:id}
	// routes to that node), so a connection whose ID cannot be resolved is left
	// at 0 and callers skip it rather than compare at the leader's index.
	for _, c := range conns {
		state, err := c.Cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		if err != nil {
			continue
		}

		for _, n := range state.GetNodes() {
			if n.GetServiceAddress() == c.Addr {
				c.NodeID = n.GetId()

				break
			}
		}

		// One successful leader response carries the full node list, so the
		// remaining connections can be resolved from the same snapshot.
		if len(state.GetNodes()) > 0 {
			byAddr := make(map[string]uint32, len(state.GetNodes()))
			for _, n := range state.GetNodes() {
				byAddr[n.GetServiceAddress()] = n.GetId()
			}

			for _, other := range conns {
				if other.NodeID == 0 {
					other.NodeID = byAddr[other.Addr]
				}
			}

			break
		}
	}

	return conns, nil
}

// WithStaleConsistency returns a copy of ctx carrying x-consistency: stale
// outgoing metadata, so the receiving node serves the read from its own local
// FSM store without forwarding to the leader.
func WithStaleConsistency(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, metadataKeyConsistency, consistencyStale)
}
