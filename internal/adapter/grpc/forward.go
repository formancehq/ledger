package grpc

import (
	"errors"
	"fmt"

	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
)

// ErrNodeNotReachable is returned by nodeForwarder.resolve when the requested
// peer has no entry in the service-connection pool — typically a network
// partition / pod restart in flight, recoverable as soon as the pool
// re-converges. convertToGRPCError maps it to codes.Unavailable so SDK
// clients (and the antithesis workload's IsTransient predicate) retry it
// the same way as the other peer-transport transients.
var ErrNodeNotReachable = errors.New("node not reachable")

// nodeForwarder resolves a target node ID to either local handling or a gRPC
// connection for forwarding the request to a peer.
type nodeForwarder struct {
	node        *node.Node
	servicePool *transport.ConnectionPool
}

// resolve returns a gRPC connection to the target node, or nil when the request
// should be handled locally (nodeID is 0 or matches the local node).
func (f *nodeForwarder) resolve(nodeID uint32) (*grpc.ClientConn, error) {
	if nodeID == 0 || uint64(nodeID) == f.node.GetNodeID() {
		return nil, nil
	}

	conn := f.servicePool.GetConnection(uint64(nodeID))
	if conn == nil {
		return nil, fmt.Errorf("%w: node %d", ErrNodeNotReachable, nodeID)
	}

	return conn, nil
}
