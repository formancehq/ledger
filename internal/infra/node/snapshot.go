package node

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// MemorySnapshotFetcher is an optional interface that transports can implement
// to support fetching large memory snapshots via streaming RPC.
// DefaultTransport implements this; ChannelTransport (used in tests) does not.
type MemorySnapshotFetcher interface {
	// FetchRemoteMemorySnapshot fetches the full snapshot data from the leader
	// via the FetchMemorySnapshot streaming RPC.
	FetchRemoteMemorySnapshot(leaderID uint64, index, term uint64) ([]byte, error)
}

// snapshotUnwrapResult holds the result of unwrapping a NodeSnapshot.
type snapshotUnwrapResult struct {
	fsmData       []byte
	peerAddresses []*raftcmdpb.PeerAddress
	isReference   bool
	sizeHint      uint64
}

// unwrapSnapshot extracts FSM snapshot data and peer addresses from a NodeSnapshot.
// When isReference is true, fsmData is empty and the caller must fetch the full
// snapshot via FetchMemorySnapshot RPC.
func unwrapSnapshot(data []byte) (*snapshotUnwrapResult, error) {
	ns := &raftcmdpb.NodeSnapshot{}

	err := ns.UnmarshalVT(data)
	if err != nil {
		return nil, err
	}

	return &snapshotUnwrapResult{
		fsmData:       ns.GetFsmSnapshot(),
		peerAddresses: ns.GetPeerAddresses(),
		isReference:   ns.GetIsReference(),
		sizeHint:      ns.GetSizeHint(),
	}, nil
}
