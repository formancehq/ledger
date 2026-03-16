package node

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// snapshotUnwrapResult holds the result of unwrapping a NodeSnapshot.
type snapshotUnwrapResult struct {
	fsmData       []byte
	peerAddresses []*raftcmdpb.PeerAddress
}

// unwrapSnapshot extracts FSM snapshot data and peer addresses from a NodeSnapshot.
func unwrapSnapshot(data []byte) (*snapshotUnwrapResult, error) {
	ns := &raftcmdpb.NodeSnapshot{}

	err := ns.UnmarshalVT(data)
	if err != nil {
		return nil, err
	}

	return &snapshotUnwrapResult{
		fsmData:       ns.GetFsmSnapshot(),
		peerAddresses: ns.GetPeerAddresses(),
	}, nil
}
