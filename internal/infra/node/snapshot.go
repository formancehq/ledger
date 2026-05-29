package node

import (
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// snapshotUnwrapResult holds the result of unwrapping a NodeSnapshot.
type snapshotUnwrapResult struct {
	peerAddresses []*raftcmdpb.PeerAddress
}

// unwrapSnapshot extracts peer addresses from a NodeSnapshot.
func unwrapSnapshot(data []byte) (*snapshotUnwrapResult, error) {
	ns := &raftcmdpb.NodeSnapshot{}

	err := ns.UnmarshalVT(data)
	if err != nil {
		return nil, err
	}

	return &snapshotUnwrapResult{
		peerAddresses: ns.GetPeerAddresses(),
	}, nil
}
