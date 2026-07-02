package membership

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// peerKeyLen is the fixed length of a peer key:
// 1 byte zone + 1 byte sub-prefix + 8 bytes big-endian NodeID.
const peerKeyLen = 1 + 1 + 8

// PeerStore persists Raft cluster membership in Pebble under
// [ZoneGlobal][SubGlobPeers][node_id BE 8] → raftcmdpb.PeerAddress.
//
// It replaces the previous mechanism that carried peer addresses inside the
// Raft WAL snapshot (and the EN-1404 PEER_ADDRESSES side-file). With this
// store, mutations land at ConfChange apply time (hot-path write through the
// node's WriteSession), and recovery reads from this prefix at boot
// (lifecycle path, outside the FSM hot path — invariant 3).
type PeerStore struct {
	store *dal.Store
}

// NewPeerStore returns a PeerStore backed by the given Pebble store.
func NewPeerStore(store *dal.Store) *PeerStore {
	return &PeerStore{store: store}
}

// peerKey builds the Pebble key for the given NodeID.
func peerKey(nodeID uint64) []byte {
	key := make([]byte, peerKeyLen)
	key[0] = dal.ZoneGlobal
	key[1] = dal.SubGlobPeers
	binary.BigEndian.PutUint64(key[2:], nodeID)

	return key
}

// peerKeyRange returns the [lower, upper) bounds for an iteration that
// covers every peer entry. Upper is the next byte after SubGlobPeers so
// the half-open range exactly matches the sub-prefix.
func peerKeyRange() (lower, upper []byte) {
	return []byte{dal.ZoneGlobal, dal.SubGlobPeers},
		[]byte{dal.ZoneGlobal, dal.SubGlobPeers + 1}
}

// Put writes (nodeID, raftAddr, serviceAddr) to Pebble. Called from the
// ConfChange apply path on AddNode / AddLearnerNode / UpdateNode.
func (p *PeerStore) Put(nodeID uint64, raftAddr, serviceAddr string) error {
	session := p.store.OpenWriteSession()
	if err := session.SetProto(peerKey(nodeID), &raftcmdpb.PeerAddress{
		NodeId:         nodeID,
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	}); err != nil {
		_ = session.Cancel()

		return fmt.Errorf("writing peer %d: %w", nodeID, err)
	}

	return session.Commit()
}

// Delete removes the entry for the given NodeID. Called from the ConfChange
// apply path on RemoveNode.
func (p *PeerStore) Delete(nodeID uint64) error {
	session := p.store.OpenWriteSession()
	if err := session.DeleteKey(peerKey(nodeID)); err != nil {
		_ = session.Cancel()

		return fmt.Errorf("deleting peer %d: %w", nodeID, err)
	}

	return session.Commit()
}

// LoadAll iterates every peer entry in Pebble and returns a map keyed by
// NodeID. Called from NewNode at boot to seed node.peerAddresses (lifecycle
// read, not FSM hot path).
//
// An unmarshalling error on any entry surfaces a fatal startup error: a
// corrupt peer entry is an invariant violation, and silently dropping it
// would resurrect the EN-1404 "missing bootstrap voter" failure mode.
func (p *PeerStore) LoadAll() (map[uint64]ConfChangeContext, error) {
	handle, err := p.store.NewDirectReadHandle()
	if err != nil {
		return nil, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	lower, upper := peerKeyRange()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating peers iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	out := make(map[uint64]ConfChangeContext)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != peerKeyLen {
			return nil, fmt.Errorf("invariant: peer key has unexpected length %d (want %d): %x",
				len(key), peerKeyLen, key)
		}

		pa := &raftcmdpb.PeerAddress{}
		if err := pa.UnmarshalVT(iter.Value()); err != nil {
			return nil, fmt.Errorf("invariant: unmarshalling peer at key %x: %w", key, err)
		}

		out[pa.GetNodeId()] = ConfChangeContext{
			RaftAddress:    pa.GetRaftAddress(),
			ServiceAddress: pa.GetServiceAddress(),
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating peers: %w", err)
	}

	return out, nil
}
