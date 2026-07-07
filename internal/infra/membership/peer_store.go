package membership

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// PeerStore persists Raft cluster membership in Pebble under two adjacent
// slices of the Global zone:
//
//   - [ZoneGlobal][SubGlobPeers][node_id BE 8] → raftcmdpb.PeerAddress — the
//     directory of currently-configured peers with their addresses and
//     16-byte instance UUIDs. Mutations land at ConfChange apply time
//     (hot-path write through the FSM's WriteSession); recovery reads from
//     this prefix at boot (lifecycle path, outside the FSM hot path —
//     invariant 3). EN-1413.
//
//   - [ZoneGlobal][SubGlobRemovedMembers][node_id BE 8][instance_id 16] →
//     raftcmdpb.RemovedMemberEntry — tombstones written on removal so a
//     still-alive pod cannot silently rejoin and be auto-promoted.
//     Consulted by JoinAsLearner admission and checkAndPromoteLearners.
//     EN-1045.
//
// Keeping both slices on the same type lets the FSM apply of a
// ConfChangeRemoveNode land both mutations (peer row delete + tombstone
// write) in the same WriteSession, and lets ForceRemoveNode's leader-local
// path do the same in a single Pebble batch (see
// Membership.UnregisterAndBlacklist).
type PeerStore struct {
	store *dal.Store
}

// NewPeerStore returns a PeerStore backed by the given Pebble store.
func NewPeerStore(store *dal.Store) *PeerStore {
	return &PeerStore{store: store}
}

// OpenWriteSession returns a fresh write session on the underlying Pebble
// store. Used by lifecycle paths that need to combine a peer mutation with
// another mutation atomically (see EN-1045 force-remove path).
func (p *PeerStore) OpenWriteSession() *dal.WriteSession {
	return p.store.OpenWriteSession()
}

// ---------------------------------------------------------------------------
// Peer directory ([ZoneGlobal][SubGlobPeers]…)
// ---------------------------------------------------------------------------

// peerKeyLen is the fixed length of a peer key:
// 1 byte zone + 1 byte sub-prefix + 8 bytes big-endian NodeID.
const peerKeyLen = 1 + 1 + 8

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

// Put writes (nodeID, raftAddr, serviceAddr, instanceID) to Pebble. Called
// from the ConfChange apply path on AddNode / AddLearnerNode / UpdateNode.
// instanceID may be empty for bootstrap initial-peer entries and for admin
// AddLearner rows written before the peer boots — those rows get refreshed
// with a real instance_id when the peer later goes through JoinAsLearner
// (EN-1045).
func (p *PeerStore) Put(nodeID uint64, raftAddr, serviceAddr string, instanceID []byte) error {
	session := p.store.OpenWriteSession()
	if err := session.SetProto(peerKey(nodeID), &raftcmdpb.PeerAddress{
		NodeId:         nodeID,
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
		InstanceId:     instanceID,
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

// DeleteInSession removes the entry for the given NodeID using the caller's
// WriteSession. Used by lifecycle paths that need to atomically combine the
// peer delete with other writes (EN-1045 ForceRemoveNode also writes a
// RemovedMemberEntry in the same batch).
func (p *PeerStore) DeleteInSession(session *dal.WriteSession, nodeID uint64) error {
	if err := session.DeleteKey(peerKey(nodeID)); err != nil {
		return fmt.Errorf("session delete peer %d: %w", nodeID, err)
	}

	return nil
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
			InstanceID:     pa.GetInstanceId(),
		}
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating peers: %w", err)
	}

	return out, nil
}

// ---------------------------------------------------------------------------
// Removed-member registry ([ZoneGlobal][SubGlobRemovedMembers]…)
// ---------------------------------------------------------------------------

// removedMemberKeyLen is the fixed length of a removed-member key:
// 1 byte zone + 1 byte sub-prefix + 8 bytes big-endian NodeID + 16 bytes instance UUID.
const removedMemberKeyLen = 1 + 1 + 8 + 16

// Reason strings for RemovedMemberEntry.Reason. Stable public values (they
// surface on the admin list-removed CLI output and cross-node audit); do
// not rename without updating docs/ops.
const (
	// removedReasonConsensus tags entries written by the consensus
	// ConfChangeRemoveNode apply path (Membership.WriteConfChange).
	removedReasonConsensus = "consensus"

	// removedReasonForce tags entries written by ForceRemoveNode's
	// leader-local Pebble batch (bypasses the log; followers converge
	// via next snapshot).
	removedReasonForce = "force"
)

// removedMemberKey builds the Pebble key for (nodeID, instanceID). Panics
// on wrong-length instanceID — an invariant violation the caller must
// catch before reaching here (instanceID is the identity we blacklist on,
// silently truncating or padding would produce wrong keys).
func removedMemberKey(nodeID uint64, instanceID []byte) []byte {
	if len(instanceID) != 16 {
		panic(fmt.Sprintf("removedMemberKey: instance id must be 16 bytes, got %d", len(instanceID)))
	}

	key := make([]byte, removedMemberKeyLen)
	key[0] = dal.ZoneGlobal
	key[1] = dal.SubGlobRemovedMembers
	binary.BigEndian.PutUint64(key[2:10], nodeID)
	copy(key[10:], instanceID)

	return key
}

// removedMemberKeyRange returns the [lower, upper) bounds for iterating
// the whole registry.
func removedMemberKeyRange() (lower, upper []byte) {
	return []byte{dal.ZoneGlobal, dal.SubGlobRemovedMembers},
		[]byte{dal.ZoneGlobal, dal.SubGlobRemovedMembers + 1}
}

// removedMemberNodeIDPrefix returns the [lower, upper) bounds for iterating
// every entry belonging to the given nodeID (any instanceID).
func removedMemberNodeIDPrefix(nodeID uint64) (lower, upper []byte) {
	lo := make([]byte, 10)
	lo[0] = dal.ZoneGlobal
	lo[1] = dal.SubGlobRemovedMembers
	binary.BigEndian.PutUint64(lo[2:], nodeID)

	up := make([]byte, 10)
	up[0] = dal.ZoneGlobal
	up[1] = dal.SubGlobRemovedMembers
	binary.BigEndian.PutUint64(up[2:], nodeID+1)

	return lo, up
}

// MarkRemoved writes a RemovedMemberEntry to the caller's WriteSession
// (typically the in-flight FSM apply batch). The session is committed by
// its owner as part of the surrounding batch. Called from
// Membership.WriteConfChange when it observes a ConfChangeRemoveNode with
// a non-empty context.
func (p *PeerStore) MarkRemoved(session *dal.WriteSession, entry *raftcmdpb.RemovedMemberEntry) error {
	if entry == nil {
		return errors.New("MarkRemoved: nil entry")
	}

	if len(entry.GetInstanceId()) != 16 {
		return fmt.Errorf("MarkRemoved: instance id must be 16 bytes, got %d", len(entry.GetInstanceId()))
	}

	if err := session.SetProto(removedMemberKey(entry.GetNodeId(), entry.GetInstanceId()), entry); err != nil {
		return fmt.Errorf("writing removed-member entry for node %d: %w", entry.GetNodeId(), err)
	}

	return nil
}

// IsRemoved reports whether a tombstone exists for (nodeID, instanceID).
// instanceID must be exactly 16 bytes — every cluster member acquires one
// at first boot via wal.EnsureInstanceID.
func (p *PeerStore) IsRemoved(nodeID uint64, instanceID []byte) (bool, error) {
	if len(instanceID) != 16 {
		return false, fmt.Errorf("PeerStore.IsRemoved: instance id must be 16 bytes, got %d", len(instanceID))
	}

	handle, err := p.store.NewDirectReadHandle()
	if err != nil {
		return false, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	_, closer, err := handle.Get(removedMemberKey(nodeID, instanceID))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}

		return false, fmt.Errorf("get removed-member: %w", err)
	}

	_ = closer.Close()

	return true, nil
}

// LoadAllRemoved iterates every registry entry and returns the decoded
// protos. Used by the admin `ledgerctl cluster list-removed` path and by
// tests.
func (p *PeerStore) LoadAllRemoved() ([]*raftcmdpb.RemovedMemberEntry, error) {
	handle, err := p.store.NewDirectReadHandle()
	if err != nil {
		return nil, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	lower, upper := removedMemberKeyRange()

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return nil, fmt.Errorf("creating removed-member iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var out []*raftcmdpb.RemovedMemberEntry

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != removedMemberKeyLen {
			return nil, fmt.Errorf("invariant: removed-member key has unexpected length %d (want %d): %x",
				len(key), removedMemberKeyLen, key)
		}

		entry := &raftcmdpb.RemovedMemberEntry{}
		if err := entry.UnmarshalVT(iter.Value()); err != nil {
			return nil, fmt.Errorf("invariant: unmarshalling removed-member at key %x: %w", key, err)
		}

		out = append(out, entry)
	}

	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterating removed-members: %w", err)
	}

	return out, nil
}

// AnyRemovedForNodeID reports whether at least one tombstone exists with
// the given nodeID, regardless of instanceID. Fast-path pre-filter for
// admin tooling; the authoritative discrimination is still IsRemoved on
// the exact (nodeID, instanceID) tuple.
func (p *PeerStore) AnyRemovedForNodeID(nodeID uint64) (bool, error) {
	handle, err := p.store.NewDirectReadHandle()
	if err != nil {
		return false, fmt.Errorf("opening read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	lower, upper := removedMemberNodeIDPrefix(nodeID)

	iter, err := handle.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return false, fmt.Errorf("creating removed-member prefix iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	found := iter.First()

	if err := iter.Error(); err != nil {
		return false, fmt.Errorf("iterating removed-members for node %d: %w", nodeID, err)
	}

	return found, nil
}

// DeleteRemovedInSession removes a single (nodeID, instanceID) tombstone
// through the caller's WriteSession. Used by the `ledgerctl cluster
// forget-removed` admin path, which packages the delete as a technical
// FSM proposal so every node applies it deterministically.
func (p *PeerStore) DeleteRemovedInSession(session *dal.WriteSession, nodeID uint64, instanceID []byte) error {
	if len(instanceID) != 16 {
		return fmt.Errorf("DeleteRemovedInSession: instance id must be 16 bytes, got %d", len(instanceID))
	}

	if err := session.DeleteKey(removedMemberKey(nodeID, instanceID)); err != nil {
		return fmt.Errorf("deleting removed-member for node %d: %w", nodeID, err)
	}

	return nil
}
