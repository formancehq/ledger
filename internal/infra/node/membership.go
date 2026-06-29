package node

import (
	"fmt"
	"maps"
	"sync"

	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Membership owns the cluster's peer-address state in two views kept in
// lockstep: the durable Pebble rows under [ZoneGlobal][SubGlobPeers] and
// the in-memory cache the transport consults on every Raft tick. All
// mutators below go through this type — writing the Pebble key directly
// or mutating the cache from outside is a bug.
//
// Three mutator classes:
//
//   - finishReady (cache-only): the Node has just observed a committed
//     ConfChange and must update the cache before the next Raft tick;
//     the matching Pebble write lands later via WriteConfChange when
//     the FSM batch commits. Use Set / Remove.
//
//   - FSM apply (cache + Pebble through the FSM's session, atomic with
//     the surrounding business writes): every EntryConfChange* in
//     PrepareEntries fires WriteConfChange.
//
//   - Lifecycle paths that bypass the FSM (cache + Pebble, own
//     session): bootstrap's initial-peer persistence in
//     PersistInitialPeers, and Node.ForceRemoveNode via Unregister.
type Membership struct {
	store  *PeerStore
	logger logging.Logger

	mu        sync.RWMutex
	addresses map[uint64]ConfChangeContext
}

// NewMembership loads the in-memory cache from Pebble. Failure to read
// the peer rows is fatal: without them the node would boot with an empty
// cache while the WAL ConfState still claims the cluster has voters.
func NewMembership(store *PeerStore, logger logging.Logger) (*Membership, error) {
	addresses, err := store.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("loading peers from pebble: %w", err)
	}

	logger.WithFields(map[string]any{
		"peerCount": len(addresses),
	}).Infof("Loaded cluster membership from Pebble")

	return &Membership{
		store:     store,
		logger:    logger,
		addresses: addresses,
	}, nil
}

// PeerAddresses returns a defensive copy of the current cache.
func (m *Membership) PeerAddresses() map[uint64]ConfChangeContext {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cp := make(map[uint64]ConfChangeContext, len(m.addresses))
	maps.Copy(cp, m.addresses)

	return cp
}

// Set upserts a peer in the cache. The matching Pebble write happens
// later via WriteConfChange when the FSM batch commits. Used from
// finishReady so the transport sees the new membership on the next tick
// without waiting for the applier.
func (m *Membership) Set(nodeID uint64, raftAddr, serviceAddr string) {
	m.mu.Lock()
	m.addresses[nodeID] = ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	}
	m.mu.Unlock()
}

// Remove deletes a peer from the cache. Pebble removal happens later
// via WriteConfChange.
func (m *Membership) Remove(nodeID uint64) {
	m.mu.Lock()
	delete(m.addresses, nodeID)
	m.mu.Unlock()
}

// Register writes a peer through Pebble (own session) AND the cache, in
// lockstep. Used by lifecycle paths that bypass the FSM: bootstrap
// initial-peer persistence and ForceRemoveNode (dual: Unregister).
func (m *Membership) Register(nodeID uint64, raftAddr, serviceAddr string) error {
	if err := m.store.Put(nodeID, raftAddr, serviceAddr); err != nil {
		return fmt.Errorf("persisting peer %d: %w", nodeID, err)
	}

	m.Set(nodeID, raftAddr, serviceAddr)

	return nil
}

// Unregister is the dual of Register: Pebble delete + cache delete.
// Pebble first so a crash between the two leaves a state the next boot
// can heal from (LoadAll won't see the deleted peer).
func (m *Membership) Unregister(nodeID uint64) error {
	if err := m.store.Delete(nodeID); err != nil {
		return fmt.Errorf("removing peer %d from pebble: %w", nodeID, err)
	}

	m.Remove(nodeID)

	return nil
}

// PersistInitialPeers writes cfg.Peers (and self when includeSelf is
// true) before the WAL snapshot / CLUSTER_JOINED marker is written, so
// a crash cannot leave a durable ConfState without the matching peer
// addresses in Pebble (EN-1413).
//
// includeSelf is true for Bootstrap and Restore; false for Join (self's
// address lands later via the AddLearner ConfChange applied through
// WriteConfChange).
func (m *Membership) PersistInitialPeers(cfg NodeConfig, includeSelf bool) error {
	if includeSelf {
		if err := m.Register(cfg.NodeID, cfg.AdvertiseAddr, cfg.ServiceAdvertiseAddr); err != nil {
			return fmt.Errorf("persisting self in peer store: %w", err)
		}
	}

	for _, p := range cfg.Peers {
		if err := m.Register(p.ID, p.Address, p.ServiceAddress); err != nil {
			return fmt.Errorf("persisting initial peer %d: %w", p.ID, err)
		}
	}

	return nil
}

// OnSnapshotInstalled refreshes the cache from Pebble after a leader
// checkpoint restore overwrites it. Wired on Applier; runs synchronously
// inside the maintenance task so the next Raft tick already sees the
// up-to-date cache.
//
// LoadAll failure is logged but not fatal: the cache stays at its
// pre-restore state and the next leadership/restart resyncs. Crashing
// the apply loop on a transient read failure is worse than a slightly
// stale cache.
func (m *Membership) OnSnapshotInstalled() {
	addresses, err := m.store.LoadAll()
	if err != nil {
		m.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Reloading peers from Pebble after snapshot install failed; cache left stale")

		return
	}

	m.mu.Lock()
	m.addresses = addresses
	m.mu.Unlock()

	m.logger.WithFields(map[string]any{
		"peerCount": len(addresses),
	}).Infof("Reloaded cluster membership from Pebble post-snapshot")
}

// WriteConfChange is the FSM ConfChange handler: invoked from
// PrepareEntries for every EntryConfChange* with the in-flight
// WriteSession. The peer mutation lands in the same Pebble commit as
// the surrounding business writes; the cache is updated in-line so it
// stays consistent without waiting for the next applier tick.
//
// PromoteLearner (ConfChangeAddNode with empty context) carries no
// address payload — it's a role change — so we skip it.
func (m *Membership) WriteConfChange(entry raftpb.Entry, session *dal.WriteSession) error {
	cc, ok, err := unmarshalConfChangeV2(entry)
	if err != nil {
		return fmt.Errorf("decoding ConfChange entry: %w", err)
	}

	if !ok {
		return nil
	}

	for _, change := range cc.Changes {
		switch change.Type {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if len(cc.Context) == 0 {
				continue
			}

			ccCtx, err := UnmarshalConfChangeContext(cc.Context)
			if err != nil {
				return fmt.Errorf("invariant: unmarshal ConfChange context for node %d: %w", change.NodeID, err)
			}

			if err := session.SetProto(peerKey(change.NodeID), &raftcmdpb.PeerAddress{
				NodeId:         change.NodeID,
				RaftAddress:    ccCtx.RaftAddress,
				ServiceAddress: ccCtx.ServiceAddress,
			}); err != nil {
				return fmt.Errorf("session write peer %d: %w", change.NodeID, err)
			}

			m.Set(change.NodeID, ccCtx.RaftAddress, ccCtx.ServiceAddress)
		case raftpb.ConfChangeRemoveNode:
			if err := session.DeleteKey(peerKey(change.NodeID)); err != nil {
				return fmt.Errorf("session delete peer %d: %w", change.NodeID, err)
			}

			m.Remove(change.NodeID)
		}
	}

	return nil
}
