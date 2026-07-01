package node

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// peerTransport is the slice of *DefaultTransport that Membership must
// keep in lockstep with its peer-address cache so the Raft transport
// dials the right hosts on the next tick.
type peerTransport interface {
	AddPeer(id uint64, addr string)
	RemovePeer(ctx context.Context, id uint64)
}

// peerPool is the slice of *transport.ConnectionPool used to forward
// client RPCs to the appropriate peer. Kept in lockstep with the cache
// the same way as peerTransport.
type peerPool interface {
	AddPeer(id uint64, addr string) error
	RemovePeer(id uint64) error
}

// Membership owns the cluster's peer-address state in two views kept in
// lockstep: the durable Pebble rows under [ZoneGlobal][SubGlobPeers] and
// the in-memory cache the transport consults on every Raft tick. All
// mutators below go through this type — writing the Pebble key directly
// or mutating the cache from outside is a bug.
//
// Three mutator classes:
//
//   - FSM apply (Pebble only, through the FSM's session, atomic with
//     the surrounding business writes): every EntryConfChange* in
//     PrepareEntries fires WriteConfChange. NO cache or transport
//     mutation here — the FSM hot path must stay deterministic and
//     free of network side effects, and Pebble must be the only place
//     that mutates synchronously with the batch.
//
//   - finishReady (cache + transport, post-commit): the Node has just
//     observed the committed ConfChange and must update the cache +
//     wire the transport / service pool before the next Raft tick.
//     Use Set / Remove. Pebble is already up to date thanks to the
//     FSM batch.
//
//   - Lifecycle paths that bypass the FSM (cache + Pebble, own
//     session): bootstrap's initial-peer persistence in
//     PersistInitialPeers, and Node.ForceRemoveNode via Unregister.
type Membership struct {
	store           *PeerStore
	transport       peerTransport
	pool            peerPool
	selfNodeID      uint64
	selfRaftAddr    string
	selfServiceAddr string
	logger          logging.Logger

	mu        sync.RWMutex
	addresses map[uint64]ConfChangeContext
}

// NewMembership loads the in-memory cache from Pebble and wires the
// transport + service pool to match. self is skipped from transport /
// pool registration — a node never dials itself — but its raft +
// service addresses are kept on the Membership so OnSnapshotInstalled
// can re-upsert the local row whenever a leader checkpoint overwrites
// it with a stale value.
//
// Failure to read the peer rows is fatal: without them the node would
// boot with an empty cache while the WAL ConfState still claims the
// cluster has voters.
func NewMembership(store *PeerStore, transport peerTransport, pool peerPool, selfNodeID uint64, selfRaftAddr, selfServiceAddr string, logger logging.Logger) (*Membership, error) {
	addresses, err := store.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("loading peers from pebble: %w", err)
	}

	m := &Membership{
		store:           store,
		transport:       transport,
		pool:            pool,
		selfNodeID:      selfNodeID,
		selfRaftAddr:    selfRaftAddr,
		selfServiceAddr: selfServiceAddr,
		logger:          logger,
		addresses:       addresses,
	}

	for nodeID, addr := range addresses {
		m.wireAdd(nodeID, addr.RaftAddress, addr.ServiceAddress)
	}

	logger.WithFields(map[string]any{
		"peerCount": len(addresses),
	}).Infof("Loaded cluster membership from Pebble")

	return m, nil
}

// wireAdd pushes a peer into the transport + service pool. Self
// is skipped because a node never dials itself.
func (m *Membership) wireAdd(nodeID uint64, raftAddr, serviceAddr string) {
	if nodeID == m.selfNodeID {
		return
	}

	m.transport.AddPeer(nodeID, raftAddr)

	if err := m.pool.AddPeer(nodeID, serviceAddr); err != nil {
		m.logger.WithFields(map[string]any{
			"peer_id": nodeID,
			"error":   err,
		}).Errorf("Failed to add peer to service pool")
	}
}

// wireRemove removes a peer from the transport + service pool.
// Self is skipped (would not be there). context.Background is fine: the
// transport's RemovePeer is internal bookkeeping, not a network call.
func (m *Membership) wireRemove(nodeID uint64) {
	if nodeID == m.selfNodeID {
		return
	}

	m.transport.RemovePeer(context.Background(), nodeID)

	if err := m.pool.RemovePeer(nodeID); err != nil {
		m.logger.WithFields(map[string]any{
			"peer_id": nodeID,
			"error":   err,
		}).Errorf("Failed to remove peer from service pool")
	}
}

// PeerAddresses returns a defensive copy of the current cache.
func (m *Membership) PeerAddresses() map[uint64]ConfChangeContext {
	m.mu.RLock()
	defer m.mu.RUnlock()

	cp := make(map[uint64]ConfChangeContext, len(m.addresses))
	maps.Copy(cp, m.addresses)

	return cp
}

// Set upserts a peer in the cache AND wires it into the transport +
// service pool so the Raft transport / client RPCs can reach it on the
// next tick. Used from finishReady once a ConfChange has been observed
// post-commit; the matching Pebble row was already written by the FSM
// handler (WriteConfChange) in the same batch as the surrounding
// business writes.
//
// Cache mutation and transport wiring both happen inside the lock so
// they stay in lockstep with a concurrent Rehydrate. See Rehydrate's
// locking note.
func (m *Membership) Set(nodeID uint64, raftAddr, serviceAddr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addresses[nodeID] = ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	}

	m.wireAdd(nodeID, raftAddr, serviceAddr)
}

// Remove deletes a peer from the cache AND from the transport +
// service pool. Pebble was already updated by the FSM handler
// (WriteConfChange) in the same batch as the surrounding business
// writes.
//
// Cache mutation and transport wiring both happen inside the lock —
// see Set / Rehydrate.
func (m *Membership) Remove(nodeID uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.addresses, nodeID)

	m.wireRemove(nodeID)
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

// ReconcileAgainstConfState drops every peer (Pebble + cache +
// transport + pool) whose NodeID is not in the supplied ConfState.
// Called by NewNode at boot once the durable ConfState is known, so
// that stale Pebble rows left over by an interrupted ForceRemoveNode
// (or carried in from a restored backup) cannot resurrect into the
// transport and shadow a future re-Add with a different address —
// DefaultTransport.AddPeer is no-op on existing entries.
func (m *Membership) ReconcileAgainstConfState(cs raftpb.ConfState) error {
	m.mu.RLock()
	stale := make([]uint64, 0)

	for nodeID := range m.addresses {
		if confStateContainsNode(cs, nodeID) || nodeID == m.selfNodeID {
			continue
		}

		stale = append(stale, nodeID)
	}
	m.mu.RUnlock()

	for _, nodeID := range stale {
		m.logger.WithFields(map[string]any{
			"peer_id": nodeID,
		}).Infof("Dropping stale peer not present in ConfState")

		if err := m.Unregister(nodeID); err != nil {
			return fmt.Errorf("reconciling stale peer %d: %w", nodeID, err)
		}
	}

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

// Rehydrate re-reads the peer rows from Pebble, computes the diff
// against the in-memory cache, publishes the new cache, and reconciles
// the transport + service pool to match (added peers wired in, removed
// peers wired out, address changes modeled as remove+add). Pebble is
// considered authoritative — this method does NOT touch self; callers
// that need to force the local self row write it through Register or
// the store directly before invoking Rehydrate.
//
// Two call sites:
//
//   - NewNode, after Applier.RecoverAndReplay: WAL replay applied
//     ConfChange entries to Pebble through WriteConfChange (FSM hot
//     path), but the FSM intentionally does not touch the in-memory
//     cache — that side effect lives in finishReady, which does not
//     run during replay. Without this catch-up the recovered node
//     would dial the pre-crash peer set until the next snapshot
//     install or restart.
//
//   - OnSnapshotInstalled: a leader checkpoint restore has just
//     overwritten Pebble; reload to match.
//
// Locking: cache mutation AND transport/pool wiring both happen inside
// the write lock. Releasing the lock between the two would let a
// concurrent finishReady Set/Remove slip in and observe an inconsistent
// snapshot — e.g. Rehydrate publishes a new cache without peer X,
// unlocks, then a fresh Set(X) fires (cache re-adds X, transport wired
// in). Rehydrate's deferred wireRemove(X) would then unwire the
// transport while the cache still holds X, and the node would be
// unable to dial X until the next rehydrate. Holding the lock through
// wire calls is safe: transport.AddPeer / RemovePeer and pool.AddPeer /
// RemovePeer are internal bookkeeping (no network round trip on the
// hot path), and Rehydrate only fires from lifecycle hooks — not the
// per-tick path.
func (m *Membership) Rehydrate() error {
	fresh, err := m.store.LoadAll()
	if err != nil {
		return fmt.Errorf("loading peers from pebble: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	old := m.addresses
	m.addresses = fresh

	// AddPeer / pool.AddPeer are no-ops on existing entries, so an
	// address change is modelled as RemovePeer + AddPeer.
	for nodeID, addr := range fresh {
		oldAddr, existed := old[nodeID]
		if existed && oldAddr == addr {
			continue
		}

		if existed {
			m.wireRemove(nodeID)
		}

		m.wireAdd(nodeID, addr.RaftAddress, addr.ServiceAddress)
	}

	for nodeID := range old {
		if _, kept := fresh[nodeID]; !kept {
			m.wireRemove(nodeID)
		}
	}

	m.logger.WithFields(map[string]any{
		"peerCount": len(fresh),
	}).Infof("Rehydrated cluster membership from Pebble")

	return nil
}

// OnSnapshotInstalled refreshes the cache from Pebble after a leader
// checkpoint restore overwrites it AND reconciles the transport +
// service pool to match. The leader's checkpoint may carry a stale
// self row (e.g. our previous AdvertiseAddr before a pod restart with
// a new endpoint), so we first overwrite the self row in Pebble with
// the locally-known truth and only then reload — that way the upcoming
// LoadAll already returns the correct self and the next checkpoint we
// serve carries the fresh address. Wired on Applier; runs synchronously
// inside the maintenance task so the next Raft tick already sees the
// up-to-date cache + transport.
//
// Failures are logged but not fatal: state stays at its pre-restore
// values and the next leadership/restart resyncs. Crashing the apply
// loop on a transient read failure is worse than a slightly stale view.
func (m *Membership) OnSnapshotInstalled() {
	// Force-write the locally-authoritative self row to Pebble BEFORE
	// the reload, so LoadAll pulls our address rather than the leader's
	// potentially-stale view and the next checkpoint we serve is
	// already correct. The empty-string check filters out the test
	// path that constructs a Membership without a real self identity.
	if m.selfRaftAddr != "" {
		if err := m.store.Put(m.selfNodeID, m.selfRaftAddr, m.selfServiceAddr); err != nil {
			m.logger.WithFields(map[string]any{
				"error": err,
			}).Errorf("Refreshing self in Pebble before snapshot reload failed; cache left stale")

			return
		}
	}

	if err := m.Rehydrate(); err != nil {
		m.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Reloading peers from Pebble after snapshot install failed; cache left stale")
	}
}

// WriteConfChange is the FSM ConfChange handler: invoked from
// PrepareEntries for every EntryConfChange* with the in-flight
// WriteSession. It writes ONLY to the supplied Pebble batch — no cache
// mutation, no transport/pool wiring — so the FSM hot path stays
// deterministic and free of network side effects, and the in-memory
// state cannot diverge from Pebble if the surrounding batch later
// fails to commit. Cache + transport wiring happens in
// Node.finishReady once the ConfChange is observed post-commit, via
// Membership.Set / Membership.Remove.
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

	return walkConfChangeContexts(cc, func(t raftpb.ConfChangeType, nodeID uint64, ctx *ConfChangeContext) error {
		switch t {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
			if ctx == nil {
				return nil
			}

			if err := session.SetProto(peerKey(nodeID), &raftcmdpb.PeerAddress{
				NodeId:         nodeID,
				RaftAddress:    ctx.RaftAddress,
				ServiceAddress: ctx.ServiceAddress,
			}); err != nil {
				return fmt.Errorf("session write peer %d: %w", nodeID, err)
			}
		case raftpb.ConfChangeRemoveNode:
			if err := session.DeleteKey(peerKey(nodeID)); err != nil {
				return fmt.Errorf("session delete peer %d: %w", nodeID, err)
			}
		}

		return nil
	})
}
