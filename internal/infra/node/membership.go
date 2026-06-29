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
	store      *PeerStore
	transport  peerTransport
	pool       peerPool
	selfNodeID uint64
	logger     logging.Logger

	mu        sync.RWMutex
	addresses map[uint64]ConfChangeContext
}

// NewMembership loads the in-memory cache from Pebble and wires the
// transport + service pool to match. transport and pool may be nil for
// tests that don't exercise the wiring. selfNodeID is skipped from
// transport/pool registration — a node never dials itself.
//
// Failure to read the peer rows is fatal: without them the node would
// boot with an empty cache while the WAL ConfState still claims the
// cluster has voters.
func NewMembership(store *PeerStore, transport peerTransport, pool peerPool, selfNodeID uint64, logger logging.Logger) (*Membership, error) {
	addresses, err := store.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("loading peers from pebble: %w", err)
	}

	m := &Membership{
		store:      store,
		transport:  transport,
		pool:       pool,
		selfNodeID: selfNodeID,
		logger:     logger,
		addresses:  addresses,
	}

	for nodeID, addr := range addresses {
		m.wireAddLocked(nodeID, addr.RaftAddress, addr.ServiceAddress)
	}

	logger.WithFields(map[string]any{
		"peerCount": len(addresses),
	}).Infof("Loaded cluster membership from Pebble")

	return m, nil
}

// wireAddLocked pushes a peer into the transport + service pool. Caller
// must NOT hold m.mu — this method makes no map access. Self is skipped
// because a node never dials itself.
func (m *Membership) wireAddLocked(nodeID uint64, raftAddr, serviceAddr string) {
	if nodeID == m.selfNodeID {
		return
	}

	if m.transport != nil {
		m.transport.AddPeer(nodeID, raftAddr)
	}

	if m.pool != nil {
		if err := m.pool.AddPeer(nodeID, serviceAddr); err != nil {
			m.logger.WithFields(map[string]any{
				"peer_id": nodeID,
				"error":   err,
			}).Errorf("Failed to add peer to service pool")
		}
	}
}

// wireRemoveLocked removes a peer from the transport + service pool.
// Self is skipped (would not be there). context.Background is fine: the
// transport's RemovePeer is internal bookkeeping, not a network call.
func (m *Membership) wireRemoveLocked(nodeID uint64) {
	if nodeID == m.selfNodeID {
		return
	}

	if m.transport != nil {
		m.transport.RemovePeer(context.Background(), nodeID)
	}

	if m.pool != nil {
		if err := m.pool.RemovePeer(nodeID); err != nil {
			m.logger.WithFields(map[string]any{
				"peer_id": nodeID,
				"error":   err,
			}).Errorf("Failed to remove peer from service pool")
		}
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
// next tick. The matching Pebble write lands later via WriteConfChange
// when the FSM batch commits. Used from finishReady (cache-side mirror
// of the FSM-applied ConfChange).
func (m *Membership) Set(nodeID uint64, raftAddr, serviceAddr string) {
	m.mu.Lock()
	m.addresses[nodeID] = ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
	}
	m.mu.Unlock()

	m.wireAddLocked(nodeID, raftAddr, serviceAddr)
}

// Remove deletes a peer from the cache AND from the transport +
// service pool. Pebble removal happens later via WriteConfChange.
func (m *Membership) Remove(nodeID uint64) {
	m.mu.Lock()
	delete(m.addresses, nodeID)
	m.mu.Unlock()

	m.wireRemoveLocked(nodeID)
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

// OnSnapshotInstalled refreshes the cache from Pebble after a leader
// checkpoint restore overwrites it AND reconciles the transport +
// service pool to match (added peers wired in, removed peers wired out).
// Wired on Applier; runs synchronously inside the maintenance task so
// the next Raft tick already sees the up-to-date cache + transport.
//
// LoadAll failure is logged but not fatal: state stays at its pre-
// restore values and the next leadership/restart resyncs. Crashing the
// apply loop on a transient read failure is worse than a slightly stale
// view.
func (m *Membership) OnSnapshotInstalled() {
	fresh, err := m.store.LoadAll()
	if err != nil {
		m.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Reloading peers from Pebble after snapshot install failed; cache left stale")

		return
	}

	m.mu.Lock()
	old := m.addresses
	m.addresses = fresh
	m.mu.Unlock()

	// Wire in anything present in the new view that wasn't (or was
	// different) before; wire out anything that disappeared.
	//
	// AddPeer / pool.AddPeer are documented as no-op when an entry
	// already exists, so an address CHANGE has to be modelled as
	// RemovePeer + AddPeer — otherwise the transport keeps dialling
	// the pre-restore address and the new one is silently dropped.
	// Same NodeID with the same addresses is a no-op short-circuit
	// (skip both removeAdd and the underlying calls).
	for nodeID, addr := range fresh {
		oldAddr, existed := old[nodeID]
		if existed && oldAddr == addr {
			continue
		}

		if existed {
			m.wireRemoveLocked(nodeID)
		}

		m.wireAddLocked(nodeID, addr.RaftAddress, addr.ServiceAddress)
	}

	for nodeID := range old {
		if _, kept := fresh[nodeID]; !kept {
			m.wireRemoveLocked(nodeID)
		}
	}

	m.logger.WithFields(map[string]any{
		"peerCount": len(fresh),
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
