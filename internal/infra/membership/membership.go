package membership

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"

	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Transport is the slice of *DefaultTransport that Membership must
// keep in lockstep with its peer-address cache so the Raft transport
// dials the right hosts on the next tick.
type Transport interface {
	AddPeer(id uint64, addr string)
	RemovePeer(ctx context.Context, id uint64)
}

// Pool is the slice of *transport.ConnectionPool used to forward
// client RPCs to the appropriate peer. Kept in lockstep with the cache
// the same way as Transport.
type Pool interface {
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
	transport       Transport
	pool            Pool
	selfNodeID      uint64
	selfRaftAddr    string
	selfServiceAddr string
	selfInstanceID  []byte
	logger          logging.Logger

	mu        sync.RWMutex
	addresses map[uint64]ConfChangeContext
	// started gates transport / service-pool wiring. Until Start fires
	// (OnStart hook, after the local Raft gRPC server is listening),
	// Set / Remove / Register / Rehydrate mutate only the cache — the
	// transport side effect is deferred. Wiring at construction time
	// would race with the remote pods not yet listening: in
	// --tls-mode=optional, pool.AddPeer probes the peer's TLS handshake
	// and returns an error if the peer isn't up, and my earlier logs-
	// and-move-on handling made those failures permanent. Deferring
	// the initial wire to Start solves this: Start fires only after
	// the local Raft server is up, by which point the peer pods have
	// had a chance to listen. Runtime Set / Remove (from finishReady,
	// long after Start) wire inline as before.
	started bool
}

// NewMembership loads the in-memory cache from Pebble. The transport +
// service pool are NOT wired here — see the started field's comment.
// self is skipped from transport / pool wiring — a node never dials
// itself — but its raft + service addresses are kept on the Membership
// so OnSnapshotInstalled can re-upsert the local row whenever a leader
// checkpoint overwrites it with a stale value.
//
// Failure to read the peer rows is fatal: without them the node would
// boot with an empty cache while the WAL ConfState still claims the
// cluster has voters.
func NewMembership(store *PeerStore, transport Transport, pool Pool, selfNodeID uint64, selfRaftAddr, selfServiceAddr string, selfInstanceID []byte, logger logging.Logger) (*Membership, error) {
	addresses, err := store.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("loading peers from pebble: %w", err)
	}

	logger.WithFields(map[string]any{
		"peerCount": len(addresses),
	}).Infof("Loaded cluster membership from Pebble")

	return &Membership{
		store:           store,
		transport:       transport,
		pool:            pool,
		selfNodeID:      selfNodeID,
		selfRaftAddr:    selfRaftAddr,
		selfServiceAddr: selfServiceAddr,
		selfInstanceID:  selfInstanceID,
		logger:          logger,
		addresses:       addresses,
	}, nil
}

// Start wires every peer currently in the cache into the transport +
// service pool. Called from an fx OnStart hook that fires AFTER the
// local Raft gRPC server is listening, so remote pods have had a chance
// to listen too (avoiding a --tls-mode=optional probe failure that
// would silently drop the peer from the pool).
//
// After Start returns, subsequent Set / Remove / Register / Rehydrate
// calls wire the transport inline. Start is idempotent — a second call
// is a plain re-wire of the current cache (all AddPeer calls are no-op
// when the peer is already registered with the same address).
func (m *Membership) Start() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.started = true

	for nodeID, addr := range m.addresses {
		m.wireAdd(nodeID, addr.RaftAddress, addr.ServiceAddress)
	}
}

// wireAdd pushes a peer into the transport + service pool. Self is
// skipped because a node never dials itself. Deferred to Start when
// invoked before the local Raft gRPC server is listening (see the
// started field's comment).
//
// Caller must hold m.mu — wire mutations must stay in lockstep with
// the cache to avoid drift under concurrent Rehydrate / finishReady.
func (m *Membership) wireAdd(nodeID uint64, raftAddr, serviceAddr string) {
	if nodeID == m.selfNodeID || !m.started {
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

// wireRemove removes a peer from the transport + service pool. Self is
// skipped (would not be there). context.Background is fine: the
// transport's RemovePeer is internal bookkeeping, not a network call.
// Deferred to Start when invoked before the local Raft gRPC server is
// listening (though a remove before Start is a corner case — the peer
// wasn't wired in the first place, so this becomes a no-op either way).
//
// Caller must hold m.mu.
func (m *Membership) wireRemove(nodeID uint64) {
	if nodeID == m.selfNodeID || !m.started {
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

// GetInstanceID returns the peer's 16-byte identity UUID from the in-memory
// cache, and ok=true when the peer is currently known. Returns (nil, false)
// when the peer is not in the cache — the caller decides whether that's an
// error (RemoveNode) or a skip (checkAndPromoteLearners of a not-yet-added
// learner).
//
// Used by Node.RemoveNode to pack the target's identity into the
// ConfChange context before proposing, so every node's FSM apply lands the
// same RemovedMemberEntry atomically with the peer row delete.
func (m *Membership) GetInstanceID(nodeID uint64) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	addr, ok := m.addresses[nodeID]
	if !ok {
		return nil, false
	}

	return addr.InstanceID, true
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
func (m *Membership) Set(nodeID uint64, raftAddr, serviceAddr string, instanceID []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addresses[nodeID] = ConfChangeContext{
		RaftAddress:    raftAddr,
		ServiceAddress: serviceAddr,
		InstanceID:     instanceID,
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
// instanceID is 16 bytes (EN-1045) for peers whose identity is known
// (self at boot, JoinAsLearner outcome); may be empty for bootstrap
// initial-peer entries whose identity is not known at cluster-formation
// time (the row is refreshed by WriteConfChange when the peer later
// goes through the ConfChange apply path).
func (m *Membership) Register(nodeID uint64, raftAddr, serviceAddr string, instanceID []byte) error {
	if err := m.store.Put(nodeID, raftAddr, serviceAddr, instanceID); err != nil {
		return fmt.Errorf("persisting peer %d: %w", nodeID, err)
	}

	m.Set(nodeID, raftAddr, serviceAddr, instanceID)

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

// UnregisterAndBlacklist removes a peer AND persists a RemovedMemberEntry
// for (nodeID, instanceID) in the same Pebble transaction. Used by
// ForceRemoveNode so a still-alive removed pod cannot silently rejoin and
// be auto-promoted (EN-1045).
//
// The two writes commit atomically inside a single dal.WriteSession — the
// closest we can get given WAL and Pebble are independent stores (see
// docs/technical/architecture/subsystems/consensus/removed-member-registry.md).
// Followers converge via the next snapshot they receive.
//
// removedAt is stamped by the leader with wall-clock time; this path runs
// outside the FSM apply hot path and is therefore free to use time (unlike
// the consensus path which is deterministic-only).
func (m *Membership) UnregisterAndBlacklist(nodeID uint64, instanceID []byte, removedAt uint64) error {
	if len(instanceID) != 16 {
		return fmt.Errorf("UnregisterAndBlacklist: instance id must be 16 bytes, got %d", len(instanceID))
	}

	session := m.store.OpenWriteSession()

	if err := m.store.DeleteInSession(session, nodeID); err != nil {
		_ = session.Cancel()

		return fmt.Errorf("deleting peer %d in force-remove batch: %w", nodeID, err)
	}

	if err := m.store.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
		NodeId:     nodeID,
		InstanceId: instanceID,
		RemovedAt:  removedAt,
		Reason:     removedReasonForce,
	}); err != nil {
		_ = session.Cancel()

		return fmt.Errorf("writing blacklist entry in force-remove batch: %w", err)
	}

	if err := session.Commit(); err != nil {
		return fmt.Errorf("commit force-remove batch: %w", err)
	}

	m.Remove(nodeID)

	return nil
}

// IsRemoved reports whether a tombstone exists for (nodeID, instanceID).
// Thin passthrough to PeerStore.IsRemoved kept on Membership so callers
// depend on a single membership surface.
func (m *Membership) IsRemoved(nodeID uint64, instanceID []byte) (bool, error) {
	return m.store.IsRemoved(nodeID, instanceID)
}

// PeerStore exposes the underlying store — admin RPCs use it for the
// list-removed / forget-removed paths. Prefer IsRemoved /
// UnregisterAndBlacklist on Membership for the common paths.
func (m *Membership) PeerStore() *PeerStore {
	return m.store
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
		if confStateContains(cs, nodeID) || nodeID == m.selfNodeID {
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

// confStateContains reports whether nodeID appears in the ConfState's
// Voters or Learners list.
func confStateContains(cs raftpb.ConfState, nodeID uint64) bool {
	return slices.Contains(cs.Voters, nodeID) || slices.Contains(cs.Learners, nodeID)
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
		if existed && oldAddr.Equal(addr) {
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
	// already correct.
	//
	// A self-Put failure is logged but does NOT short-circuit the
	// Rehydrate below: the checkpoint restore has already swapped
	// Pebble to the leader's peer set, so skipping Rehydrate would
	// leave the in-memory cache holding pre-restore peers while Pebble
	// holds the post-restore set — a durable cache-vs-Pebble
	// divergence that persists until the next leadership change.
	// Better to fall through and let Rehydrate reload from Pebble
	// (with the leader's possibly-stale self); the next OnSnapshot
	// or restart will re-attempt the self-Put.
	if err := m.store.Put(m.selfNodeID, m.selfRaftAddr, m.selfServiceAddr, m.selfInstanceID); err != nil {
		m.logger.WithFields(map[string]any{
			"error": err,
		}).Errorf("Refreshing self in Pebble before snapshot reload failed; next checkpoint we serve may carry a stale self address")
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
	cc, ok, err := UnmarshalConfChangeV2(entry)
	if err != nil {
		return fmt.Errorf("decoding ConfChange entry: %w", err)
	}

	if !ok {
		return nil
	}

	return WalkConfChangeContexts(cc, func(t raftpb.ConfChangeType, nodeID uint64, ctx *ConfChangeContext) error {
		switch t {
		case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode, raftpb.ConfChangeUpdateNode:
			if ctx == nil {
				return nil
			}

			// UpdateNode uses the same peer-row write path as Add/AddLearner:
			// it refreshes an existing row with a fresh (addr, instance_id).
			// Used by the admin cluster.AddLearner + boot flow (EN-1045) so
			// a row initially written with nil instance_id gets updated when
			// the pod actually calls JoinAsLearner.
			if err := session.SetProto(peerKey(nodeID), &raftcmdpb.PeerAddress{
				NodeId:         nodeID,
				RaftAddress:    ctx.RaftAddress,
				ServiceAddress: ctx.ServiceAddress,
				InstanceId:     ctx.InstanceID,
			}); err != nil {
				return fmt.Errorf("session write peer %d: %w", nodeID, err)
			}
		case raftpb.ConfChangeRemoveNode:
			if err := session.DeleteKey(peerKey(nodeID)); err != nil {
				return fmt.Errorf("session delete peer %d: %w", nodeID, err)
			}

			// EN-1045: when the proposer packed the removed peer's
			// instance_id into the context, land the RemovedMemberEntry
			// atomically with the peer row delete. Missing context is
			// legal for peers whose row was created via the admin
			// cluster.AddLearner path without the target ever booting
			// (phantom learner) — their identity is unknown and there
			// is nothing to blacklist. In that case only the peer row
			// is deleted.
			if ctx == nil || len(ctx.InstanceID) != 16 {
				return nil
			}

			if err := m.store.MarkRemoved(session, &raftcmdpb.RemovedMemberEntry{
				NodeId:     nodeID,
				InstanceId: ctx.InstanceID,
				Reason:     removedReasonConsensus,
				// RemovedAt is intentionally left 0 in the consensus
				// path: FSM apply must be deterministic (invariant #2),
				// wall-clock time is off limits, and the raft entry
				// index/term are not audit-friendly timestamps. The
				// force path (leader-local, not FSM apply) stamps a
				// wall-clock microsecond timestamp.
			}); err != nil {
				return err
			}
		}

		return nil
	})
}
