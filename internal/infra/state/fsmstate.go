package state

import (
	"fmt"
	"math"

	"github.com/formancehq/ledger/v3/internal/domain"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// FSMState groups the mutable FSM-level state fields that the apply path
// writes and Recovery reads back from Pebble at boot or post-sync. It is
// held as Machine.State and is the explicit contract between the two:
// Recovery mutates r.apply.State.X; the hot path reads fsm.State.X.
//
// Fields that are conceptually "state of the FSM" live here. Capability
// objects (queryCheckpoints, sentinel, cacheSnapshotter), in-memory plumbing
// (channels, mutexes, metrics), and shared sub-trackers (Registry,
// KeyStore, SharedState, BloomFilters) remain on Machine — they are not
// recovered from Pebble at boot and have their own lifecycles.
type FSMState struct {
	// Raft / apply progress.
	LastAppliedIndex     uint64
	LastAppliedTimestamp uint64
	SnapshotIndex        uint64

	// Sequence counters bumped on every apply.
	NextSequenceID        uint64
	NextAuditSequenceID   uint64
	NextLedgerID          uint32
	NextQueryCheckpointID uint64

	// Audit chain: rolling hash of the last persisted audit entry.
	LastAuditHash []byte

	// Pending work derived from durable state.
	QueryCheckpointSchedule string

	// LiveQueryCheckpointIDs is the set of query-checkpoint IDs currently live,
	// recovered by scanning the SubGlobQueryCheckpoint rows at boot and updated
	// by the create/delete handlers. The FSM reads it to enforce the policy cap
	// and reject deletes of non-live IDs deterministically, without a Pebble read
	// on the apply path.
	LiveQueryCheckpointIDs map[uint64]struct{}

	// Last cluster config applied + derived hash generator. Persisted under
	// ZoneGlobal so it survives restarts.
	LastClusterConfig *commonpb.ClusterConfig
	HashGenerator     processing.HashGenerator

	// CacheEpoch is the persisted cache epoch read alongside LastClusterConfig.
	// Carried in FSMState so that LoadFSMStateFromStore performs exactly one
	// cluster-state read per recovery; consumers (Registry.Cache.SetEpoch) read
	// the value from the swapped state instead of issuing a second Pebble Get.
	CacheEpoch uint64

	// ClusterPolicy is the applied Raft-replicated cluster policy. Persisted
	// under ZoneGlobal so it survives restarts and rides inside snapshots and
	// backups. Never nil: NewFSMState seeds the revision-0 default so the apply
	// path and the readiness gate can read it before any policy is committed.
	ClusterPolicy *commonpb.ClusterPolicy

	// ClusterID is the immutable identifier injected at boot. Kept here so
	// rebuilds of HashGenerator (after a ClusterConfig change) have it at
	// hand without having to plumb it from elsewhere.
	ClusterID string
}

// NewFSMState builds a fresh FSMState. Counters start at their canonical
// initial values; the map is allocated empty.
func NewFSMState(clusterID string) *FSMState {
	return &FSMState{
		NextSequenceID:         1,
		NextAuditSequenceID:    1,
		NextLedgerID:           1,
		HashGenerator:          processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID),
		ClusterID:              clusterID,
		ClusterPolicy:          &commonpb.ClusterPolicy{},
		LiveQueryCheckpointIDs: map[uint64]struct{}{},
	}
}

// UpdateClusterPolicy installs policy as the applied cluster policy. Revision
// monotonicity is enforced by the caller (the FSM apply path) before this runs.
func (s *FSMState) UpdateClusterPolicy(policy *commonpb.ClusterPolicy) {
	s.ClusterPolicy = policy
}

// AdvanceHLC enforces Hybrid Logical Clock monotonicity over LastAppliedTimestamp.
// If proposalDate is strictly ahead of the current clock, it is adopted as-is;
// otherwise the clock is incremented by one tick so the returned value is
// strictly greater than the previous one. Returns the effective timestamp.
func (s *FSMState) AdvanceHLC(proposalDate uint64) uint64 {
	if proposalDate > s.LastAppliedTimestamp {
		s.LastAppliedTimestamp = proposalDate
	} else {
		s.LastAppliedTimestamp++
	}

	return s.LastAppliedTimestamp
}

// UpdateClusterConfig installs cfg as the current cluster config and, if the
// hash algorithm changed, rebuilds HashGenerator so future audit-chain hashes
// use the new algorithm. Centralising the rule here guarantees that no call
// site can swap LastClusterConfig without re-deriving HashGenerator.
func (s *FSMState) UpdateClusterConfig(cfg *commonpb.ClusterConfig) {
	if cfg.GetHashAlgorithm() != s.HashGenerator.Algorithm() {
		s.HashGenerator = processing.NewHashGenerator(cfg.GetHashAlgorithm(), s.ClusterID)
	}

	s.LastClusterConfig = cfg
}

// AppendAuditEntry commits a new audit-chain entry: the caller-computed hash
// becomes LastAuditHash and the sequence counter advances by one. Returns the
// sequence number the entry should carry (the value before the bump). Tying
// the hash and the sequence to a single method prevents call sites from
// advancing one without the other.
// AppendAuditEntry allocates the next audit sequence and advances the chain
// head. Fails with domain.ErrSequenceSpaceExhausted at math.MaxUint64 rather
// than wrapping to 0, which would restart the chain over its own beginning.
// Same reasoning as WriteSet.IncrementNextSequenceID.
func (s *FSMState) AppendAuditEntry(hash []byte) (uint64, error) {
	if s.NextAuditSequenceID == math.MaxUint64 {
		return 0, fmt.Errorf("allocating audit sequence: %w", domain.ErrSequenceSpaceExhausted)
	}

	sequence := s.NextAuditSequenceID
	s.LastAuditHash = hash
	s.NextAuditSequenceID++

	return sequence, nil
}

// LoadFSMStateFromStore reads every FSM-level field that lives in FSMState
// from the main Pebble store, returning a fully-hydrated value ready to be
// swapped into a Machine via Machine.RestoreState. Sub-trackers (
// Registry.Reversions, KeyStore, SharedState, Registry.Cache settings,
// Registry.Idempotency) are out of scope — they have their own lifecycles
// and are reset separately by the caller.
//
// SnapshotIndex is in-memory only and NOT loaded here: the caller (Recovery
// at boot, Synchronizer at install-snapshot) carries the right value across
// the swap. ClusterID is immutable and carried as a parameter.
//
// Used by Recovery.RecoverState (boot and post-follower-sync) so the load
// is atomic: any error returns before the Machine is touched, leaving the
// current state intact instead of half-written.
func LoadFSMStateFromStore(reader dal.RecoveryReader, handle *dal.ReadHandle, clusterID string) (*FSMState, error) {
	s := NewFSMState(clusterID)

	lastAppliedIndex, err := query.ReadLastAppliedIndex(reader)
	if err != nil {
		return nil, fmt.Errorf("reading last applied index: %w", err)
	}

	s.LastAppliedIndex = lastAppliedIndex

	lastSeq, err := query.ReadLastSequence(handle)
	if err != nil {
		return nil, fmt.Errorf("reading last sequence: %w", err)
	}

	// A head at MaxUint64 cannot be advanced past: lastSeq+1 wraps to 0, and
	// the FSM would start allocating at a sequence the checker reports as
	// impossible, on top of whatever row is already there. The FSM cannot reach
	// that head on its own — NextSequenceID is seeded at 1 and only ever
	// incremented — so a store holding one was corrupted or restored from a
	// tampered stream. Refuse to boot rather than wrap (invariant #7).
	if lastSeq == math.MaxUint64 {
		return nil, fmt.Errorf("stored log head is %d, the maximum uint64: no further log sequence can be "+
			"allocated, so this store is corrupt or was restored from a tampered export", lastSeq)
	}

	if lastSeq > 0 {
		s.NextSequenceID = lastSeq + 1
	}

	lastAuditEntry, err := query.ReadLastAuditEntry(handle)
	if err != nil {
		return nil, fmt.Errorf("reading last audit entry: %w", err)
	}

	if lastAuditEntry != nil {
		// Same wrap, same refusal: an audit head at MaxUint64 would restart the
		// audit sequence at 0 and rewrite the chain from the beginning.
		if lastAuditEntry.GetSequence() == math.MaxUint64 {
			return nil, fmt.Errorf("stored audit head is %d, the maximum uint64: no further audit sequence "+
				"can be allocated, so this store is corrupt or was restored from a tampered export",
				lastAuditEntry.GetSequence())
		}

		s.LastAuditHash = lastAuditEntry.GetHash()
		s.NextAuditSequenceID = lastAuditEntry.GetSequence() + 1
	}

	nextQCPID, err := query.ReadNextQueryCheckpointID(reader)
	if err != nil {
		return nil, fmt.Errorf("reading next query checkpoint ID: %w", err)
	}

	s.NextQueryCheckpointID = nextQCPID

	liveCheckpoints, err := query.ReadLiveQueryCheckpointIDs(handle)
	if err != nil {
		return nil, fmt.Errorf("reading live query checkpoint IDs: %w", err)
	}

	s.LiveQueryCheckpointIDs = liveCheckpoints

	qcpSchedule, err := query.ReadQueryCheckpointSchedule(reader)
	if err != nil {
		return nil, fmt.Errorf("reading query checkpoint schedule: %w", err)
	}

	s.QueryCheckpointSchedule = qcpSchedule

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(reader)
	if err != nil {
		return nil, fmt.Errorf("reading last applied timestamp: %w", err)
	}

	s.LastAppliedTimestamp = lastAppliedTimestamp

	nextLedgerID, err := query.ReadNextLedgerID(reader)
	if err != nil {
		return nil, fmt.Errorf("reading next ledger ID: %w", err)
	}

	s.NextLedgerID = nextLedgerID

	clusterState, err := query.ReadClusterState(reader)
	if err != nil {
		return nil, fmt.Errorf("reading cluster state: %w", err)
	}

	if clusterState != nil {
		s.LastClusterConfig = clusterState.GetConfig()
		s.HashGenerator = processing.NewHashGenerator(clusterState.GetConfig().GetHashAlgorithm(), clusterID)
		s.CacheEpoch = clusterState.GetCacheEpoch()
	}

	clusterPolicy, err := query.ReadClusterPolicy(reader)
	if err != nil {
		return nil, fmt.Errorf("reading cluster policy: %w", err)
	}

	if clusterPolicy != nil {
		s.ClusterPolicy = clusterPolicy
	}

	return s, nil
}
