package state

import (
	"fmt"

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
// (channels, mutexes, metrics), and shared sub-trackers (Registry, Chapters,
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
	PendingLedgerCleanups   map[string]uint64

	// Last cluster config applied + derived hash generator. Persisted under
	// ZoneGlobal so it survives restarts.
	LastClusterConfig *commonpb.ClusterConfig
	HashGenerator     processing.HashGenerator

	// CacheEpoch is the persisted cache epoch read alongside LastClusterConfig.
	// Carried in FSMState so that LoadFSMStateFromStore performs exactly one
	// cluster-state read per recovery; consumers (Registry.Cache.SetEpoch) read
	// the value from the swapped state instead of issuing a second Pebble Get.
	CacheEpoch uint64

	// ClusterID is the immutable identifier injected at boot. Kept here so
	// rebuilds of HashGenerator (after a ClusterConfig change) have it at
	// hand without having to plumb it from elsewhere.
	ClusterID string

	// LastFSMDigest is the rolling cross-node FSM digest of every persisted
	// byte up to LastAppliedIndex (when fsm_determinism_enabled is ON).
	// Empty when the flag is OFF or when no digest has been computed yet
	// (pre-feature cluster, first apply post-restore). Persisted under
	// [ZoneGlobal][SubGlobFSMDigest] with the same atomicity as the rest
	// of the batch — Pebble's batch commit is the recovery boundary, so
	// LastFSMDigest is always consistent with LastAppliedIndex.
	LastFSMDigest []byte
}

// NewFSMState builds a fresh FSMState. Counters start at their canonical
// initial values; the map is allocated empty.
func NewFSMState(clusterID string) *FSMState {
	return &FSMState{
		NextSequenceID:        1,
		NextAuditSequenceID:   1,
		NextLedgerID:          1,
		PendingLedgerCleanups: map[string]uint64{},
		HashGenerator:         processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID),
		ClusterID:             clusterID,
	}
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
func (s *FSMState) AppendAuditEntry(hash []byte) uint64 {
	sequence := s.NextAuditSequenceID
	s.LastAuditHash = hash
	s.NextAuditSequenceID++

	return sequence
}

// LoadFSMStateFromStore reads every FSM-level field that lives in FSMState
// from the main Pebble store, returning a fully-hydrated value ready to be
// swapped into a Machine via Machine.RestoreState. Sub-trackers (Chapters,
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

	if lastSeq > 0 {
		s.NextSequenceID = lastSeq + 1
	}

	lastAuditEntry, err := query.ReadLastAuditEntry(handle)
	if err != nil {
		return nil, fmt.Errorf("reading last audit entry: %w", err)
	}

	if lastAuditEntry != nil {
		s.LastAuditHash = lastAuditEntry.GetHash()
		s.NextAuditSequenceID = lastAuditEntry.GetSequence() + 1
	}

	nextQCPID, err := query.ReadNextQueryCheckpointID(reader)
	if err != nil {
		return nil, fmt.Errorf("reading next query checkpoint ID: %w", err)
	}

	s.NextQueryCheckpointID = nextQCPID

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

	pendingCleanups, err := query.ReadPendingLedgerCleanups(handle)
	if err != nil {
		return nil, fmt.Errorf("reading pending ledger cleanups: %w", err)
	}

	s.PendingLedgerCleanups = pendingCleanups

	clusterState, err := query.ReadClusterState(reader)
	if err != nil {
		return nil, fmt.Errorf("reading cluster state: %w", err)
	}

	if clusterState != nil {
		s.LastClusterConfig = clusterState.GetConfig()
		s.HashGenerator = processing.NewHashGenerator(clusterState.GetConfig().GetHashAlgorithm(), clusterID)
		s.CacheEpoch = clusterState.GetCacheEpoch()
	}

	// LastFSMDigest is the rolling cross-node FSM digest persisted under
	// SubGlobFSMDigest. Absent when fsm_determinism_enabled is OFF or on
	// pre-feature clusters; either way, the apply path treats nil as
	// "chain starts from the snapshotIndex anchor".
	_, _, digest, err := query.ReadFSMDigest(reader)
	if err != nil {
		return nil, fmt.Errorf("reading fsm digest: %w", err)
	}

	s.LastFSMDigest = digest

	return s, nil
}
