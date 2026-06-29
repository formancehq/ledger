package state

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ErrBackupInProgress rejects a BackupOrderStart whose destination
// already has a Running job. Cluster-wide mutual exclusion lives in the
// apply path; the only honest answer to a duplicate Start is "wait".
var ErrBackupInProgress = errors.New("backup: destination already has a running job")

// ErrBackupJobNotFound rejects a Complete / Fail order whose job_id no
// longer matches the active job for any destination. Most commonly this
// fires after the cleanup loop has presumed the job orphaned and proposed
// Fail for it before the executor's Complete lands.
var ErrBackupJobNotFound = errors.New("backup: no active job matches the supplied job_id")

// ErrBackupJobIDCollision rejects a BackupOrderStart whose job_id is
// already active under a different destination. The lifecycle orders
// (Complete / Fail) address jobs by job_id, so unicity of job_id across
// all active destinations is an FSM invariant — without it,
// findActiveByJobID would have to choose between two candidates and the
// choice could differ across nodes (Go map iteration order is not
// guaranteed stable), breaking determinism. The orchestrator generates
// job_ids via crypto/rand so this should never fire in practice; it is
// a defensive backstop the apply path enforces in case a future change
// introduces a non-random source.
var ErrBackupJobIDCollision = errors.New("backup: job_id already active on a different destination")

// BackupJobsState is the FSM-managed view of every in-flight backup
// job, keyed by destination_key. It mirrors
// [ZoneClusterTransient][SubTransientBackupJob][destination_key] on
// disk so the FSM apply path never reads from Pebble (CLAUDE.md
// invariant #3).
//
// Mutual exclusion is per-destination: two Start orders targeting
// byte-equal destinations contend for the same map slot; two targeting
// different destinations are orthogonal. This is the same semantic the
// per-destination leased lock (#299) was building toward, now applied
// directly inside the deterministic Raft apply.
//
// All apply-path decisions take an applied index (raftIndex) supplied
// by the FSM dispatcher, never time.Now(): the FSM must be
// deterministic across nodes (CLAUDE.md invariant #2) and the proposer
// wall clock is untrustworthy (#298). Liveness — "is the executor
// goroutine still alive?" — is NOT inferred here from progress
// staleness; it is observed in-memory on the leader. See
// internal/application/backup/cleanup.go for the rationale.
type BackupJobsState struct {
	mu     sync.RWMutex
	active map[string]*raftcmdpb.BackupJob
}

// NewBackupJobsState returns an empty BackupJobsState.
func NewBackupJobsState() *BackupJobsState {
	return &BackupJobsState{
		active: make(map[string]*raftcmdpb.BackupJob),
	}
}

// CanonicalDestinationKey returns the deterministic 32-byte digest of a
// BackupDestination. The same destination always yields the same key so
// concurrent Start orders aimed at it land on the same map slot.
//
// We hash a length-prefixed concatenation of every field rather than
// re-marshaling the proto so the encoding is independent of protobuf
// field-ordering ambiguity and future field additions.
//
// Two destinations that resolve to the same physical storage location
// must produce the same key. For Azure that means a missing endpoint
// canonicalises to the default `https://<account>.blob.core.windows.net/`,
// matching what the SDK uses at runtime — otherwise an explicit-default
// call and an empty-endpoint call would hash differently and the FSM
// mutex would not protect concurrent writes to the same container.
func CanonicalDestinationKey(dst *raftcmdpb.BackupDestination) []byte {
	h := sha256.New()
	writePart := func(label, value string) {
		var lenBuf [8]byte
		binary.BigEndian.PutUint32(lenBuf[:4], uint32(len(label)))
		binary.BigEndian.PutUint32(lenBuf[4:], uint32(len(value)))
		_, _ = h.Write(lenBuf[:])
		_, _ = h.Write([]byte(label))
		_, _ = h.Write([]byte(value))
	}

	writePart("bucket_id", dst.GetBucketId())

	// base_path is *not* hashed: it is documented as "reserved for future
	// use" and the inner backup runners (RunBackup / RunIncrementalBackup)
	// ignore it. Including it in the destination key would let two callers
	// race the same actual manifest under different base_path values —
	// each would land on a distinct map slot and the FSM mutual exclusion
	// would not protect concurrent writes to the same prefix.

	switch t := dst.GetTarget().(type) {
	case *raftcmdpb.BackupDestination_S3:
		writePart("target", "s3")
		writePart("s3_bucket", t.S3.GetBucket())
		writePart("s3_endpoint", canonicalS3Endpoint(t.S3.GetEndpoint()))
		// s3_region is *not* hashed: it never determines which object
		// namespace we write into. AWS S3 bucket names are globally
		// unique, so two callers with the same bucket name target the
		// same physical bucket regardless of the region hint. On
		// MinIO/S3-compatible setups with an explicit endpoint, region
		// is pure metadata the SDK ignores for path construction. In
		// both cases two callers that differ only on region would
		// bypass the FSM destination mutex while still racing the same
		// manifest, so the key excludes it.
	case *raftcmdpb.BackupDestination_Azure:
		writePart("target", "azure")
		writePart("azure_account_name", t.Azure.GetAccountName())
		writePart("azure_container", t.Azure.GetContainer())
		writePart("azure_endpoint", canonicalAzureEndpoint(t.Azure.GetAccountName(), t.Azure.GetEndpoint()))
	default:
		writePart("target", "none")
	}

	return h.Sum(nil)
}

// canonicalEndpoint normalises any endpoint URL into a single form so
// purely cosmetic differences (e.g. a trailing slash) do not hash into
// distinct destination slots. The storage client treats
// `http://minio:9000` and `http://minio:9000/` identically; without
// this normalisation a careless caller could bypass the FSM mutex by
// alternating the two forms.
func canonicalEndpoint(endpoint string) string {
	return strings.TrimRight(endpoint, "/")
}

// awsS3EndpointRE matches every public AWS S3 endpoint shape: the
// global `s3.amazonaws.com`, regional `s3.<region>.amazonaws.com`,
// transfer-acceleration `s3-accelerate.amazonaws.com`, dual-stack
// `s3.dualstack.<region>.amazonaws.com`, and FIPS / similar
// `s3-fips.<region>.amazonaws.com` variants. All of them route to
// the same AWS bucket namespace (bucket names are globally unique),
// so they must collapse to one canonical form for the FSM mutex to
// hold across spellings.
var awsS3EndpointRE = regexp.MustCompile(`^https?://s3([.-][a-z0-9-]+)*\.amazonaws\.com$`)

// canonicalS3Endpoint extends canonicalEndpoint with AWS-endpoint
// folding. Empty endpoint (SDK default) and any *.amazonaws.com S3
// URL collapse to the same canonical form, so callers that switch
// between leaving the field blank and supplying an explicit AWS URL
// for the same bucket share the FSM destination slot. Non-AWS
// endpoints (MinIO, custom S3-compatible) are returned trimmed only;
// the endpoint really does select the physical namespace there.
func canonicalS3Endpoint(endpoint string) string {
	trimmed := canonicalEndpoint(endpoint)
	if trimmed == "" || awsS3EndpointRE.MatchString(trimmed) {
		return ""
	}

	return trimmed
}

// canonicalAzureEndpoint returns the endpoint that the Azure SDK would
// actually use at runtime, in a single canonical form. Three callers
// can spell the same destination three ways:
//
//  1. endpoint=""                                      → SDK default
//  2. endpoint="https://<account>.blob.core.windows.net/"  → default written long-hand
//  3. endpoint="https://<account>.blob.core.windows.net"   → same, no trailing slash
//
// All three resolve to the same container at runtime; they must hash
// to the same destination_key so the FSM slot is shared. We substitute
// the no-slash default for the empty form and trim any trailing slash
// from explicit endpoints.
func canonicalAzureEndpoint(accountName, endpoint string) string {
	if endpoint == "" {
		return fmt.Sprintf("https://%s.blob.core.windows.net", accountName)
	}

	return canonicalEndpoint(endpoint)
}

// Start opens a new backup job for the given destination, kind and
// executor. Returns (job, true, nil) on success and (existing, false,
// ErrBackupInProgress) when the destination is already running a job.
// The kind is carried by the *order* (BackupOrder vs IncrementalBackupOrder)
// so it is supplied as an argument rather than inferred from any field.
func (s *BackupJobsState) Start(
	batch *dal.WriteSession,
	raftIndex uint64,
	kind raftcmdpb.BackupKind,
	start *raftcmdpb.BackupOrderStart,
) (*raftcmdpb.BackupJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := CanonicalDestinationKey(start.GetDestination())
	if existing, ok := s.active[string(key)]; ok {
		return existing, false, ErrBackupInProgress
	}

	// job_id must be unique across all active destinations: the lifecycle
	// orders (Complete / Fail) address by job_id and a collision would let
	// two destinations contend for the same lookup. See ErrBackupJobIDCollision.
	if collision := s.findActiveByJobID(start.GetJobId()); collision != nil {
		return collision, false, ErrBackupJobIDCollision
	}

	// Clone the destination: start.GetDestination() points into the
	// pooled Proposal that machine.ApplyEntries returns to the
	// vtprotobuf pool after this apply. Without the clone, a future
	// reuse of that pooled object would overwrite the Destination
	// fields we just stored in long-lived FSM state, corrupting
	// history records and CanonicalDestinationKey re-derivation. The
	// other proposal fields we copy (JobId, kind, ExecutorNodeId)
	// are scalars, no aliasing concern.
	job := &raftcmdpb.BackupJob{
		JobId:                 start.GetJobId(),
		Kind:                  kind,
		Status:                raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_RUNNING,
		Destination:           start.GetDestination().CloneVT(),
		DestinationKey:        key,
		ExecutorNodeId:        start.GetExecutorNodeId(),
		StartedAtAppliedIndex: raftIndex,
	}

	if err := writeBackupJob(batch, key, job); err != nil {
		return nil, false, err
	}

	s.active[string(key)] = job

	return job, true, nil
}

// Complete moves the job from the active map to the history store and
// frees the destination slot. The executor populates the final upload
// counters on the order — the FSM stores them on the history record
// for operator inspection but never reads them. Idempotent: a Complete
// against an already-completed (or unknown) job returns
// ErrBackupJobNotFound, so retrying a Complete propose against the same
// job_id after success signals the caller their intent already landed.
func (s *BackupJobsState) Complete(
	batch *dal.WriteSession,
	raftIndex uint64,
	complete *raftcmdpb.BackupOrderComplete,
) (*raftcmdpb.BackupJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job := s.findActiveByJobID(complete.GetJobId())
	if job == nil {
		return nil, ErrBackupJobNotFound
	}

	job.Status = raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_COMPLETE
	job.CompletedAtAppliedIndex = raftIndex
	job.LastLogSequence = complete.GetLastLogSequence()
	job.LastAuditSequence = complete.GetLastAuditSequence()
	job.LastAppliedIndex = complete.GetLastAppliedIndex()
	job.FilesUploaded = complete.GetFilesUploaded()
	job.SegmentsUploaded = complete.GetSegmentsUploaded()

	if err := s.terminate(batch, job); err != nil {
		return nil, err
	}

	return job, nil
}

// Fail mirrors Complete but stamps Status=FAILED and a failure message.
func (s *BackupJobsState) Fail(
	batch *dal.WriteSession,
	raftIndex uint64,
	fail *raftcmdpb.BackupOrderFail,
) (*raftcmdpb.BackupJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job := s.findActiveByJobID(fail.GetJobId())
	if job == nil {
		return nil, ErrBackupJobNotFound
	}

	job.Status = raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_FAILED
	job.CompletedAtAppliedIndex = raftIndex
	job.FailureMessage = fail.GetMessage()

	if err := s.terminate(batch, job); err != nil {
		return nil, err
	}

	return job, nil
}

// terminate persists the job to history, deletes the active entry, and
// drops it from the in-memory map. Called by Complete and Fail.
func (s *BackupJobsState) terminate(batch *dal.WriteSession, job *raftcmdpb.BackupJob) error {
	if err := writeBackupJobHistory(batch, job); err != nil {
		return err
	}

	if err := deleteBackupJob(batch, job.GetDestinationKey()); err != nil {
		return err
	}

	delete(s.active, string(job.GetDestinationKey()))

	return nil
}

// findActiveByJobID linear-scans the active map. The map has at most a
// handful of entries in practice (one per destination), so an explicit
// secondary index would be premature.
func (s *BackupJobsState) findActiveByJobID(jobID uint64) *raftcmdpb.BackupJob {
	for _, job := range s.active {
		if job.GetJobId() == jobID {
			return job
		}
	}

	return nil
}

// ActiveByDestination returns a clone of the active job for a
// destination, or nil when none exists. Used by RPC handlers and the
// cleanup loop; mutating the returned value does not affect FSM state.
func (s *BackupJobsState) ActiveByDestination(dst *raftcmdpb.BackupDestination) *raftcmdpb.BackupJob {
	key := CanonicalDestinationKey(dst)

	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.active[string(key)]
	if !ok {
		return nil
	}

	return job.CloneVT()
}

// Snapshot returns a clone of the in-memory active map. Used by tests
// and diagnostic logging; production callers should prefer ForEachActive
// to avoid the per-tick clone.
func (s *BackupJobsState) Snapshot() map[string]*raftcmdpb.BackupJob {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string]*raftcmdpb.BackupJob, len(s.active))
	for k, v := range s.active {
		out[k] = v.CloneVT()
	}

	return out
}

// ForEachActive invokes fn for every active job under the read lock.
// fn must not mutate the value (it points into the FSM-owned map) and
// must not call back into the BackupJobsState — the lock is held for
// the duration of the iteration.
func (s *BackupJobsState) ForEachActive(fn func(*raftcmdpb.BackupJob)) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, job := range s.active {
		fn(job)
	}
}

// Reset clears the in-memory map. Called during snapshot restore before
// RestoreFromStore repopulates from Pebble.
func (s *BackupJobsState) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.active = make(map[string]*raftcmdpb.BackupJob)
}

// RestoreFromStore scans [ZoneClusterTransient][SubTransientBackupJob]
// and rebuilds the active-jobs map. Called at boot and after snapshot
// restore (within the same cluster — a cross-cluster restore wipes the
// zone before this runs, see FinalizeRestore). Boot path only; not
// allowed in the apply hot path.
func (s *BackupJobsState) RestoreFromStore(reader dal.PebbleReader) error {
	prefix := []byte{dal.ZoneClusterTransient, dal.SubTransientBackupJob}

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: backupJobUpperBound(),
	})
	if err != nil {
		return fmt.Errorf("creating iterator for backup-job restore: %w", err)
	}

	defer func() { _ = iter.Close() }()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.active = make(map[string]*raftcmdpb.BackupJob)

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= len(prefix) {
			continue
		}

		destKey := bytes.Clone(key[len(prefix):])

		valBytes, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading backup-job value for %x: %w", destKey, err)
		}

		job := &raftcmdpb.BackupJob{}
		if err := job.UnmarshalVT(valBytes); err != nil {
			return fmt.Errorf("unmarshaling backup-job for %x: %w", destKey, err)
		}

		s.active[string(destKey)] = job
	}

	if err := iter.Error(); err != nil {
		return fmt.Errorf("iterating backup-job entries: %w", err)
	}

	return nil
}

// backupJobKey builds the Pebble key for a destination's active entry.
func backupJobKey(destinationKey []byte) []byte {
	out := make([]byte, 2+len(destinationKey))
	out[0] = dal.ZoneClusterTransient
	out[1] = dal.SubTransientBackupJob
	copy(out[2:], destinationKey)

	return out
}

// backupJobUpperBound is the exclusive upper bound for an iterator
// scoped to [ZoneClusterTransient][SubTransientBackupJob], following
// the same pattern as leasedLockUpperBound.
func backupJobUpperBound() []byte {
	return []byte{dal.ZoneClusterTransient, dal.SubTransientBackupJob + 1}
}

// backupJobHistoryKey builds the Pebble key for one history entry,
// sorted by completed_at_applied_index so the most recent N entries are
// a tail-read. The destination key is appended as a tie-breaker: two
// terminal proposals applied in the same Raft batch share the index, so
// without the suffix they would collide in Pebble and one history row
// would be lost.
func backupJobHistoryKey(completedAtIndex uint64, destinationKey []byte) []byte {
	out := make([]byte, 2+8+len(destinationKey))
	out[0] = dal.ZoneClusterTransient
	out[1] = dal.SubTransientBackupJobHistory
	binary.BigEndian.PutUint64(out[2:], completedAtIndex)
	copy(out[2+8:], destinationKey)

	return out
}

func writeBackupJob(batch *dal.WriteSession, destinationKey []byte, job *raftcmdpb.BackupJob) error {
	// nil batch is a test affordance — mirrors the LeasedLockState
	// pattern. Production callers always pass a real batch.
	if batch == nil {
		return nil
	}

	// Route through batch.SetProto so the cluster-wide deterministic flag
	// controls the marshal (sorts map keys when ON, no-op when OFF).
	return batch.SetProto(backupJobKey(destinationKey), job)
}

func deleteBackupJob(batch *dal.WriteSession, destinationKey []byte) error {
	if batch == nil {
		return nil
	}

	// DeleteKey, not SingleDeleteKey: writeBackupJob rewrites the active
	// entry on every Start in case a future order needs to mutate it
	// (today we only Start then Complete/Fail without intermediate
	// writes, but the keying assumption stays the same). Pebble's docs
	// are explicit that a SingleDelete on a rewritten key can be
	// reordered with prior Sets and resurrect the value after compaction,
	// which would leave the destination locked even though the job has
	// terminated.
	return batch.DeleteKey(backupJobKey(destinationKey))
}

func writeBackupJobHistory(batch *dal.WriteSession, job *raftcmdpb.BackupJob) error {
	if batch == nil {
		return nil
	}

	return batch.SetProto(backupJobHistoryKey(job.GetCompletedAtAppliedIndex(), job.GetDestinationKey()), job)
}
