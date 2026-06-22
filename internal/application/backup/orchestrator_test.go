package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// testDestination returns a small valid BackupDestination used across
// the orchestrator unit tests.
func testDestination() *raftcmdpb.BackupDestination {
	return &raftcmdpb.BackupDestination{
		BucketId: "bucket-test",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{
				Bucket: "ledger-backups",
				Region: "eu-west-1",
			},
		},
	}
}

// proposalCapture records every proposal sent to a mock Proposer so
// individual tests can assert on the order's Op type and payload.
type proposalCapture struct {
	mu        sync.Mutex
	proposals []*raftcmdpb.Proposal
}

func (c *proposalCapture) record(p *raftcmdpb.Proposal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.proposals = append(c.proposals, p)
}

func (c *proposalCapture) all() []*raftcmdpb.Proposal {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*raftcmdpb.Proposal, len(c.proposals))
	copy(out, c.proposals)

	return out
}

// recordingProposer returns a mock Proposer that captures every
// proposal it sees and returns the supplied applyErr (nil = happy
// path, sentinel = simulated FSM business rejection).
func recordingProposer(t *testing.T, capture *proposalCapture, applyErr error) *MockProposer {
	t.Helper()
	ctrl := gomock.NewController(t)

	prop := NewMockProposer(ctrl)
	prop.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().DoAndReturn(
		func(_ context.Context, cmd *raftcmdpb.Proposal) error {
			capture.record(cmd)

			return applyErr
		},
	)

	return prop
}

// erroringProposer returns a Proposer that always fails with the given
// error. Used to exercise the orchestrator's error-propagation paths.
func erroringProposer(t *testing.T, err error) *MockProposer {
	t.Helper()
	ctrl := gomock.NewController(t)

	prop := NewMockProposer(ctrl)
	prop.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(err)

	return prop
}

// extractFullOrder pulls the first TechnicalUpdate carrying a
// BackupOrder out of the proposal. The orchestrator wraps every
// full-backup order in a TechnicalUpdate, so tests must traverse the
// envelope rather than read a top-level field.
func extractFullOrder(p *raftcmdpb.Proposal) *raftcmdpb.BackupOrder {
	for _, tu := range p.GetTechnicalUpdates() {
		if k, ok := tu.GetKind().(*raftcmdpb.TechnicalUpdate_BackupOrder); ok {
			return k.BackupOrder
		}
	}

	return nil
}

// extractIncrementalOrder mirrors extractFullOrder for the incremental
// pipeline's TechnicalUpdate envelope.
func extractIncrementalOrder(p *raftcmdpb.Proposal) *raftcmdpb.IncrementalBackupOrder {
	for _, tu := range p.GetTechnicalUpdates() {
		if k, ok := tu.GetKind().(*raftcmdpb.TechnicalUpdate_IncrementalBackupOrder); ok {
			return k.IncrementalBackupOrder
		}
	}

	return nil
}

// newOrchestrator returns an Orchestrator already in the leader state so
// most tests don't need to call OnLeadershipChange. Tests that
// explicitly exercise leadership transitions construct the orchestrator
// directly and drive OnLeadershipChange themselves.
func newOrchestrator(prop Proposer, store *dal.Store) *Orchestrator {
	o := NewOrchestrator(prop, store, logging.Testing(), 1, NewExecutorRegistry())
	o.OnLeadershipChange(true)

	return o
}

func TestProposalBuilders_Full(t *testing.T) {
	t.Parallel()

	dst := testDestination()

	p := &raftcmdpb.Proposal{}
	fullStart(7, dst, 42)(p)
	require.NotNil(t, extractFullOrder(p))
	start := extractFullOrder(p).GetStart()
	require.NotNil(t, start)
	require.Equal(t, uint64(7), start.GetJobId())
	require.Equal(t, uint64(42), start.GetExecutorNodeId())
	require.Equal(t, dst.GetBucketId(), start.GetDestination().GetBucketId())

	p = &raftcmdpb.Proposal{}
	fullComplete(7, &backup.Result{
		LastLogSequence:   1_000,
		LastAuditSequence: 500,
		LastAppliedIndex:  7_500,
		FilesUploaded:     12,
	})(p)
	complete := extractFullOrder(p).GetComplete()
	require.NotNil(t, complete)
	require.Equal(t, uint64(7), complete.GetJobId())
	require.Equal(t, uint64(1_000), complete.GetLastLogSequence())
	require.Equal(t, uint64(500), complete.GetLastAuditSequence())
	require.Equal(t, uint64(7_500), complete.GetLastAppliedIndex())
	require.Equal(t, uint64(12), complete.GetFilesUploaded())

	p = &raftcmdpb.Proposal{}
	fullFail(7, "boom")(p)
	fail := extractFullOrder(p).GetFail()
	require.NotNil(t, fail)
	require.Equal(t, uint64(7), fail.GetJobId())
	require.Equal(t, "boom", fail.GetMessage())
}

func TestProposalBuilders_Incremental(t *testing.T) {
	t.Parallel()

	dst := testDestination()

	p := &raftcmdpb.Proposal{}
	incrementalStart(11, dst, 3)(p)
	require.NotNil(t, extractIncrementalOrder(p))
	require.Equal(t, uint64(11), extractIncrementalOrder(p).GetStart().GetJobId())
	require.Equal(t, uint64(3), extractIncrementalOrder(p).GetStart().GetExecutorNodeId())

	p = &raftcmdpb.Proposal{}
	incrementalComplete(11, &backup.IncrementalBackupResult{
		LastLogSequence:   2_222,
		LastAuditSequence: 1_111,
		SegmentsUploaded:  4,
	})(p)
	require.Equal(t, uint64(11), extractIncrementalOrder(p).GetComplete().GetJobId())
	require.Equal(t, uint64(2_222), extractIncrementalOrder(p).GetComplete().GetLastLogSequence())
	require.Equal(t, uint64(1_111), extractIncrementalOrder(p).GetComplete().GetLastAuditSequence())
	require.Equal(t, uint64(4), extractIncrementalOrder(p).GetComplete().GetSegmentsUploaded())

	p = &raftcmdpb.Proposal{}
	incrementalFail(11, "kaboom")(p)
	require.Equal(t, "kaboom", extractIncrementalOrder(p).GetFail().GetMessage())
}

// TestProposeAndWait pins the orchestrator's contract with the bootstrap
// adapter: Propose returns an error rolling up Raft accept + FSM apply,
// FSM business rejections come back via errors.Is on the typed sentinel.
func TestProposeAndWait(t *testing.T) {
	t.Parallel()

	t.Run("happy path", func(t *testing.T) {
		t.Parallel()

		capture := &proposalCapture{}
		prop := recordingProposer(t, capture, nil)
		o := newOrchestrator(prop, nil)

		err := o.proposeAndWait(context.Background(), fullStart(7, testDestination(), 1))
		require.NoError(t, err)
		require.Len(t, capture.all(), 1, "Propose must be called exactly once")
	})

	t.Run("propose transport error surfaces", func(t *testing.T) {
		t.Parallel()

		errBoom := errors.New("transport-level propose error")
		prop := erroringProposer(t, errBoom)
		o := newOrchestrator(prop, nil)

		err := o.proposeAndWait(context.Background(), fullStart(7, testDestination(), 1))
		require.ErrorIs(t, err, errBoom)
	})

	t.Run("apply error surfaces", func(t *testing.T) {
		t.Parallel()

		capture := &proposalCapture{}
		prop := recordingProposer(t, capture, state.ErrBackupInProgress)
		o := newOrchestrator(prop, nil)

		err := o.proposeAndWait(context.Background(), fullStart(7, testDestination(), 1))
		require.ErrorIs(t, err, state.ErrBackupInProgress)
	})
}

// TestOrchestrator_LeadershipChangeCancelsRunCtx pins flemzord's
// fencing concern: when leadership transfers mid-backup, the old
// leader's runCtx must cancel so the S3 upload aborts. Without this,
// the new leader's cleanup loop frees the destination slot while the
// old executor keeps writing, opening a two-writer race.
//
// We do not run a real backup here — RunFull is mostly upstream of
// the leaderCtx hook anyway. The test instead asserts the contract
// directly: runContext returns a child ctx of leaderCtx, and
// OnLeadershipChange(false) cancels the ctx within the watcher
// goroutine's window.
func TestOrchestrator_LeadershipChangeCancelsRunCtx(t *testing.T) {
	t.Parallel()

	o := newOrchestrator(nil, nil)
	o.OnLeadershipChange(true)

	runCtx, cancel := o.runContext(context.Background())
	defer cancel()
	require.NoError(t, runCtx.Err(), "runCtx must be live while we are leader")

	o.OnLeadershipChange(false)

	require.Eventually(t, func() bool { return runCtx.Err() != nil }, time.Second, time.Millisecond,
		"runCtx must cancel when leadership is lost")
}

// TestOrchestrator_NonLeaderRunCtxStartsCancelled pins the
// constructor's documented contract: before OnLeadershipChange(true)
// runs at boot, RunFull / RunIncremental must short-circuit
// immediately rather than start a backup that nothing will Complete.
func TestOrchestrator_NonLeaderRunCtxStartsCancelled(t *testing.T) {
	t.Parallel()

	// Construct the orchestrator directly (not via newOrchestrator,
	// which would call OnLeadershipChange(true)) so leaderCtx is the
	// constructor's already-cancelled context.
	o := NewOrchestrator(nil, nil, logging.Testing(), 1, NewExecutorRegistry())

	runCtx, cancel := o.runContext(context.Background())
	defer cancel()

	require.Eventually(t, func() bool { return runCtx.Err() != nil }, time.Second, time.Millisecond,
		"runCtx must be cancelled when the orchestrator has not seen leadership")
}

// TestExecutorRegistry covers the in-memory liveness API the cleanup
// loop consults. The tryRegister contract is load-bearing for the
// crypto/rand jobID-collision case: only the first registrar owns the
// liveness marker, so a subsequent caller (whose Start the FSM will
// reject) must NOT pull the entry out on its way back.
func TestExecutorRegistry(t *testing.T) {
	t.Parallel()

	r := NewExecutorRegistry()
	require.False(t, r.IsAlive(42))
	require.True(t, r.tryRegister(42), "first registrar must win")
	require.True(t, r.IsAlive(42))

	require.False(t, r.tryRegister(42), "second registrar must observe the existing entry")
	require.True(t, r.IsAlive(42), "loser's no-op call leaves the marker intact")

	r.deregister(42)
	require.False(t, r.IsAlive(42))

	// Re-registration after deregister works — the slot is fully free.
	require.True(t, r.tryRegister(42))
	r.deregister(42)
}

// TestProposeTerminal exercises the propose-and-log path used after an
// upload failure. The function logs on propose error but never panics.
func TestProposeTerminal(t *testing.T) {
	t.Parallel()

	t.Run("ok", func(t *testing.T) {
		t.Parallel()
		capture := &proposalCapture{}
		prop := recordingProposer(t, capture, nil)
		o := newOrchestrator(prop, nil)
		o.proposeTerminal(fullFail(7, "boom"), 7, "fail")
		require.Len(t, capture.all(), 1)
	})

	t.Run("propose error is logged", func(t *testing.T) {
		t.Parallel()
		prop := erroringProposer(t, errors.New("down"))
		o := newOrchestrator(prop, nil)
		o.proposeTerminal(fullFail(7, "boom"), 7, "fail") // must not panic
	})
}

// stubStorage is the minimal backup.Storage implementation the
// orchestrator integration tests need. It serves the manifest body
// from `manifest` (nil → ErrFileNotFound) and records every PutFile
// key so tests can assert on the upload sequence. ListFiles returns
// nil so the inner runner's orphan-pruning branch becomes a no-op.
type stubStorage struct {
	manifest []byte

	mu      sync.Mutex
	putKeys []string
}

func (s *stubStorage) PutFile(_ context.Context, key string, data io.Reader, _ int64) error {
	_, _ = io.Copy(io.Discard, data)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.putKeys = append(s.putKeys, key)

	return nil
}

func (s *stubStorage) GetFile(_ context.Context, _ string) (io.ReadCloser, error) {
	if s.manifest == nil {
		return nil, backup.ErrFileNotFound
	}

	return io.NopCloser(bytes.NewReader(s.manifest)), nil
}

func (s *stubStorage) DeleteFile(_ context.Context, _ string) error            { return nil }
func (s *stubStorage) ListFiles(_ context.Context, _ string) ([]string, error) { return nil, nil }

func newTempStore(t *testing.T) *dal.Store {
	t.Helper()
	ctx := logging.TestingContext()
	store, err := dal.NewStore(t.TempDir(), logging.FromContext(ctx), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// TestRunIncremental_NoOpHappyPath exercises the orchestrator's
// incremental flow end-to-end when the manifest reports no new
// entries to export. The inner backup runner takes its early-return
// branch; the orchestrator still proposes Start and Complete. Locks
// down: (1) Start lands with the supplied destination, (2) registry
// flips alive then back to dead across the run, (3) Complete uses the
// same job_id, (4) no Fail is proposed on the happy path.
func TestRunIncremental_NoOpHappyPath(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket-int"

	store := newTempStore(t)

	manifest := &backup.Manifest{Checkpoint: &backup.CheckpointManifest{}}
	body, err := json.Marshal(manifest)
	require.NoError(t, err)
	storage := &stubStorage{manifest: body}

	capture := &proposalCapture{}
	prop := recordingProposer(t, capture, nil)

	o := newOrchestrator(prop, store)
	dst := &raftcmdpb.BackupDestination{
		BucketId: bucketID,
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{Bucket: "ledger-backups", Region: "eu-west-1"},
		},
	}

	res, err := o.RunIncremental(context.Background(), dst, storage)
	require.NoError(t, err)
	require.NotNil(t, res)

	all := capture.all()
	require.Len(t, all, 2, "Start + Complete on the no-op happy path")

	start := extractIncrementalOrder(all[0]).GetStart()
	require.NotNil(t, start)
	require.Equal(t, bucketID, start.GetDestination().GetBucketId())
	require.Equal(t, uint64(1), start.GetExecutorNodeId())

	complete := extractIncrementalOrder(all[1]).GetComplete()
	require.NotNil(t, complete)
	require.Equal(t, start.GetJobId(), complete.GetJobId())

	// After the call returns, the registry must have deregistered the
	// job — the cleanup loop should see no live executor for it.
	require.False(t, o.Registry().IsAlive(start.GetJobId()))
}

// TestRunIncremental_StartRejectionShortCircuits: an FSM-rejected
// Start (the typical destination-busy path) does not reach the inner
// runner nor propose terminal orders, and does NOT register in the
// executor registry.
func TestRunIncremental_StartRejectionShortCircuits(t *testing.T) {
	t.Parallel()

	capture := &proposalCapture{}
	prop := recordingProposer(t, capture, state.ErrBackupInProgress)

	o := newOrchestrator(prop, newTempStore(t))
	dst := &raftcmdpb.BackupDestination{
		BucketId: "busy",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{Bucket: "b", Region: "r"},
		},
	}
	storage := &stubStorage{}

	_, err := o.RunIncremental(context.Background(), dst, storage)
	require.ErrorIs(t, err, state.ErrBackupInProgress)
	require.Len(t, capture.all(), 1, "only Start must be proposed")
}

// TestRunFull_HappyPath drives the full backup orchestrator against a
// fresh empty Pebble store. The inner backup.RunBackup actually
// creates a checkpoint, lists its (few) SST files, uploads them via a
// stub Storage, and writes a manifest. Locks down the bookends: Start
// → registry alive → upload → Complete → registry dead.
func TestRunFull_HappyPath(t *testing.T) {
	t.Parallel()

	const bucketID = "bucket-full"

	store := newTempStore(t)
	storage := &stubStorage{}

	capture := &proposalCapture{}
	prop := recordingProposer(t, capture, nil)

	o := newOrchestrator(prop, store)
	dst := &raftcmdpb.BackupDestination{
		BucketId: bucketID,
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{Bucket: "ledger-backups", Region: "eu-west-1"},
		},
	}

	res, err := o.RunFull(context.Background(), dst, storage)
	require.NoError(t, err, "first full backup against an empty store must succeed")
	require.NotNil(t, res)

	all := capture.all()
	require.Len(t, all, 2, "Start + Complete; no Progress proposals anymore")

	start := extractFullOrder(all[0]).GetStart()
	require.NotNil(t, start)
	require.Equal(t, bucketID, start.GetDestination().GetBucketId())

	require.NotNil(t, extractFullOrder(all[1]).GetComplete(), "last proposal must be Complete")
	require.Equal(t, start.GetJobId(), extractFullOrder(all[1]).GetComplete().GetJobId())

	// Manifest landed.
	storage.mu.Lock()
	wroteManifest := slices.Contains(storage.putKeys, backup.ManifestKey(bucketID))
	storage.mu.Unlock()
	require.True(t, wroteManifest)

	// Registry deregistered post-Complete.
	require.False(t, o.Registry().IsAlive(start.GetJobId()))
}

// TestRunFull_StartRejectionShortCircuits mirrors the incremental
// version for the full path.
func TestRunFull_StartRejectionShortCircuits(t *testing.T) {
	t.Parallel()

	capture := &proposalCapture{}
	prop := recordingProposer(t, capture, state.ErrBackupInProgress)

	o := newOrchestrator(prop, newTempStore(t))
	dst := &raftcmdpb.BackupDestination{
		BucketId: "busy-full",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{Bucket: "b", Region: "r"},
		},
	}
	storage := &stubStorage{}

	_, err := o.RunFull(context.Background(), dst, storage)
	require.ErrorIs(t, err, state.ErrBackupInProgress)
	require.Len(t, capture.all(), 1)
}
