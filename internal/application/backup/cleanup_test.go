package backup

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// runningJob returns a RUNNING BackupJob the cleanup loop will inspect.
func runningJob(jobID uint64, kind raftcmdpb.BackupKind) *raftcmdpb.BackupJob {
	return &raftcmdpb.BackupJob{
		JobId:  jobID,
		Status: raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_RUNNING,
		Kind:   kind,
	}
}

// expectForEachActive wires the ForEachActive mock to deliver the given
// jobs to the callback. The cleanup loop iterates whatever the FSM map
// holds; the test pre-seeds it through the mock.
func expectForEachActive(provider *MockStaleProvider, jobs ...*raftcmdpb.BackupJob) {
	provider.EXPECT().ForEachActive(gomock.Any()).Do(func(fn func(*raftcmdpb.BackupJob)) {
		for _, j := range jobs {
			fn(j)
		}
	})
}

// resolvingProposer returns a Proposer mock that returns the given
// applyErr (nil = happy path) on every Propose call. Mirrors the
// contract the bootstrap adapter presents to the cleanup loop.
func resolvingProposer(t *testing.T, applyErr error) *MockProposer {
	t.Helper()
	ctrl := gomock.NewController(t)

	prop := NewMockProposer(ctrl)
	prop.EXPECT().Propose(gomock.Any(), gomock.Any()).AnyTimes().Return(applyErr)

	return prop
}

// liveProbe returns a LiveJobsProbe mock that reports the supplied set
// of job IDs as alive. Anything else is treated as orphaned.
func liveProbe(t *testing.T, alive ...uint64) *MockLiveJobsProbe {
	t.Helper()
	ctrl := gomock.NewController(t)
	set := map[uint64]struct{}{}
	for _, j := range alive {
		set[j] = struct{}{}
	}

	probe := NewMockLiveJobsProbe(ctrl)
	probe.EXPECT().IsAlive(gomock.Any()).AnyTimes().DoAndReturn(func(jobID uint64) bool {
		_, ok := set[jobID]

		return ok
	})

	return probe
}

func TestCleanup_SkipsWhenNotLeader(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	provider := NewMockStaleProvider(ctrl)
	probe := NewMockLeaderProbe(ctrl)
	probe.EXPECT().IsLeader().Return(false)
	prop := NewMockProposer(ctrl) // zero Propose calls expected

	c := NewCleanup(provider, prop, probe, liveProbe(t), logging.Testing())
	c.tick(context.Background())
}

// TestCleanup_SkipsLiveJob asserts the headline guarantee: a RUNNING
// job whose driver goroutine is alive on this leader is NEVER failed
// out by the cleanup loop, no matter how long it has been uploading.
// The wall-clock / index-gap signal is gone; only the in-memory
// registry decides.
func TestCleanup_SkipsLiveJob(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	provider := NewMockStaleProvider(ctrl)
	expectForEachActive(provider, runningJob(42, raftcmdpb.BackupKind_BACKUP_KIND_FULL))

	probe := NewMockLeaderProbe(ctrl)
	probe.EXPECT().IsLeader().Return(true)

	prop := NewMockProposer(ctrl) // zero Propose expected: job is alive

	c := NewCleanup(provider, prop, probe, liveProbe(t, 42), logging.Testing())
	c.tick(context.Background())
}

// TestCleanup_FailsOrphanJob asserts the inverse: a RUNNING entry
// with no live executor on this leader is orphaned and gets a Fail
// proposal. This is the case that fires after process restart or
// leadership transfer: the new leader's BackupJobsState comes back
// with the RUNNING entry from Pebble, but the executor goroutine is
// not in this process.
func TestCleanup_FailsOrphanJob(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	provider := NewMockStaleProvider(ctrl)
	expectForEachActive(provider, runningJob(42, raftcmdpb.BackupKind_BACKUP_KIND_FULL))

	probe := NewMockLeaderProbe(ctrl)
	probe.EXPECT().IsLeader().Return(true)

	prop := resolvingProposer(t, nil)

	c := NewCleanup(provider, prop, probe, liveProbe(t /* nobody alive */), logging.Testing())
	c.tick(context.Background())
}

// TestCleanup_IgnoresAlreadyTerminalJob: the cleanup loop only looks at
// RUNNING entries. A COMPLETE / FAILED entry in the map (it would be
// there briefly between Complete-apply and the in-memory delete) must
// be left alone.
func TestCleanup_IgnoresAlreadyTerminalJob(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	completed := runningJob(42, raftcmdpb.BackupKind_BACKUP_KIND_FULL)
	completed.Status = raftcmdpb.BackupJobStatus_BACKUP_JOB_STATUS_COMPLETE

	provider := NewMockStaleProvider(ctrl)
	expectForEachActive(provider, completed)

	probe := NewMockLeaderProbe(ctrl)
	probe.EXPECT().IsLeader().Return(true)

	prop := NewMockProposer(ctrl) // zero Propose expected

	c := NewCleanup(provider, prop, probe, liveProbe(t), logging.Testing())
	c.tick(context.Background())
}

var errSimulated = errors.New("simulated")

// TestCleanup_PropagatesProposeError: the error path logs the failure
// but does not panic. Next tick retries.
func TestCleanup_PropagatesProposeError(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	provider := NewMockStaleProvider(ctrl)
	expectForEachActive(provider, runningJob(7, raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL))

	probe := NewMockLeaderProbe(ctrl)
	probe.EXPECT().IsLeader().Return(true)

	prop := NewMockProposer(ctrl)
	prop.EXPECT().Propose(gomock.Any(), gomock.Any()).Return(errSimulated)

	c := NewCleanup(provider, prop, probe, liveProbe(t), logging.Testing())
	c.tick(context.Background())
}

// TestCleanup_FailOrphan_RejectsUnknownKind guards the default arm of
// the kind switch — a future BackupKind that the loop does not know
// how to dispatch must surface as an explicit error rather than
// silently propose nothing.
func TestCleanup_FailOrphan_RejectsUnknownKind(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	c := NewCleanup(NewMockStaleProvider(ctrl), NewMockProposer(ctrl), NewMockLeaderProbe(ctrl), liveProbe(t), logging.Testing())
	const unknownKind = raftcmdpb.BackupKind(99)

	err := c.failOrphan(context.Background(), 1, unknownKind)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown backup kind")
}

// TestCleanup_RunFillsDefaultInterval guards the zero-value-friendly
// constructor contract: NewCleanup + Run(ctx) is safe with a zero
// Interval, Run sets the default.
func TestCleanup_RunFillsDefaultInterval(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	c := NewCleanup(NewMockStaleProvider(ctrl), NewMockProposer(ctrl), NewMockLeaderProbe(ctrl), liveProbe(t), logging.Testing())
	c.Interval = 0

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Run(ctx)

	require.Equal(t, DefaultCleanupInterval, c.Interval)
}

func TestCleanup_RunTerminatesOnContextCancel(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)

	c := NewCleanup(NewMockStaleProvider(ctrl), NewMockProposer(ctrl), NewMockLeaderProbe(ctrl), liveProbe(t), logging.Testing())
	c.Interval = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}
