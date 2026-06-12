package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// TestApplierPipelinedCheckpointDoesNotRace is the node-level companion to
// state.TestPipelinedApplyWithCheckpointDoesNotDiverge. It drives the actual
// applier hot path (applier.Run + Submit + applyEntriesPipelined + runCommitter)
// so the pipelined commit path is exercised end to end. Under -race, this
// catches any regression that reintroduces a synchronous Pebble write from
// inside PrepareEntries while a previous batch's commit is still in flight on
// runCommitter.
//
// Scenario: many cycles of "normal batch → batch ending in CreateQueryCheckpoint",
// submitted via two successive Submit calls so the applier sees them as two
// distinct work items. The second batch's PrepareEntries runs concurrently with
// the first batch's still-in-flight commit. Each cycle exercises the drain
// after the checkpoint (waitPendingCommit) plus the maintenance window before
// returning the applier to statusNormal.
func TestApplierPipelinedCheckpointDoesNotRace(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	const cycles = 5

	var (
		idx  uint64
		want []string
	)

	for c := range cycles {
		// First batch: two plain CreateLedger entries. Triggers async commit.
		idx++
		a1, _ := makeCreateLedgerEntry(t, idx, fmt.Sprintf("pipelined-%d-a1", c))
		idx++
		a2, _ := makeCreateLedgerEntry(t, idx, fmt.Sprintf("pipelined-%d-a2", c))
		setup.applier.Submit([]raftpb.Entry{a1, a2}, setup.confState, setup.stop)

		// Second batch: another CreateLedger followed by CreateQueryCheckpoint
		// as the LAST entry. PrepareEntries for this batch runs concurrently
		// with the first batch's commit on runCommitter.
		idx++
		b1, _ := makeCreateLedgerEntry(t, idx, fmt.Sprintf("pipelined-%d-b1", c))
		idx++
		chkpt, _ := makeCreateQueryCheckpointEntry(t, idx)
		setup.applier.Submit([]raftpb.Entry{b1, chkpt}, setup.confState, setup.stop)

		want = append(want,
			fmt.Sprintf("pipelined-%d-a1", c),
			fmt.Sprintf("pipelined-%d-a2", c),
			fmt.Sprintf("pipelined-%d-b1", c),
		)
	}

	require.Eventually(t, func() bool {
		for _, name := range want {
			if !listLedgerContains(setup.store, name) {
				return false
			}
		}

		return true
	}, 30*time.Second, 100*time.Millisecond, "all ledgers must exist after pipelined checkpoint cycles")

	require.Eventually(t, func() bool {
		return setup.applier.Status() == statusNormal
	}, 10*time.Second, 50*time.Millisecond, "applier must return to statusNormal after the last checkpoint")

	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}
