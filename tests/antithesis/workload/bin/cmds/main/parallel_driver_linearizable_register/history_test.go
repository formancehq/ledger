package main

import (
	"context"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func write(worker, key int, value string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: worker,
		Input:    regInput{Key: key, Write: true, Value: value},
		Call:     call,
		Return:   ret,
		Metadata: opMeta{Worker: worker, Node: nodeOf(worker)},
	}
}

func read(worker, key int, value string, call, ret int64) porcupine.Operation {
	return porcupine.Operation{
		ClientId: worker,
		Input:    regInput{Key: key},
		Call:     call,
		Output:   value,
		Return:   ret,
		Metadata: opMeta{Worker: worker, Node: nodeOf(worker)},
	}
}

func nodeOf(worker int) string {
	return []string{"ledger-0", "ledger-1"}[worker%2]
}

func results(ops ...porcupine.Operation) map[int]porcupine.CheckResult {
	out := make(map[int]porcupine.CheckResult)
	for _, v := range checkHistory(ops, time.Second, time.Time{}) {
		out[v.Key] = v.Result
	}

	return out
}

func TestCheckHistory(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		ops  []porcupine.Operation
		want map[int]porcupine.CheckResult
	}{
		{
			name: "read after write sees it",
			ops:  []porcupine.Operation{write(0, 0, "a", 0, 10), read(1, 0, "a", 20, 30)},
			want: map[int]porcupine.CheckResult{0: porcupine.Ok},
		},
		{
			name: "read before any write sees absent",
			ops:  []porcupine.Operation{read(1, 0, absent, 0, 5), write(0, 0, "a", 10, 20)},
			want: map[int]porcupine.CheckResult{0: porcupine.Ok},
		},
		{
			name: "stale read after a newer write completed",
			ops: []porcupine.Operation{
				write(0, 0, "a", 0, 10),
				write(0, 0, "b", 20, 30),
				read(1, 0, "a", 40, 50),
			},
			want: map[int]porcupine.CheckResult{0: porcupine.Illegal},
		},
		{
			name: "read misses a completed write",
			ops:  []porcupine.Operation{write(0, 0, "a", 0, 10), read(1, 0, absent, 20, 30)},
			want: map[int]porcupine.CheckResult{0: porcupine.Illegal},
		},
		{
			name: "read sees a value before its write began",
			ops:  []porcupine.Operation{read(1, 0, "a", 0, 10), write(0, 0, "a", 20, 30)},
			want: map[int]porcupine.CheckResult{0: porcupine.Illegal},
		},
		{
			name: "read concurrent with a write may see either value",
			ops: []porcupine.Operation{
				write(0, 0, "a", 0, 100),
				read(1, 0, absent, 10, 20),
				read(1, 0, "a", 30, 40),
			},
			want: map[int]porcupine.CheckResult{0: porcupine.Ok},
		},
		{
			name: "value flips back after being observed",
			ops: []porcupine.Operation{
				write(0, 0, "a", 0, 100),
				read(1, 0, "a", 10, 20),
				read(1, 0, absent, 30, 40),
			},
			want: map[int]porcupine.CheckResult{0: porcupine.Illegal},
		},
		{
			name: "indeterminate write observed later",
			ops:  []porcupine.Operation{write(0, 0, "a", 0, indeterminate), read(1, 0, "a", 500, 510)},
			want: map[int]porcupine.CheckResult{0: porcupine.Ok},
		},
		{
			name: "indeterminate write never observed",
			ops:  []porcupine.Operation{write(0, 0, "a", 0, indeterminate), read(1, 0, absent, 500, 510)},
			want: map[int]porcupine.CheckResult{0: porcupine.Ok},
		},
		{
			name: "indeterminate write cannot be observed before it began",
			ops:  []porcupine.Operation{read(1, 0, "a", 0, 10), write(0, 0, "a", 20, indeterminate)},
			want: map[int]porcupine.CheckResult{0: porcupine.Illegal},
		},
		{
			name: "registers are checked independently",
			ops: []porcupine.Operation{
				write(0, 0, "a", 0, 10),
				read(1, 1, absent, 20, 30),
				write(0, 1, "b", 40, 50),
				write(0, 1, "c", 60, 70),
				read(1, 1, "b", 80, 90),
			},
			want: map[int]porcupine.CheckResult{0: porcupine.Ok, 1: porcupine.Illegal},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, results(tc.ops...))
		})
	}
}

func TestReadsAcrossNodes(t *testing.T) {
	t.Parallel()

	require.False(t, readsAcrossNodes([]porcupine.Operation{
		write(0, 0, "a", 0, 10), read(0, 0, "a", 20, 30),
	}), "a read on the writer's own node compares nothing")

	require.True(t, readsAcrossNodes([]porcupine.Operation{
		write(0, 0, "a", 0, 10), read(1, 0, "a", 20, 30),
	}))

	require.False(t, readsAcrossNodes([]porcupine.Operation{
		write(0, 0, "a", 0, 10), read(1, 1, "a", 20, 30),
	}), "a read returning another register's value is a violation, not coverage")

	require.False(t, readsAcrossNodes([]porcupine.Operation{
		read(1, 0, "a", 0, 10), write(0, 0, "a", 20, 30),
	}), "a read that returned before its write began is a violation, not coverage")

	require.True(t, readsAcrossNodes([]porcupine.Operation{
		write(0, 0, "a", 5, 30), read(1, 0, "a", 0, 10),
	}), "a read overlapping its write still counts")
}

func TestPruneUnobservedKeepsVerdictTractable(t *testing.T) {
	t.Parallel()

	// Many writes that never committed, then a read of the initial value.
	// Without pruning, the search must consider every subset of the pending
	// writes; Codex measured this timing out under a one-second budget.
	var ops []porcupine.Operation
	for i := range 24 {
		ops = append(ops, write(i, 0, string(rune('a'+i)), int64(i), indeterminate))
	}
	ops = append(ops, read(99, 0, absent, 1000, 1010))
	require.Equal(t, map[int]porcupine.CheckResult{0: porcupine.Ok}, results(ops...))

	// A wrong read is still caught among unobserved pending writes.
	ops[len(ops)-1] = read(99, 0, "never-written", 1000, 1010)
	require.Equal(t, map[int]porcupine.CheckResult{0: porcupine.Illegal}, results(ops...))

	// An observed pending write is kept and still explains its read.
	ops[len(ops)-1] = read(99, 0, "c", 1000, 1010)
	require.Equal(t, map[int]porcupine.CheckResult{0: porcupine.Ok}, results(ops...))

	// Pruning never touches completed writes.
	require.Len(t, pruneUnobserved([]porcupine.Operation{write(0, 0, "a", 0, 10)}), 1)
}

func TestCheckHistoryPastDeadlineIsUnknown(t *testing.T) {
	t.Parallel()

	// An illegal history that would be caught with time left must come back
	// Unknown, not searched without a bound and not reported as Ok.
	ops := []porcupine.Operation{write(0, 0, "a", 0, 10), read(1, 0, absent, 20, 30)}
	verdicts := checkHistory(ops, time.Second, time.Now().Add(-time.Second))
	require.Len(t, verdicts, 1)
	require.Equal(t, porcupine.Unknown, verdicts[0].Result)

	verdicts = checkHistory(ops, time.Second, time.Now().Add(time.Minute))
	require.Equal(t, porcupine.Illegal, verdicts[0].Result)
}

func TestBudgetReservesCheckTime(t *testing.T) {
	t.Parallel()

	now := time.Now()

	// No deadline: the full work budget, an unbounded check.
	workEnd, checkDeadline := budget(t.Context(), now, 3)
	require.Equal(t, now.Add(workBudget), workEnd)
	require.True(t, checkDeadline.IsZero())

	// Plenty of time left: the work budget applies unchanged.
	ctx, cancel := context.WithDeadline(t.Context(), now.Add(time.Hour))
	defer cancel()
	workEnd, checkDeadline = budget(ctx, now, 3)
	require.Equal(t, now.Add(workBudget), workEnd)
	require.Equal(t, now.Add(time.Hour-reportMargin), checkDeadline)

	// Setup spent most of the driver deadline: the workers give way so
	// every register keeps its full search time before the deadline.
	ctx, cancel = context.WithDeadline(t.Context(), now.Add(6*time.Minute))
	defer cancel()
	workEnd, checkDeadline = budget(ctx, now, 3)
	require.Equal(t, now.Add(6*time.Minute-reportMargin), checkDeadline)
	require.Equal(t, checkDeadline.Add(-3*perKeyTimeout), workEnd)
}

func TestIllegalHistoriesReportsEveryIllegalKey(t *testing.T) {
	t.Parallel()

	verdicts := checkHistory([]porcupine.Operation{
		write(0, 0, "a", 0, 10), read(1, 0, absent, 20, 30), // key 0: lost write
		write(0, 1, "b", 0, 10), read(1, 1, "b", 20, 30), // key 1: fine
		read(1, 2, "c", 0, 10), // key 2: value never written
	}, time.Second, time.Time{})

	got := illegalHistories(verdicts)
	require.Len(t, got, 2)
	require.Contains(t, got, "key 0")
	require.Contains(t, got, "key 2")
	require.Empty(t, illegalHistories(checkHistory([]porcupine.Operation{write(0, 0, "a", 0, 10)}, time.Second, time.Time{})))
}

func TestConclusive(t *testing.T) {
	t.Parallel()

	verdictsOf := func(ops ...porcupine.Operation) ([]keyVerdict, []porcupine.Operation) {
		return checkHistory(ops, time.Second, time.Time{}), ops
	}

	require.True(t, conclusive(verdictsOf(write(0, 0, "a", 0, 10), read(1, 0, "a", 20, 30))))
	require.False(t, conclusive(verdictsOf()), "an empty history checks nothing")
	require.False(t, conclusive(verdictsOf(write(0, 0, "a", 0, indeterminate), write(1, 0, "b", 0, indeterminate))),
		"writes alone constrain nothing")
	require.False(t, conclusive(verdictsOf(write(0, 0, "a", 0, indeterminate), read(1, 0, absent, 5, 10))),
		"reads of the initial state alone constrain no write")
	require.False(t, conclusive(verdictsOf(write(0, 0, "a", 0, 10), read(1, 0, absent, 20, 30))),
		"an Illegal verdict is not conclusive coverage")
	require.False(t, conclusive([]keyVerdict{{Key: 0, Result: porcupine.Unknown}}, []porcupine.Operation{read(1, 0, "a", 20, 30)}),
		"a timed-out search is not conclusive")
}

func TestRegisterValue(t *testing.T) {
	t.Parallel()

	v, ok := registerValue(nil)
	require.True(t, ok)
	require.Equal(t, absent, v)

	v, ok = registerValue(&commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "w0-1"}})
	require.True(t, ok)
	require.Equal(t, "w0-1", v)

	_, ok = registerValue(&commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: ""}})
	require.False(t, ok, "an empty string is never written and must not read as absent")

	_, ok = registerValue(&commonpb.MetadataValue{Type: &commonpb.MetadataValue_IntValue{IntValue: 0}})
	require.False(t, ok, "a non-string value must not read as absent")

	_, ok = registerValue(&commonpb.MetadataValue{})
	require.False(t, ok, "a value with no variant must not read as absent")
}

func TestRenderOpsMarksIndeterminateWrites(t *testing.T) {
	t.Parallel()

	lines := renderOps([]porcupine.Operation{
		read(1, 0, "a", 20, 30),
		write(0, 0, "a", 0, indeterminate),
	})
	require.Equal(t, []string{
		`[0, ?] worker 0 @ ledger-0: write(0, "a")`,
		`[20, 30] worker 1 @ ledger-1: read(0) -> "a"`,
	}, lines)
}
