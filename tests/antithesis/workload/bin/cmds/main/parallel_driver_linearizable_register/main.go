// Driver for the "linearizable reads are linearizable" property, checked by
// search rather than by hand-written bounds.
//
// Default reads go through a Raft ReadIndex plus WaitForApplied on whichever
// node receives them, and claim linearizability. Other drivers check
// consequences of that claim (read-after-write on one client, bounds on a
// counter). This one records a concurrent history and hands it to Porcupine,
// which searches for any order of the operations that respects real time and
// register semantics. If none exists, the claim is false, whatever the
// specific anomaly turns out to be. This is the dragonboat approach (client
// histories checked by Knossos and Porcupine), run under Antithesis faults.
//
// Shape: a few registers, each an account's "v" metadata key in an owned
// ledger. Several workers, each sending every request to one node through a
// single-target connection, so reads are received by every node in turn. Which
// node finally serves a read is not observable here: a follower may forward to
// the leader. Every write stores a globally unique value, so a stale read
// names the exact write it came from.
//
// Soundness against benign interleavings:
//   - Each write carries its own idempotency key, reused by the client's
//     transparent retries. A retry of a write that already committed replays
//     from the idempotency cache instead of committing a second time, which
//     would otherwise make an old value reappear and look like a violation.
//     This holds while the key is retained. The default retention is 24 hours
//     and the Antithesis config does not shorten it; a retention shorter than
//     the driver's 10-minute deadline would let a late retry commit twice and
//     produce a false alarm. Eviction follows the servers' wall clock, so a
//     clock skip of the order of the retention could do the same.
//   - A write that returns any error is indeterminate: it gets an open-ended
//     return time, so the checker may apply it at any point after invocation
//     or never. This only weakens the check; it cannot produce a false alarm.
//     It is deliberately not narrowed for errors that look definitive: the
//     client retries transparently, so the final error says nothing about
//     whether an earlier attempt committed.
//   - A read that returns a transient error has no effect and is dropped.
//   - GetAccount answers with an empty account for an address nothing was
//     written to, and the ledger is created and acknowledged before any worker
//     starts, so a linearizable read answering NotFound is itself a violation.
//   - Time comes from one monotonic clock in this process, so intervals from
//     different workers are comparable.
package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

const (
	metadataKey = "v"
	// perKeyTimeout bounds Porcupine's search on one register. Histories here
	// are at most a few hundred operations; hitting it is reported as an
	// inconclusive check, never as a pass.
	perKeyTimeout = 60 * time.Second
	// workBudget bounds the workers. Operations cut off by it are
	// indeterminate writes or dropped reads, like any other interruption.
	workBudget = 5 * time.Minute
	// reportMargin is kept free at the end of the driver deadline for the
	// assertions that follow the check.
	reportMargin = 15 * time.Second
)

// budget splits what is left of the driver deadline between the workers and
// the check. Setup RPCs share that deadline and may have spent minutes in
// retry backoff, so the workers stop early enough to leave every register its
// full search time plus reportMargin; the check itself is capped by
// checkDeadline. A zero checkDeadline means the context has no deadline.
func budget(ctx context.Context, now time.Time, keys int) (workEnd, checkDeadline time.Time) {
	workEnd = now.Add(workBudget)
	deadline, ok := ctx.Deadline()
	if !ok {
		return workEnd, time.Time{}
	}
	checkDeadline = deadline.Add(-reportMargin)

	return minTime(workEnd, checkDeadline.Add(-time.Duration(keys)*perKeyTimeout)), checkDeadline
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}

	return a
}

// step is one planned operation. Plans are drawn from Hegel up front, on the
// driver goroutine, because the generation source must not be shared with the
// workers.
type step struct {
	key   int
	write bool
}

func address(key int) string {
	return fmt.Sprintf("reg:%d", key)
}

func main() {
	internal.RunGeneratedDriver("parallel_driver_linearizable_register", func(ctx context.Context, client servicepb.BucketServiceClient, _ string, src *generation.Source) {
		run := internal.Rand().Uint64()
		ledger := internal.PrefixLinearizableReg.WithSeed(run)
		if err := internal.CreateLedger(ctx, client, ledger); err != nil {
			return
		}

		all, err := internal.DialPerNode(ctx)
		if err != nil {
			return
		}
		defer all.Close()

		// LEDGER_PER_NODE_GRPC_ADDR lists every slot the scaling drivers may
		// fill (seven), while the cluster usually runs three. Workers on an
		// empty slot would spend the run in retry backoff and fill the history
		// with writes that never reached a node, so only targets resolved
		// from cluster membership at dial time get workers.
		var members internal.PerNodeConns
		for _, c := range all {
			if c.NodeID != 0 {
				members = append(members, c)
			}
		}
		if len(members) == 0 {
			return
		}

		// Menu axes: registers (one hot key versus a few), workers per node,
		// operations per worker. One hot key maximises contention; several
		// keys let writes and reads interleave across registers.
		keys := generation.Choice(src, []int{1, 3})
		workers := len(members) * generation.Choice(src, []int{1, 2})
		opsPerWorker := generation.Choice(src, []int{10, 25})

		plans := make([][]step, workers)
		for w := range plans {
			plans[w] = make([]step, opsPerWorker)
			for i := range plans[w] {
				plans[w][i] = step{key: src.Intn(keys), write: src.Intn(2) == 0}
			}
		}

		details := internal.Details{"ledger": ledger, "run": run, "keys": keys, "workers": workers, "nodes": len(members)}

		start := time.Now()
		workEnd, checkDeadline := budget(ctx, start, keys)
		workCtx, cancel := context.WithDeadline(ctx, workEnd)
		defer cancel()

		clock := func() int64 { return int64(time.Since(start)) }

		var (
			mu      sync.Mutex
			history []porcupine.Operation
			unknown int
			wg      sync.WaitGroup
		)
		for w, plan := range plans {
			conn := members[w%len(members)]
			wg.Go(func() {
				ops, indeterminate := runWorker(workCtx, conn, ledger, run, w, plan, clock, details)
				mu.Lock()
				history = append(history, ops...)
				unknown += indeterminate
				mu.Unlock()
			})
		}
		wg.Wait()

		details = details.With(internal.Details{"operations": len(history), "indeterminateWrites": unknown})

		verdicts := checkHistory(history, perKeyTimeout, checkDeadline)
		// Only Illegal verdicts are reported here. An Always over every
		// verdict would count a timed-out or vacuous search as a passing
		// observation; coverage is owned by the conclusiveness probe below.
		// Illegal keys are reported even when another key timed out, and in
		// one assertion: the SDK emits only the first failure per message in
		// a process, so one call per key would drop every key after the first.
		if illegal := illegalHistories(verdicts); len(illegal) > 0 {
			assert.Unreachable("linearizable register history is not linearizable",
				details.With(internal.Details{"illegal": illegal}))
		}

		assert.Sometimes(conclusive(verdicts, history), "linearizable register check was conclusive", details)
		assert.Sometimes(unknown > 0, "linearizable register history contained an indeterminate write", details)
		assert.Sometimes(readsAcrossNodes(history), "linearizable register read a value written through another node", details)
	})
}

// runWorker executes one worker's plan against its node and returns the
// operations it completed and the number of indeterminate writes among them.
func runWorker(
	ctx context.Context,
	conn *internal.PerNodeConn,
	ledger string,
	run uint64,
	worker int,
	plan []step,
	clock func() int64,
	details internal.Details,
) ([]porcupine.Operation, int) {
	meta := opMeta{Worker: worker, Node: conn.Addr}
	ops := make([]porcupine.Operation, 0, len(plan))
	indeterminateWrites := 0

	for i, s := range plan {
		if ctx.Err() != nil {
			break
		}

		if s.write {
			value := fmt.Sprintf("w%d-%d", worker, i)
			key := fmt.Sprintf("linreg-%d-%d-%d", run, worker, i)

			call := clock()
			_, err := conn.Bucket.Apply(ctx, servicepb.UnsignedApplyRequest(key, saveMetadata(ledger, address(s.key), value)))
			ret := clock()
			if err != nil {
				ret = indeterminate
				indeterminateWrites++
				if unexpected(ctx, err) {
					assert.Unreachable("linearizable register write returned an unexpected error",
						details.With(internal.Details{"node": conn.Addr, "key": s.key, "error": err}))
				}
			}

			ops = append(ops, porcupine.Operation{
				ClientId: worker,
				Input:    regInput{Key: s.key, Write: true, Value: value},
				Call:     call,
				Return:   ret,
				Metadata: meta,
			})

			continue
		}

		call := clock()
		acct, err := conn.Bucket.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledger, Address: address(s.key)})
		ret := clock()
		if err != nil {
			// The controller reports a missing ledger as a bare NotFound
			// with no reason, and an unwritten address is not an error at
			// all, so any NotFound means the read missed the acknowledged
			// ledger creation.
			if internal.IsNotFound(err) {
				assert.Unreachable("linearizable register read missed an acknowledged ledger",
					details.With(internal.Details{"node": conn.Addr, "error": err}))
			} else if unexpected(ctx, err) {
				assert.Unreachable("linearizable register read returned an unexpected error",
					details.With(internal.Details{"node": conn.Addr, "key": s.key, "error": err}))
			}

			continue
		}
		// A response for another address would be read as "absent" and
		// constrain the wrong register.
		if acct.GetAddress() != address(s.key) {
			assert.Unreachable("linearizable register read answered for another account",
				details.With(internal.Details{"node": conn.Addr, "asked": address(s.key), "got": acct.GetAddress()}))

			continue
		}

		value, ok := registerValue(acct.GetMetadata()[metadataKey])
		if !ok {
			// Only non-empty strings are ever written. Anything else would
			// otherwise read as absent and pass as the initial state.
			assert.Unreachable("linearizable register read a value that was never written",
				details.With(internal.Details{"node": conn.Addr, "key": s.key, "value": acct.GetMetadata()[metadataKey].String()}))

			continue
		}

		ops = append(ops, porcupine.Operation{
			ClientId: worker,
			Input:    regInput{Key: s.key},
			Call:     call,
			Output:   value,
			Return:   ret,
			Metadata: meta,
		})
	}

	return ops, indeterminateWrites
}

// unexpected reports whether an error falls outside the tolerated set
// (transient, the driver's own cancellation or deadline). DialPerNode
// connections do not carry the classification interceptor, and the operation
// is still excluded from the history, so without a check a node failing every
// request with, say, Internal would vanish while other nodes supply a clean
// history. Following internal.IsCanceled, a Canceled status is the driver's
// own shutdown only once its context is done. Callers assert with literal
// messages so the instrumentor's catalog matches what is emitted.
func unexpected(ctx context.Context, err error) bool {
	return !internal.IsTolerated(err) || (internal.IsCanceled(err) && ctx.Err() == nil)
}

func saveMetadata(ledger, addr, value string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: addr}},
						},
						Metadata: commonpb.MetadataFromGoMap(map[string]string{metadataKey: value}),
					},
				}},
			},
		},
	}
}
