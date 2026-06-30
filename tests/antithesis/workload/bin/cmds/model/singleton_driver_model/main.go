// Command singleton_driver_model stress-tests the chart-of-accounts feature
// against a model that predicts the set of legal server responses. N workers fan
// out across a fleet of ledgers, dispatching bulks concurrently; multiple bulks
// may be in flight at once. The model mirrors the single Raft log: one global
// re-order buffer, one committed state spanning all ledgers.
//
// Test template: this command runs in its OWN Antithesis test template (model),
// separate from the rest of the suite (main). Antithesis selects exactly one
// template per execution history, so it never runs alongside the other drivers —
// it drives the system itself and must own the whole timeline (no concurrent
// driver, no eventually_* command pre-empting it). Each bin/cmds/<template>/
// directory becomes a test template, so keeping this command under
// bin/cmds/model/ (not bin/cmds/main/) is what keeps it isolated.
//
// Layout:
//
//   - the model itself is the oracle package (tests/oracle): LedgerState
//     (per-ledger sub-state) + GlobalState + the pure forward Apply that predicts
//     the server's outcome for a bulk. A bulk may span ledgers; Apply is atomic
//     across them. This driver imports it as `oracle`.
//   - checker.go: Checker — the harness bookkeeping (in-flight/pending re-order
//     buffer, modelState).
//   - processor.go: one goroutine; re-orders observed responses by log sequence
//     and drains them in order under the read/in-flight gate.
//   - search.go: candidateBases folds the in-flight bulks onto modelState to
//     enumerate the states the server could be in.
//   - validate.go: all model-conformance checks — committed bulks, failures, and
//     reads — over the candidate states.
//   - actions.go: random bulk generation.
//   - reads.go: GetAccount + chart read execution.
//   - main.go: workers + entry point.
//
// Invariant: every observed response is consistent with some serialization of
// the in-flight bulks (see candidateBases).
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: singleton_driver_model")

	// The model treats every IsTransient gRPC error as "the bulk didn't happen"
	// (processor.go handleObservation). That only holds if outcomes are
	// definitive, so force retry-forever: the retry interceptors retry the whole
	// IsTransient set until it clears, so a transient the processor still sees is
	// one retry could not resolve (ctx/shutdown), correctly "didn't happen". An
	// ambiguous-commit retry that lands after recovery hits the idempotency cache
	// and returns the committed log reference. Business outcomes (NotFound,
	// LedgerDeleted) are not in IsTransient — they are validated, not dropped.
	if os.Getenv("LEDGER_NO_RETRY") == "" {
		_ = os.Setenv("LEDGER_RETRY_FOREVER", "1")
	}

	ctx := context.Background()

	// Self-terminate after MODEL_MAX_SECONDS so an orphaned driver from
	// a killed shell can't keep hammering a shared ledger into the next
	// run.
	if secs := os.Getenv("MODEL_MAX_SECONDS"); secs != "" {
		if d, err := strconv.Atoi(secs); err == nil && d > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(d)*time.Second)
			defer cancel()
		}
	}

	numLedgers := envInt("MODEL_LEDGERS", defaultLedgers)
	numWorkers := envInt("MODEL_WORKERS", defaultWorkers)

	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	// Unique per-run prefix so a fresh invocation never reattaches to a
	// previous run's ledgers (the model starts empty; inherited committed
	// state would diverge). The Antithesis source varies per invocation and
	// stays replayable.
	runID := fmt.Sprintf("%016x", internal.Rand().Uint64())
	names := ledgerNames(runID, numLedgers)

	if !setupLedgers(ctx, client, names) {
		return
	}

	checker := NewChecker(names)

	// No seed type — workers fill the chart organically; early txs at
	// untyped prefixes fail ACCOUNT_NOT_MATCHING_TYPE and validate fine.

	log.Printf("starting %d workers across %d ledgers", numWorkers, numLedgers)

	var processors sync.WaitGroup
	processors.Add(1)
	go func() {
		defer processors.Done()
		checker.runProcessor(ctx)
	}()

	var workers sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			runWorker(ctx, client, checker)
		}()
	}

	// Workers stop on ctx.Done. Close the processor's channel so it can
	// drain and exit.
	workers.Wait()
	close(checker.incoming)
	processors.Wait()
}

// Holds c.mu only to generate + register the bulk, releases before the Apply
// round-trip, then pushes the observation to the processor.
func runWorker(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	c *Checker,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// 1-in-5: a read this iteration — 1-in-3 of those a whole-ledger read
		// (chart + ledger metadata), the rest a single-account read. Reads
		// validate against the in-flight bulk set, exercising cross-node freshness
		// without needing quiescence.
		if random.RandomChoice([]uint8{0, 1, 2, 3, 4}) == 0 {
			if random.RandomChoice([]uint8{0, 1, 2}) == 0 {
				runLedgerRead(ctx, client, c)
			} else {
				runRead(ctx, client, c)
			}
			time.Sleep(workerLoopPause)
			continue
		}

		c.mu.Lock()
		bulk := generateBulk(c.modelState, c.ledgerNames)
		if len(bulk.Requests) == 0 {
			c.mu.Unlock()
			continue
		}
		ticket := c.registerInflight(bulk)
		c.mu.Unlock()

		resp, err := client.Apply(ctx, applyRequest(bulk))

		// Snapshot the ticket high-water at observe (lock-free, atomic counter);
		// the drain gate compares outstanding tickets against it (see tryDrain).
		// This is a loose upper bound: a sibling worker can register its own bulk
		// between Apply returning and this Load, so observeTicket may cover a
		// ticket whose effect could not precede this response. That only enlarges
		// the candidate-base set validation considers (it gets more permissive),
		// never shrinks it — it can mask a divergence but never manufacture a
		// false failure. The window is irreducible: the counter can always climb
		// between the RPC returning and the atomic read, so we accept it.
		obs := observation{
			ticket:        ticket,
			bulk:          bulk,
			resp:          resp,
			err:           err,
			observeTicket: c.ticketSeq.Load(),
		}

		// Block on a full channel — natural back-pressure.
		select {
		case <-ctx.Done():
			return
		case c.incoming <- obs:
		}

		time.Sleep(workerLoopPause)
	}
}

// Per-run fleet names: model-<runID>-0, model-<runID>-1, ... PrefixModel
// is in internal.ownedLedgerPrefixes so generic drivers skip them.
func ledgerNames(runID string, n int) []string {
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = internal.PrefixModel.WithSuffix(fmt.Sprintf("%s-%d", runID, i))
	}
	return out
}

// envInt reads an int from env, defaulting on missing or invalid.
func envInt(key string, def int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}

	v, err := strconv.Atoi(raw)
	if err != nil || v < 1 {
		log.Printf("warning: invalid %s=%q, using default %d", key, raw, def)
		return def
	}

	return v
}

// Creates each ledger. CreateLedger carries an idempotency key, so a create whose
// commit response was lost replays to the committed success instead of
// AlreadyExists; with the unique per-run names, AlreadyExists cannot occur here.
// Any error is therefore a genuine setup failure: the model can't run against a
// missing ledger, so it asserts Unreachable. Shutdown (ctx cancelled) is teardown,
// not a finding. Returns false to stop the run; the chart is left empty for
// workers to fill.
func setupLedgers(ctx context.Context, client servicepb.BucketServiceClient, names []string) bool {
	for _, name := range names {
		err := internal.CreateLedger(ctx, client, name)
		if err == nil {
			continue
		}
		if isShutdownError(err) {
			return false
		}

		assert.Unreachable("singleton_driver_model: ledger setup failed", internal.Details{
			"ledger": name,
			"error":  err.Error(),
		})

		return false
	}

	return true
}
