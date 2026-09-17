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

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"

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
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := client.Apply(cleanupCtx, servicepb.UnsignedApplyRequest(idempotencyKey(), actions.SetMaintenanceModeAction(false))); err != nil {
			log.Printf("disable maintenance during shutdown: %v", err)
		}
	}()

	// A previous driver may have died after enabling the cluster-wide gate.
	// Recover before setup so CreateLedger cannot wait behind maintenance forever.
	if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(idempotencyKey(), actions.SetMaintenanceModeAction(false))); err != nil {
		log.Printf("disable maintenance during startup: %v", err)
		return
	}

	// Unique per-run prefix so a fresh invocation never reattaches to a
	// previous run's ledgers (the model starts empty; inherited committed
	// state would diverge). The Antithesis source varies per invocation and
	// stays replayable.
	runID := fmt.Sprintf("%016x", internal.Rand().Uint64())
	names := ledgerNames(runID, numLedgers)

	// Each ledger is created with a small, random initial metadata schema; the
	// checker seeds the same declarations so the model's schema state matches
	// the server's from the first bulk.
	schemas := make(map[string][]*commonpb.SetMetadataFieldTypeCommand, len(names))
	for _, name := range names {
		schemas[name] = initialSchema()
	}

	if !setupLedgers(ctx, client, names, schemas) {
		return
	}

	checker := NewChecker(names, schemas)
	checker.modelState = checker.modelState.WithQueryCheckpointLimit(uint64(envInt("MODEL_QUERY_CHECKPOINT_LIMIT", defaultModelCheckpointLimit)))
	dialCtx, cancelDial := context.WithTimeout(ctx, 5*time.Second)
	checkpointNodes, _ := internal.DialPerNode(dialCtx)
	cancelDial()
	if len(checkpointNodes) == 0 {
		assert.Unreachable("singleton_driver_model: checkpoint node connections unavailable", nil)
		return
	}
	defer checkpointNodes.Close()
	checkpointSetupNode, err := waitForCheckpointSetupNode(ctx, checkpointNodes, names[0])
	if err != nil {
		if ctx.Err() == nil {
			assert.Unreachable("singleton_driver_model: no checkpoint setup node available", internal.Details{"error": err.Error()})
		}
		return
	}
	if !setupQueryCheckpoints(ctx, checkpointSetupNode.Bucket, checkpointSetupNode.Cluster, checker) {
		return
	}

	// Declared before the first query so an index the run never exercises shows
	// up as an unsatisfied property rather than as no output at all.
	registerCoverage()

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
			runWorker(ctx, client, checkpointNodes, checker)
		}()
	}

	var restore sync.WaitGroup
	if trigger := selectRestoreTrigger(); trigger != nil {
		restore.Add(1)
		go func() {
			defer restore.Done()
			runRestoreCycle(ctx, checker, trigger, restoreInterval())
		}()
		log.Printf("restore cycle enabled (interval ~%s)", restoreInterval())
	}

	// Index readiness poller: reconciles each created index's active flag against
	// per-replica CurrentVersion, so has-asset queries validate results once the
	// index is live everywhere. Per-node conns are lazy, so a down node is handled
	// by the poller's transient-error path.
	var pollers sync.WaitGroup
	pollers.Add(1)
	go func() {
		defer pollers.Done()
		runIndexReadinessPoller(ctx, checker, checkpointNodes, indexPollInterval)
	}()

	// Workers stop on ctx.Done. Wait for the restore cycle and poller too before
	// closing the processor's channel, so nothing touches the checker during
	// teardown.
	workers.Wait()
	restore.Wait()
	pollers.Wait()
	checker.recoveries.Wait()
	close(checker.incoming)
	processors.Wait()
}

// Holds c.mu only to snapshot the state and register the bulk; generation and
// the Apply round-trip run lock-free, then the observation goes to the processor.
func runWorker(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	checkpointNodes internal.PerNodeConns,
	c *Checker,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Park here while a restore cycle has quiesced the driver.
		if !c.awaitResume(ctx) {
			return
		}

		// 1-in-5: a read this iteration, split across the whole-ledger read
		// (chart + ledger metadata), a single-account read, a transaction read
		// (id + postings + reverted + metadata), a metadata-schema read (declared
		// field types), and the two list queries (filtered, paginated, ordered
		// windows over accounts and transactions). Reads validate against the
		// in-flight bulk set, exercising cross-node freshness without needing
		// quiescence. Account and transaction queries receive extra slots because
		// together they must exercise every builtin and declared metadata index.
		if random.RandomChoice([]uint8{0, 1, 2, 3, 4}) == 0 {
			switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}) {
			case 0:
				runLedgerRead(ctx, client, c)
			case 1:
				runTransactionRead(ctx, client, c)
			case 2:
				runSchemaRead(ctx, client, c)
			case 3, 4:
				runAccountQuery(ctx, client, c)
			case 5, 6, 7:
				runTransactionQuery(ctx, client, c)
			case 8:
				runReplay(ctx, client, c)
			case 9:
				runLogQuery(ctx, client, c)
			case 10:
				node := random.RandomChoice(checkpointNodes)
				runCheckpointRead(ctx, node.Bucket, node.Cluster, c)
			default:
				runRead(ctx, client, c)
			}
			continue
		}

		// Snapshot the committed state (O(1): persistent collections) and
		// generate outside the lock — a published GlobalState is never mutated,
		// the processor only rebinds c.modelState to a fork.
		c.mu.Lock()
		if c.paused {
			c.mu.Unlock()
			continue
		}
		state := c.modelState
		ledgers := c.ledgerNamesSnapshot()
		c.mu.Unlock()

		var bulk oracle.Bulk
		if percentChance(10) {
			bulk = generateCheckpointBulk(state)
		} else {
			bulk = generateBulk(state, ledgers, c.nextLedgerName(), c.liveTarget)
		}
		if len(bulk.Requests) == 0 {
			continue
		}
		dispatchBulk(ctx, client, checkpointNodes, c, bulk)
	}
}

// dispatchBulk sends every generated request through the same inflight and
// processor path. Maintenance enable schedules a modeled disable independently,
// so a write-blocked worker fleet cannot stall the run permanently.
func dispatchBulk(ctx context.Context, client servicepb.BucketServiceClient, checkpointNodes internal.PerNodeConns, c *Checker, bulk oracle.Bulk) {
	checkpointCreate := isCheckpointCreate(bulk)
	if checkpointCreate {
		c.checkpointCreateMu.Lock()
		defer c.checkpointCreateMu.Unlock()
	}

	c.mu.Lock()
	if c.paused {
		c.mu.Unlock()
		return
	}
	c.stampIdempotency(&bulk)
	var predictedCheckpointID uint64
	if checkpointCreate {
		predictedCheckpointID = c.modelState.NextQueryCheckpointID()
	}
	ticket := c.registerInflight(bulk)
	c.mu.Unlock()

	var probeDone <-chan struct{}
	if checkpointCreate {
		start := make(chan struct{})
		registered := make(chan struct{})
		done := make(chan struct{})
		probeDone = done
		node := random.RandomChoice(checkpointNodes)
		go func() {
			defer close(done)
			runPredictedCheckpointRead(ctx, node.Bucket, c, predictedCheckpointID, start, registered)
		}()
		<-registered
		close(start)
	}

	req := applyRequest(bulk)
	var resp *servicepb.ApplyResponse
	var err error
	hadAmbiguousAttempt := false
	maintenanceRecoveryScheduled := false
	for {
		resp, err = client.Apply(ctx, req)
		if err == nil || ctx.Err() != nil {
			break
		}
		if internal.IsMaintenanceAfterAmbiguousCommit(err) {
			hadAmbiguousAttempt = true
			if bulkEnablesMaintenance(bulk) && !maintenanceRecoveryScheduled {
				scheduleMaintenanceRecovery(ctx, client, c)
				maintenanceRecoveryScheduled = true
			}
		}
		if internal.HasErrorReason(err, domain.ErrReasonMaintenanceMode) && !hadAmbiguousAttempt {
			break
		}
		if !internal.IsTransient(err) && !internal.IsCanceled(err) {
			break
		}
		hadAmbiguousAttempt = hadAmbiguousAttempt || internal.IsAmbiguousCommit(err)
		select {
		case <-ctx.Done():
		case <-time.After(200 * time.Millisecond):
		}
	}

	dumpBatch(ticket, req, resp, err)
	if probeDone != nil {
		<-probeDone
	}
	obs := observation{ticket: ticket, bulk: bulk, resp: resp, err: err, observeTicket: c.ticketSeq.Load()}
	if checkpointCreate {
		obs.processed = make(chan struct{})
	}
	// Register the disable recovery before publishing the successful enable.
	// The processor may otherwise make that enable visible to restore, which can
	// begin draining while no recovery read protects the maintenance window.
	if err == nil && bulkEnablesMaintenance(bulk) && !maintenanceRecoveryScheduled {
		scheduleMaintenanceRecovery(ctx, client, c)
	}
	select {
	case <-ctx.Done():
		return
	case c.incoming <- obs:
	}
	if checkpointCreate {
		select {
		case <-ctx.Done():
		case <-obs.processed:
		}
	}
}

func scheduleMaintenanceRecovery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	c.maintenanceEnableSeq++
	if c.maintenanceRecoveryActive {
		c.mu.Unlock()
		return
	}
	c.maintenanceRecoveryActive = true
	recoveryID := c.registerRead()
	c.recoveries.Add(1)
	c.mu.Unlock()

	go func() {
		defer c.recoveries.Done()
		defer c.finishRead(recoveryID)
		for {
			c.mu.Lock()
			enableSeq := c.maintenanceEnableSeq
			c.mu.Unlock()

			delay := time.Duration(internal.Rand().Int63n(int64(maintenanceMaxWindow)))
			select {
			case <-ctx.Done():
				c.mu.Lock()
				c.maintenanceRecoveryActive = false
				c.mu.Unlock()
				return
			case <-time.After(delay):
			}
			dispatchMaintenanceRecovery(ctx, client, c)

			c.mu.Lock()
			if c.maintenanceEnableSeq == enableSeq {
				c.maintenanceRecoveryActive = false
				c.mu.Unlock()
				return
			}
			c.mu.Unlock()
		}
	}()
}

// dispatchMaintenanceRecovery bypasses the restore pause because the recovery
// read registered before its delay keeps pauseAndDrain from completing. A fresh
// key prevents deliberate conflict injection from turning a temporary
// maintenance window into a permanent stall.
func dispatchMaintenanceRecovery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	bulk := oracle.Bulk{
		Requests:       []*servicepb.Request{actions.SetMaintenanceModeAction(false)},
		IdempotencyKey: idempotencyKey(),
	}

	c.mu.Lock()
	ticket := c.registerInflight(bulk)
	c.mu.Unlock()

	req := applyRequest(bulk)
	var resp *servicepb.ApplyResponse
	var err error
	for {
		resp, err = client.Apply(ctx, req)
		if err == nil || ctx.Err() != nil || (!internal.IsTransient(err) && !internal.IsCanceled(err)) {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
	dumpBatch(ticket, req, resp, err)
	obs := observation{ticket: ticket, bulk: bulk, resp: resp, err: err, observeTicket: c.ticketSeq.Load()}
	select {
	case <-ctx.Done():
		return
	case c.incoming <- obs:
	}
}

func bulkEnablesMaintenance(bulk oracle.Bulk) bool {
	for _, req := range bulk.Requests {
		if toggle := req.GetSetMaintenanceMode(); toggle != nil && toggle.GetEnabled() {
			return true
		}
	}
	return false
}

// initialSchema generates a small, random metadata schema declared at ledger
// creation via CreateLedger's initial_schema: 0-3 field types across targets,
// keys, and types from the same pools the runtime schema ops use. NewChecker
// seeds the same declarations so the model's schema state matches the server's
// from the start. Declaring a type creates no index (populateInitialSchema only
// records the schema), so a later remove drops nothing.
func initialSchema() []*commonpb.SetMetadataFieldTypeCommand {
	n := int(random.RandomChoice([]uint8{0, 1, 2, 3}))
	cmds := make([]*commonpb.SetMetadataFieldTypeCommand, 0, n)
	for i := 0; i < n; i++ {
		cmds = append(cmds, &commonpb.SetMetadataFieldTypeCommand{
			TargetType: random.RandomChoice(metaTargetPool),
			Key:        metaKey(),
			Type:       random.RandomChoice(metaTypePool),
		})
	}

	return cmds
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
func setupLedgers(ctx context.Context, client servicepb.BucketServiceClient, names []string, schemas map[string][]*commonpb.SetMetadataFieldTypeCommand) bool {
	for _, name := range names {
		err := internal.CreateLedger(ctx, client, name, schemas[name]...)
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
