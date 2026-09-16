package main

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// Checker drives validation against the model: it owns the in-flight/pending
// bulks (one re-order buffer, ordered by global log sequence) and the model's
// committed state across all ledgers. It mirrors the single Raft log — every
// bulk, whatever ledgers it touches, commits to the cluster in one global order.
//
// Concurrency: mu guards every field. Workers hold mu only for the brief
// generate-bulk + register-inflight window; the processor goroutine
// (processor.go) drains responses through the re-order buffer under mu.
// Expensive validation searches run on a snapshot taken under mu, not under it.
type Checker struct {
	mu sync.Mutex
	// checkpointCreateMu keeps a predicted-ID probe paired with exactly one
	// create transition until that transition has drained into modelState.
	checkpointCreateMu sync.Mutex
	// cycleMu gives a restore or lifecycle episode exclusive ownership of the
	// dispatch pause. Neither may resume workers while the other is active.
	cycleMu  sync.Mutex
	ledgerMu sync.RWMutex

	// ledgerNames grows when a generated CreateLedger commits. Deleted names stay
	// reserved in the oracle but are filtered from generation and reads.
	ledgerNames  []string
	ledgerPrefix string
	liveTarget   int
	ledgerSeq    atomic.Uint64

	// ticketSeq hands out a monotonic ticket per dispatched operation (bulk or
	// read) — the dispatch order the drain gate compares against. It is atomic
	// so a worker can snapshot the high-water mark at observe time
	// (observation.observeTicket) without taking the lock.
	ticketSeq atomic.Uint64

	// inflight: dispatched bulks whose response hasn't been observed yet, keyed
	// by ticket (their dispatch order). The value is what the serialization
	// search (candidateBases) folds.
	inflight map[uint64]oracle.Bulk

	// pending: observed successes not yet drained, sorted by minSeq.
	pending []*pendingObservation

	// reads: tickets of outstanding reads. Holding a read's ticket gates draining
	// (see tryDrain), so reads need no drain-race skip.
	reads map[uint64]struct{}

	// Worker → processor channel.
	incoming chan observation

	// modelState is the committed (drained) state across all ledgers. Bulks
	// drain in global log-sequence order, so it is always the exact predecessor
	// of the next bulk to validate, and the base candidateBases folds the
	// in-flight set onto.
	modelState oracle.GlobalState

	// Frozen business states are published only as their creation drains.
	checkpoints                map[uint64]checkpointSnapshot
	deletedCheckpoints         []uint64
	deletedCheckpointSnapshots map[uint64]checkpointSnapshot

	// retypeObs tracks each open retype window's per-node closure progress —
	// see retypeObservation. Keyed by retypeObsKey. Guarded by mu.
	retypeObs map[string]*retypeObservation

	// indexCreateSeq is each tracked index's create frontier: the committed log
	// sequence of the latest CreateIndex folded for (ledger → canonical). A
	// drop+recreate reuses the canonical but moves the frontier, so it is the
	// index's lifecycle generation. The readiness poller snapshots it before
	// polling, discards any per-node report whose indexer has not folded past
	// it (the report describes the prior incarnation), and refuses to apply a
	// verdict once the frontier moved under it. Guarded by mu.
	indexCreateSeq map[string]map[string]uint64

	// replayable holds committed bulks that carried a tracked idempotency key —
	// the originals runReplay re-sends to exercise the server's idempotency
	// replay. Populated at commit (rememberReplayable), capped at
	// replayRegistryCap. Guarded by mu.
	replayable []replayEntry

	// Lifecycle coverage is credited only after the follow-up promised by the
	// sonde has itself been observed and model-validated.
	pendingDeleted  map[string]struct{}
	pendingPromoted map[string]struct{}

	// paused gates worker dispatch during a restore cycle; resumeCh is closed on
	// resume so parked workers wake. Both guarded by mu (see restore.go).
	paused   bool
	resumeCh chan struct{}

	// Maintenance recovery is coalesced so concurrent successful enables do not
	// create an unbounded fleet of delayed disable RPCs. Guarded by mu.
	maintenanceEnableSeq      uint64
	maintenanceRecoveryActive bool
	recoveries                sync.WaitGroup
}

// One worker → processor message. observeTicket is the ticket high-water mark
// when the response was received; the drain gate uses it to tell which
// outstanding ops were dispatched after this bulk was observed.
type observation struct {
	ticket        uint64
	bulk          oracle.Bulk
	resp          *servicepb.ApplyResponse
	err           error
	observeTicket uint64
	processed     chan struct{}
}

func isCheckpointCreate(bulk oracle.Bulk) bool {
	return len(bulk.Requests) == 1 && bulk.Requests[0].GetCreateQueryCheckpoint() != nil
}

// Buffered observation awaiting in-order replay. minSeq = the bulk's smallest
// Log.Sequence.
type pendingObservation struct {
	minSeq uint64
	obs    observation
}

// NewChecker returns a checker seeded with each ledger's initial metadata schema
// (declared at creation, see setupLedgers); caller spawns the processor
// goroutine. The schema is replayed as SetMetadataFieldType orders — the server
// records the identical declared types at creation (populateInitialSchema), so
// the model's schema state matches the server's from the first bulk. They are
// seeded rather than applied: at creation they produce no ledger log.
func NewChecker(ledgerNames []string, schemas map[string][]*commonpb.SetMetadataFieldTypeCommand) *Checker {
	modelState := oracle.NewGlobalState()
	for _, ledger := range ledgerNames {
		// setupLedgers created these outside the modeled Apply stream. Seed their
		// identities so lifecycle generation can delete or otherwise target even
		// a still-empty initial ledger without predicting LEDGER_NOT_FOUND.
		created := modelState.Apply(oracle.Bulk{Requests: []*servicepb.Request{{
			Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: ledger}},
		}}})
		modelState = created.State

		cmds := schemas[ledger]
		if len(cmds) == 0 {
			continue
		}

		reqs := make([]*servicepb.Request, 0, len(cmds))
		for _, cmd := range cmds {
			reqs = append(reqs, &servicepb.Request{
				Type: &servicepb.Request_SetMetadataFieldType{
					SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
						Ledger:     ledger,
						TargetType: cmd.GetTargetType(),
						Key:        cmd.GetKey(),
						Type:       cmd.GetType(),
					},
				},
			})
		}

		// Seeded, not applied: the server declares these at creation, so they
		// emit no ledger log (see SeedInitialSchema).
		modelState = modelState.SeedInitialSchema(reqs)
	}

	prefix := strings.TrimSuffix(ledgerNames[0], fmt.Sprintf("-%d", len(ledgerNames)-1))
	c := &Checker{
		ledgerNames:                ledgerNames,
		ledgerPrefix:               prefix,
		liveTarget:                 len(ledgerNames),
		inflight:                   map[uint64]oracle.Bulk{},
		reads:                      map[uint64]struct{}{},
		incoming:                   make(chan observation, incomingBuffer),
		modelState:                 modelState,
		checkpoints:                map[uint64]checkpointSnapshot{},
		deletedCheckpointSnapshots: map[uint64]checkpointSnapshot{},
		retypeObs:                  map[string]*retypeObservation{},
		pendingDeleted:             map[string]struct{}{},
		pendingPromoted:            map[string]struct{}{},

		indexCreateSeq: map[string]map[string]uint64{},
	}
	c.ledgerSeq.Store(uint64(len(ledgerNames)))
	return c
}

func (c *Checker) nextLedgerName() string {
	return fmt.Sprintf("%s-%d", c.ledgerPrefix, c.ledgerSeq.Add(1)-1)
}

func (c *Checker) ledgerNamesSnapshot() []string {
	c.ledgerMu.RLock()
	defer c.ledgerMu.RUnlock()
	return append([]string(nil), c.ledgerNames...)
}

func (c *Checker) liveLedgerNamesSnapshot() []string {
	c.mu.Lock()
	state := c.modelState
	c.mu.Unlock()

	return liveLedgerNames(state, c.ledgerNamesSnapshot())
}

func liveLedgerNames(state oracle.GlobalState, names []string) []string {
	live := make([]string, 0, len(names))
	for _, name := range names {
		if lifecycle, exists := state.Lifecycle(name); exists && !lifecycle.Deleted {
			live = append(live, name)
		}
	}

	return live
}

// retypeObservation drives one retype window's closure, two-phase per node so
// the close can never race the fold: a poll proving the retype's own log
// folded (last_indexed_sequence >= openSeq) arms the node, and only a LATER
// poll showing pending_version == 0 confirms its switch — the two fields need
// not be sampled atomically within one response, but pending cannot return to
// zero before the switch once the bump is known applied. The window closes
// when every node confirmed. openSeq extends and phases reset on a chained
// retype, whose new rewrite must be observed afresh. Guarded by c.mu.
type retypeObservation struct {
	ledger    string
	canonical string
	openSeq   uint64
	foldSeen  map[int]bool
	pendClear map[int]bool

	// confirmedAt is the dispatch-ticket frontier at the moment every node
	// confirmed its switch; zero until then. The window itself closes only
	// once every read ticketed at or before it has finished: a read served
	// from a pre-switch snapshot is validated against the model AFTER its
	// response arrives, and closing under it would judge a legitimately
	// old-typed answer by post-switch rules.
	confirmedAt uint64
}

// closeAllRetypeWindows ends every open retype window, for the restore cycle:
// rebuilt read-stores encode under the live schema, so no replica serves an
// old-typed index after a restore.
func (c *Checker) closeAllRetypeWindows() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, obs := range c.retypeObs {
		c.modelState.CloseRetypeWindow(obs.ledger, obs.canonical)
		delete(c.retypeObs, key)
	}
}

func retypeObsKey(ledger, canonical string) string {
	return ledger + "\x00" + canonical
}

// noteRetypeCommit registers (or re-arms) the closure observation for a
// committed retype whose window the fold just opened or extended. Caller
// holds c.mu.
func (c *Checker) noteRetypeCommit(ledger, canonical string, seq uint64) {
	key := retypeObsKey(ledger, canonical)

	obs, ok := c.retypeObs[key]
	if !ok {
		obs = &retypeObservation{ledger: ledger, canonical: canonical}
		c.retypeObs[key] = obs
	}

	if seq > obs.openSeq {
		obs.openSeq = seq
	}

	obs.foldSeen = map[int]bool{}
	obs.pendClear = map[int]bool{}
	obs.confirmedAt = 0
}
