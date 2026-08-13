package main

import (
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

	// ledgerNames is the fleet the generator and reads draw from. Immutable.
	ledgerNames []string

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

	// committedSeq is the highest global log sequence drained into modelState —
	// the model's committed frontier. Query reads pin Read.MinLogSequence to
	// observedFrontier (committedSeq plus observed-but-undrained successes) so
	// the server's snapshot is at least every state the drain gate may fold
	// beneath the read; a windowed read validated by exact ordered equality
	// needs a snapshot the (forward-folded) candidate bases can represent,
	// unlike the tolerant single-cell reads.
	committedSeq uint64

	// receiptByRef maps a committed transaction's reference to the signed receipt
	// the server returned for it, so generateRevert can exercise the
	// receipt-carried revert path. Guarded by its own leaf mutex (receiptsMu, never
	// held together with anything else) so lock-free generation can look receipts
	// up without touching mu. Populated at commit (captureReceipts), read through
	// receiptFor.
	receiptsMu   sync.Mutex
	receiptByRef map[string]string

	// retypeObs tracks each open retype window's per-node closure progress —
	// see retypeObservation. Keyed by retypeObsKey. Guarded by mu.
	retypeObs map[string]*retypeObservation

	// replayable holds committed bulks that carried a tracked idempotency key —
	// the originals runReplay re-sends to exercise the server's idempotency
	// replay. Populated at commit (rememberReplayable), capped at
	// replayRegistryCap. Guarded by mu.
	replayable []replayEntry

	// paused gates worker dispatch during a restore cycle; resumeCh is closed on
	// resume so parked workers wake. Both guarded by mu (see restore.go).
	paused   bool
	resumeCh chan struct{}
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

	return &Checker{
		ledgerNames:  ledgerNames,
		inflight:     map[uint64]oracle.Bulk{},
		reads:        map[uint64]struct{}{},
		incoming:     make(chan observation, incomingBuffer),
		modelState:   modelState,
		receiptByRef: map[string]string{},
		retypeObs:    map[string]*retypeObservation{},
	}
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

// receiptFor returns the captured receipt for ref ("" when none). Safe without
// c.mu — see receiptByRef.
func (c *Checker) receiptFor(ref string) string {
	c.receiptsMu.Lock()
	defer c.receiptsMu.Unlock()

	return c.receiptByRef[ref]
}
