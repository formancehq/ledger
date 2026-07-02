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
// the model's schema state matches the server's from the first bulk.
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

		modelState = modelState.Apply(oracle.Bulk{Requests: reqs}).State
	}

	return &Checker{
		ledgerNames: ledgerNames,
		inflight:    map[uint64]oracle.Bulk{},
		reads:       map[uint64]struct{}{},
		incoming:    make(chan observation, incomingBuffer),
		modelState:  modelState,
	}
}
