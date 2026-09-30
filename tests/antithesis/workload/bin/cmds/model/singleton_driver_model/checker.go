package main

import (
	"fmt"
	"slices"
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
	// dispatchMu orders write registration against a read's response frontier.
	// Reads hold it through the response frontier. Writers wait on it before
	// taking mu, so a stalled read never blocks processor model work.
	dispatchMu sync.Mutex
	// checkpointCreateMu keeps a predicted-ID probe paired with exactly one
	// create transition until that transition has drained into modelState.
	checkpointCreateMu sync.Mutex
	ledgerMu           sync.RWMutex

	// ledgerNames grows when a generated CreateLedger commits. Deleted names stay
	// reserved in the oracle but are filtered from generation and reads.
	ledgerNames           []string
	ledgerPrefix          string
	liveTarget            int
	ledgerSeq             atomic.Uint64
	pendingLedgerCreates  int
	reservedLedgerCreates map[string]uint64

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

	// rejections is the set of rejection shapes the model explained, as the
	// audit trail records a rejected bulk: the distinct ledgers it touched, its
	// order count and the reason. The oracle keeps no rejected history of its
	// own, since a rejection leaves its state untouched. Guarded by mu.
	rejections map[rejectedBulk]struct{}

	// ledgerLogSeqs maps the global sequence of each committed log that carries
	// no ledger-local id — a ledger-metadata log — to its ledger and kind. The
	// oracle keeps no row for those, so both are learned here at drain. Guarded
	// by mu.
	ledgerLogSeqs map[uint64]ledgerLogRecord

	// committedLogs maps the global sequence of every committed log to what the
	// model knows about it. The audit trail is permanent while a deleted ledger's
	// rows leave the state, so the trail keeps naming sequences the live model no
	// longer holds. Guarded by mu.
	committedLogs map[uint64]committedLog

	// committedBulks maps the first log sequence of each committed bulk to its
	// boundaries. One bulk is one audit entry, so these are the only log ranges a
	// success entry may name: without them a fabricated entry merging two adjacent
	// bulks, or naming part of one, satisfies every other check. Guarded by mu.
	committedBulks map[uint64]committedBulk

	// ledgerIdentities maps each ledger to the id and creation timestamp its
	// creation log reported. The model assigns neither, so this is the only
	// record a read of those two fields can be held to. Guarded by mu.
	ledgerIdentities map[string]ledgerIdentity

	// knownAudit is a lower bound on the audit trail: entries the server served
	// on a page that validated. Audit history is permanent, so a remembered entry
	// still exists on every later read. The set is never complete — the trail also
	// holds proposals this driver never made — so it can only ever prove that MORE
	// entries match, never that none do. Guarded by mu.
	knownAudit map[uint64]auditEntry

	// auditSamples holds indexed fields of served audit entries, for aiming
	// audit filters at values the trail holds. Guarded by mu.
	auditSamples []auditSample

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
	// probe has itself been observed and model-validated.
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
	maintenanceRecoveryTicket uint64
	ambiguousBulks            map[uint64]oracle.Bulk
	ambiguousEnableClearSeq   uint64
	recoveries                sync.WaitGroup
}

// One worker → processor message. observeTicket is the ticket high-water mark
// when the response was received; the drain gate uses it to tell which
// outstanding ops were dispatched after this bulk was observed.
type observation struct {
	ticket          uint64
	bulk            oracle.Bulk
	resp            *servicepb.ApplyResponse
	err             error
	ambiguousEnable bool
	recoverySeq     uint64
	observeTicket   uint64
	processed       chan struct{}
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

	prefix := strings.TrimSuffix(ledgerNames[0], "-0")
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
		ambiguousBulks:             map[uint64]oracle.Bulk{},
		reservedLedgerCreates:      map[string]uint64{},
		ledgerLogSeqs:              map[uint64]ledgerLogRecord{},
		ledgerIdentities:           map[string]ledgerIdentity{},
		knownAudit:                 map[uint64]auditEntry{},
		committedBulks:             map[uint64]committedBulk{},
		committedLogs:              map[uint64]committedLog{},
		rejections:                 map[rejectedBulk]struct{}{},

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

// reserveLedgerCreate keeps concurrent workers from all replacing the same
// deleted ledger. The reservation remains held until dispatchBulk's observation
// has been processed, at which point a success is already reflected in
// modelState and a failure no longer consumes capacity.
func (c *Checker) reserveLedgerCreate(bulk oracle.Bulk) bool {
	var name string
	for _, req := range bulk.Requests {
		if req.GetCreateLedger() != nil {
			name = req.GetCreateLedger().GetName()

			break
		}
	}
	if name == "" {
		return true
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if lifecycle, exists := c.modelState.Lifecycle(name); exists && lifecycle.Deleted {
		return true
	}
	if len(liveLedgerNames(c.modelState, c.ledgerNamesSnapshot()))+c.pendingLedgerCreates >= c.liveTarget {
		return false
	}
	c.pendingLedgerCreates++
	c.reservedLedgerCreates[name]++

	return true
}

func (c *Checker) releaseLedgerCreate(bulk oracle.Bulk) {
	for _, req := range bulk.Requests {
		name := req.GetCreateLedger().GetName()
		if name == "" {
			continue
		}
		c.mu.Lock()
		if c.reservedLedgerCreates[name] > 0 {
			c.pendingLedgerCreates--
			c.reservedLedgerCreates[name]--
			if c.reservedLedgerCreates[name] == 0 {
				delete(c.reservedLedgerCreates, name)
			}
		}
		c.mu.Unlock()

		return
	}
}

func liveLedgerNames(state oracle.GlobalState, names []string) []string {
	live := make([]string, 0, len(names))
	for _, name := range names {
		if ledgerIsLive(state, name) {
			live = append(live, name)
		}
	}

	return live
}

func ledgerIsLive(state oracle.GlobalState, name string) bool {
	lifecycle, exists := state.Lifecycle(name)

	return exists && !lifecycle.Deleted
}

func partitionLifecycleLedgers(state oracle.GlobalState, names []string) (live, deleted []string) {
	live = make([]string, 0, len(names))
	deleted = make([]string, 0, len(names))
	for _, name := range names {
		lifecycle, exists := state.Lifecycle(name)
		if !exists {
			continue
		}
		if lifecycle.Deleted {
			deleted = append(deleted, name)
		} else {
			live = append(live, name)
		}
	}

	return live, deleted
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

// rejectedBulk is the shape of one model-explained rejection as the audit trail
// records it. Every audit page from the start of the trail can name any past
// rejection, so the set is never pruned; it is bounded by the distinct shapes,
// not by the number of rejections.
type rejectedBulk struct {
	ledgers string // distinct ledgers, ascending, comma-joined
	orders  uint32
	reason  string // domain reason name, e.g. INSUFFICIENT_FUNDS
}

// recordRejection remembers a rejection the model explained. Caller holds c.mu.
func (c *Checker) recordRejection(bulk oracle.Bulk, reason string) {
	c.rejections[rejectedBulk{
		ledgers: strings.Join(distinctLedgers(bulk), ","),
		orders:  uint32(len(bulk.Requests)),
		reason:  reason,
	}] = struct{}{}
}

// distinctLedgers lists the ledgers a bulk's requests name, ascending.
func distinctLedgers(bulk oracle.Bulk) []string {
	seen := map[string]bool{}
	var out []string
	for _, r := range bulk.Requests {
		if l := oracle.LedgerOf(r); l != "" && !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	slices.Sort(out)

	return out
}

// committedBulk is the log range one committed bulk produced, with the number
// of orders that produced it.
type committedBulk struct {
	minSeq, maxSeq uint64
	orders         uint32
}

// recordCommittedBulk remembers the boundaries a bulk committed at. Caller holds
// c.mu.
func (c *Checker) recordCommittedBulk(bulk oracle.Bulk, logs []*commonpb.Log) {
	var minSeq, maxSeq uint64

	for _, l := range logs {
		seq := l.GetSequence()
		if seq == 0 {
			continue
		}

		if minSeq == 0 || seq < minSeq {
			minSeq = seq
		}

		maxSeq = max(maxSeq, seq)
	}

	if minSeq == 0 {
		return
	}

	c.committedBulks[minSeq] = committedBulk{minSeq: minSeq, maxSeq: maxSeq, orders: uint32(len(bulk.Requests))}

	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		seq := logs[i].GetSequence()
		if seq == 0 {
			continue
		}

		apply := logs[i].GetPayload().GetApply()
		c.committedLogs[seq] = committedLog{
			ledger: apply.GetLedgerName(),
			id:     apply.GetLog().GetId(),
			kind:   requestLogKind(req),
		}

		if created := logs[i].GetPayload().GetCreateLedger(); created != nil {
			c.ledgerIdentities[created.GetName()] = ledgerIdentity{
				id:        created.GetId(),
				createdAt: created.GetCreatedAt(),
			}
		}
	}
}

// requestLogKind names the order a request carries, the way the audit index
// keys it: the accepted intent's payload variant. An order apply skipped is
// still the order that was accepted, so the skip does not change its kind.
func requestLogKind(req *servicepb.Request) string {
	switch r := req.GetType().(type) {
	case *servicepb.Request_Apply:
		switch r.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			return "created_transaction"
		case *servicepb.LedgerAction_RevertTransaction:
			return "reverted_transaction"
		case *servicepb.LedgerAction_AddMetadata:
			return "saved_metadata"
		case *servicepb.LedgerAction_DeleteMetadata:
			return "deleted_metadata"
		case *servicepb.LedgerAction_AddAccountType:
			return "added_account_type"
		case *servicepb.LedgerAction_RemoveAccountType:
			return "removed_account_type"
		case *servicepb.LedgerAction_SetDefaultEnforcementMode:
			return "updated_default_enforcement_mode"
		default:
			return ""
		}
	case *servicepb.Request_SetMetadataFieldType:
		return "set_metadata_field_type"
	case *servicepb.Request_RemoveMetadataFieldType:
		return "removed_metadata_field_type"
	case *servicepb.Request_CreateIndex:
		return "create_index"
	case *servicepb.Request_DropIndex:
		return "drop_index"
	case *servicepb.Request_AddAccountType:
		return "added_account_type"
	case *servicepb.Request_RemoveAccountType:
		return "removed_account_type"
	case *servicepb.Request_SetDefaultEnforcementMode:
		return "updated_default_enforcement_mode"
	default:
		return ledgerLogKindOf(req)
	}
}

// learnLedgerLogSequences records the sequences of a committed bulk's logs
// that carry no ledger-local id. Caller holds c.mu.
func (c *Checker) learnLedgerLogSequences(bulk oracle.Bulk, logs []*commonpb.Log) {
	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		seq := logs[i].GetSequence()
		if seq == 0 || logs[i].GetPayload().GetApply().GetLog().GetId() != 0 {
			continue
		}

		c.ledgerLogSeqs[seq] = ledgerLogRecord{ledger: oracle.LedgerOf(req), kind: ledgerLogKindOf(req)}
	}
}

// ledgerLogRecord is a committed ledger-metadata log: the ledger it targets and
// the order that produced it.
type ledgerLogRecord struct {
	ledger string
	kind   string
}

// ledgerLogKindOf names the order behind a ledger-metadata log, empty for a
// request that produces none.
func ledgerLogKindOf(req *servicepb.Request) string {
	switch req.GetType().(type) {
	case *servicepb.Request_SaveLedgerMetadata:
		return "saved_ledger_metadata"
	case *servicepb.Request_DeleteLedgerMetadata:
		return "deleted_ledger_metadata"
	default:
		return ""
	}
}
