package main

import (
	"context"
	"strconv"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// unassignedSeqSlack is how far past the highest learned sequence an unassigned
// sequence is picked. Sequences are handed out in order, so nothing this far
// ahead can be taken however many bulks are in flight.
const unassignedSeqSlack = 1_000_000

// committedLogTarget names a committed log by ledger and id together with the
// global sequence the model learned for it at commit.
type committedLogTarget struct {
	ledger   string
	id       uint64
	sequence uint64
}

// pickLogSequence chooses a sequence to read back: usually one the state's
// global log stream holds, one in eight a sequence far past every one handed
// out. ok=false before any sequence has been learned.
func pickLogSequence(state oracle.GlobalState) (target committedLogTarget, learned, ok bool) {
	var (
		known  []committedLogTarget
		maxSeq uint64
	)

	for seq, entry := range state.Logs() {
		known = append(known, committedLogTarget{ledger: entry.Ledger, id: entry.ID, sequence: seq})
		maxSeq = max(maxSeq, seq)
	}

	if len(known) == 0 {
		return committedLogTarget{}, false, false
	}

	if oneIn(8) {
		return committedLogTarget{sequence: maxSeq + unassignedSeqSlack}, false, true
	}

	return known[internal.Rand().Intn(len(known))], true, true
}

// runGetLog reads one log by global sequence and checks it against the model's
// own record of that log, field by field, the way a ListLogs page is checked.
//
// A log is immutable once committed and GetLog falls back to cold storage, so a
// learned sequence must resolve to the same log on every base; NotFound on one
// is a lost log rather than staleness. The reverse direction is checked too: a
// sequence past every one handed out must resolve to nothing.
func runGetLog(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	state := c.modelState
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Picking runs lock-free on the snapshot. The ticket is taken in the same
	// critical section the snapshot comes from, so no bulk observed from here on
	// — a DeleteLedger among them, which retires the ledger's whole log stream
	// from the model — can drain between the pick and the comparison.
	target, learned, ok := pickLogSequence(state)
	if !ok {
		return
	}

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	log, err := client.GetLog(readCtx, &servicepb.GetLogRequest{Sequence: target.sequence})

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if status.Code(err) != codes.NotFound {
			assert.Unreachable("singleton_driver_model: GetLog returned unexpected error", internal.Details{
				"sequence": target.sequence,
				"learned":  learned,
				"error":    err.Error(),
			})

			return
		}

		if !learned {
			// Coverage: a sequence nothing was assigned resolves to nothing.
			assert.Reachable("singleton_driver_model: GetLog on an unassigned sequence returned NotFound", internal.Details{"sequence": target.sequence})

			return
		}

		assert.Unreachable("singleton_driver_model: GetLog lost a committed log", internal.Details{
			"sequence":   target.sequence,
			"wantLedger": target.ledger,
			"wantLogID":  target.id,
		})

		return
	}

	if !learned {
		assert.Unreachable("singleton_driver_model: GetLog served an unassigned sequence", internal.Details{
			"sequence":     target.sequence,
			"maxLearned":   target.sequence - unassignedSeqSlack,
			"servedLedger": log.GetPayload().GetApply().GetLedgerName(),
			"servedLogID":  log.GetPayload().GetApply().GetLog().GetId(),
		})

		return
	}

	got := serverLogRows([]*commonpb.Log{log})[0]

	if !c.matchesModel(maxTicket, "GETLOG", func(base oracle.GlobalState) bool {
		return committedLogMatches(base, target, got)
	}) {
		assert.Unreachable("singleton_driver_model: GetLog answered with another log", internal.Details{
			"sequence":     target.sequence,
			"wantLedger":   target.ledger,
			"wantLogID":    target.id,
			"servedLedger": got.ledger,
			"servedLogID":  got.id,
			"servedSeq":    got.sequence,
			"servedRow":    describeServerLogRows([]serverLogRow{got}),
			"modelRow":     c.describeCommittedLog(target),
		})

		return
	}

	// Coverage: a log read back by its global sequence matched the model's
	// record of it.
	assert.Reachable("singleton_driver_model: GetLog validated", internal.Details{
		"ledger": target.ledger,
		"kind":   got.kind,
	})
}

// committedLogMatches reports whether got is the log the base's global stream
// holds at target's sequence: the same owner, the same id, and the same row.
// The stream outlives its ledgers, so a deleted ledger's log still resolves.
func committedLogMatches(base oracle.GlobalState, target committedLogTarget, got serverLogRow) bool {
	entry, ok := base.Log(target.sequence)
	if !ok || entry.Ledger != target.ledger || entry.ID != target.id {
		return false
	}

	if got.sequence != target.sequence || got.id != target.id {
		return false
	}

	return logRowMatches(target.ledger, globalLogWindowRow(entry), got)
}

// globalLogWindowRow is one entry of the global log stream as a comparable row.
func globalLogWindowRow(entry oracle.GlobalLogRow) logWindowRow {
	row := logWindowRow{
		id: entry.Row.ID, kind: entry.Row.Kind, payload: entry.Row.Payload,
		date: entry.Row.Date, sequence: entry.Row.Sequence,
		purged: entry.Row.PurgedVolumes, newKept: entry.Row.NewKeptVolumes, ephemeral: entry.Row.EphemeralVolumes,
		required: true,
	}

	if entry.Tx != nil {
		row.tx = entry.Tx
		row.revertsID = entry.Tx.RevertsTransaction()
	}

	return row
}

// describeCommittedLog renders the committed model's row for a finding's
// diagnostics. Acquires c.mu.
func (c *Checker) describeCommittedLog(target committedLogTarget) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.modelState.Log(target.sequence)
	if !ok {
		return "absent"
	}

	return entry.Ledger + "/" + strconv.FormatUint(entry.ID, 10) + ":" + entry.Row.Kind + "@seq" + strconv.FormatUint(entry.Row.Sequence, 10) + "[" + entry.Row.Payload + "]"
}
