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

// pickLogSequence chooses a sequence to read back: usually one the committed
// model learned for a log, one in eight a sequence far past every one handed
// out. ok=false before any sequence has been learned. Acquires c.mu.
func (c *Checker) pickLogSequence() (target committedLogTarget, learned, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var (
		known  []committedLogTarget
		maxSeq uint64
	)

	for _, ledger := range c.ledgerNames {
		for _, row := range c.modelState.Ledger(ledger).LogRows() {
			if row.Sequence == 0 {
				continue
			}

			known = append(known, committedLogTarget{ledger: ledger, id: row.ID, sequence: row.Sequence})
			maxSeq = max(maxSeq, row.Sequence)
		}
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
	target, learned, ok := c.pickLogSequence()
	if !ok {
		return
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

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
		return committedLogMatches(base.Ledger(target.ledger), target, got)
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

// committedLogMatches reports whether got is the base's log target names, with
// the sequence the model learned for it.
func committedLogMatches(ls oracle.LedgerState, target committedLogTarget, got serverLogRow) bool {
	rows := ls.LogRows()
	if target.id == 0 || target.id > uint64(len(rows)) {
		return false
	}

	row := rows[target.id-1]
	if row.Sequence != target.sequence || got.sequence != target.sequence {
		return false
	}

	return logRowMatches(target.ledger, logWindowRowOf(ls, row, true), got)
}

// describeCommittedLog renders the committed model's row for a finding's
// diagnostics. Acquires c.mu.
func (c *Checker) describeCommittedLog(target committedLogTarget) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	rows := c.modelState.Ledger(target.ledger).LogRows()
	if target.id == 0 || target.id > uint64(len(rows)) {
		return "absent"
	}

	row := rows[target.id-1]

	return row.Kind + "@seq" + strconv.FormatUint(row.Sequence, 10) + "[" + row.Payload + "]"
}
