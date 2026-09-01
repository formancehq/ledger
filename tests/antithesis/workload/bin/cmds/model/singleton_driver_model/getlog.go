package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// learnedLog is the Apply log a global sequence belongs to.
type learnedLog struct {
	ledger string
	id     uint64
}

// learnedLogSeqWindow bounds the seq→log table. An unbounded one would grow for
// the whole run, and a recent sequence is also one chapter archiving cannot have
// moved to cold storage yet — so the window keeps the oracle exact without
// having to model the archive boundary.
const learnedLogSeqWindow = 4096

// unassignedSeqSlack is how far past the high-water mark an unassigned sequence
// is picked. Sequences are handed out in order, so nothing this far ahead can be
// taken however many bulks are in flight.
const unassignedSeqSlack = 1_000_000

// learnLogSequences records which Apply log took each global sequence a
// committed bulk's response carried. Only Apply logs are learnable: ledger
// metadata is dispatched as its own top-level payload and takes no ledger-local
// id (see oracle.logKindFor), so its sequence is counted into maxLogSeq and
// otherwise skipped.
//
// Caller holds c.mu and calls this only on the committed state, in the same
// critical section that advanced it — the safety condition learnTxStamps
// documents, for the same reason.
func (c *Checker) learnLogSequences(bulk oracle.Bulk, logs []*commonpb.Log) {
	for i, req := range bulk.Requests {
		if i >= len(logs) {
			break
		}

		seq := logs[i].GetSequence()
		if seq == 0 {
			continue
		}

		if seq > c.maxLogSeq {
			c.maxLogSeq = seq
		}

		id := logs[i].GetPayload().GetApply().GetLog().GetId()
		if id == 0 {
			continue
		}

		if _, dup := c.logBySeq[seq]; dup {
			continue
		}

		c.logBySeq[seq] = learnedLog{ledger: oracle.LedgerOf(req), id: id}
		c.logSeqRing = append(c.logSeqRing, seq)

		if len(c.logSeqRing) > learnedLogSeqWindow {
			delete(c.logBySeq, c.logSeqRing[0])
			c.logSeqRing = c.logSeqRing[1:]
		}
	}
}

// pickLogSequence chooses a sequence to read back: usually one whose Apply log
// the model learned, one in eight a sequence far past every one handed out.
// ok=false before any log has been learned.
func (c *Checker) pickLogSequence() (seq uint64, want learnedLog, kind string, learned, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.logSeqRing) == 0 {
		return 0, learnedLog{}, "", false, false
	}

	if oneIn(8) {
		return c.maxLogSeq + unassignedSeqSlack, learnedLog{}, "", false, true
	}

	seq = c.logSeqRing[internal.Rand().Intn(len(c.logSeqRing))]
	want = c.logBySeq[seq]

	// The log was drained into the committed state before its sequence was
	// learned, so its kind is there to read.
	kind, _ = c.modelState.Ledger(want.ledger).LogKindAt(want.id)

	return seq, want, kind, true, true
}

// runGetLog reads one log by global sequence and checks it against what the
// model learned.
//
// A log is immutable once committed and GetLog falls back to cold storage
// (ReadLogBySequenceWithCold), so a learned sequence must resolve to exactly the
// same log every time — no candidate-base search is needed, and NotFound on one
// is a lost log rather than staleness. The reverse direction is checked too: a
// sequence past every one handed out must resolve to nothing.
func runGetLog(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	seq, want, wantKind, learned, ok := c.pickLogSequence()
	if !ok {
		return
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	log, err := client.GetLog(readCtx, &servicepb.GetLogRequest{Sequence: seq})

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if status.Code(err) != codes.NotFound {
			assert.Unreachable("singleton_driver_model: GetLog returned unexpected error", internal.Details{
				"sequence": seq,
				"learned":  learned,
				"error":    err.Error(),
			})

			return
		}

		if !learned {
			// Coverage: a sequence nothing was assigned resolves to nothing.
			assert.Reachable("singleton_driver_model: GetLog on an unassigned sequence returned NotFound", internal.Details{"sequence": seq})

			return
		}

		assert.Unreachable("singleton_driver_model: GetLog lost a committed log", internal.Details{
			"sequence":   seq,
			"wantLedger": want.ledger,
			"wantLogID":  want.id,
		})

		return
	}

	if !learned {
		assert.Unreachable("singleton_driver_model: GetLog served an unassigned sequence", internal.Details{
			"sequence":     seq,
			"maxLearned":   seq - unassignedSeqSlack,
			"servedLedger": log.GetPayload().GetApply().GetLedgerName(),
			"servedLogID":  log.GetPayload().GetApply().GetLog().GetId(),
		})

		return
	}

	gotLedger := log.GetPayload().GetApply().GetLedgerName()
	gotID := log.GetPayload().GetApply().GetLog().GetId()
	gotKind := serverLogKind(log)

	if log.GetSequence() != seq || gotLedger != want.ledger || gotID != want.id || gotKind != wantKind {
		assert.Unreachable("singleton_driver_model: GetLog answered with another log", internal.Details{
			"sequence":     seq,
			"servedSeq":    log.GetSequence(),
			"wantLedger":   want.ledger,
			"servedLedger": gotLedger,
			"wantLogID":    want.id,
			"servedLogID":  gotID,
			"wantKind":     wantKind,
			"servedKind":   gotKind,
		})

		return
	}

	// Coverage: a log read back by its global sequence was the one the model
	// learned there.
	assert.Reachable("singleton_driver_model: GetLog validated", internal.Details{
		"ledger": want.ledger,
		"kind":   gotKind,
	})
}
