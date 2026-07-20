package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// replayEntry is a committed keyed bulk the driver can re-send to exercise the
// server's idempotency replay: the idempotency key, the exact body that froze
// under it, and the log sequences its commit was assigned. Re-sending the same
// body reproduces the recorded outcome (a differing body would conflict); the
// sequences distinguish a replay from a re-execution — a true replay echoes them
// verbatim, a re-execution commits fresh, higher ones.
type replayEntry struct {
	key     string
	bulk    oracle.Bulk
	logSeqs []uint64
}

// stampIdempotency may tag a freshly generated bulk with an idempotency key to
// exercise the server's per-batch dedup. With committed keys on hand it
// sometimes reuses one carrying a DIFFERENT body, which the server must reject
// IDEMPOTENCY_KEY_CONFLICT; otherwise, while the registry has room, it sometimes
// mints a fresh tracked key so the bulk becomes a replayable original. Most bulks
// stay untracked. Caller holds c.mu.
func (c *Checker) stampIdempotency(bulk *oracle.Bulk) {
	if len(c.replayable) > 0 && rollConflict() {
		entry := random.RandomChoice(c.replayable)
		// A differing body is guaranteed to conflict, not replay (RequestsEqual is
		// a faithful proxy for the server's idempotency hash). Skip the rare exact
		// match, which would replay and misroute through the commit path.
		if !oracle.RequestsEqual(entry.bulk.Requests, bulk.Requests) {
			bulk.IdempotencyKey = entry.key

			return
		}
	}

	if len(c.replayable) < replayRegistryCap && rollReplayTrack() {
		bulk.IdempotencyKey = idempotencyKey()
	}
}

// rememberReplayable records a committed keyed bulk so runReplay can later
// re-send it. Only keyed bulks are eligible, and only until the registry is
// full — the cap bounds the model's frozen idempotency map, which the model
// never evicts (infinite TTL). Caller holds c.mu.
func (c *Checker) rememberReplayable(bulk oracle.Bulk, logs []*commonpb.Log) {
	if bulk.IdempotencyKey == "" || len(c.replayable) >= replayRegistryCap {
		return
	}

	c.replayable = append(c.replayable, replayEntry{
		key:     bulk.IdempotencyKey,
		bulk:    bulk,
		logSeqs: logSequences(logs),
	})
}

// runReplay re-sends a previously committed keyed bulk and checks the server
// replays the recorded outcome and commits no new log. It is read-like: a replay
// echoes the original's log references (which the server resolves to the original
// sequences) and produces no fresh log, so it must NOT flow through the
// log-sequence re-order buffer — it registers as a read and validates against
// the candidate states, exactly like GetAccount/GetTransaction.
func runReplay(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	if len(c.replayable) == 0 {
		c.mu.Unlock()
		return
	}
	entry := random.RandomChoice(c.replayable)
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	resp, err := client.Apply(ctx, applyRequest(entry.bulk))
	// High-water at the replay's response: only bulks dispatched by now could be
	// folded into the states this replay is validated against.
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		// The original committed and the server's TTL dwarfs any run, so a replay
		// must not surface a definitive error (a re-execution that now conflicts,
		// or an eviction) — that is a finding.
		assert.Unreachable("singleton_driver_model: idempotency replay returned unexpected error", internal.Details{
			"ledgers": bulkLedgers(entry.bulk),
			"key":     entry.key,
			"error":   err.Error(),
		})
		return
	}

	c.validateReplay(maxTicket, entry, resp)
}

// validateReplay checks a replay's response two ways: it must echo the original
// commit's log sequences verbatim (a fresh sequence means the key was
// re-executed, not replayed — an exactly-once violation), and its content must
// match the model's frozen outcome under some candidate base (Apply replays the
// bulk because the base holds the key, folded from modelState).
func (c *Checker) validateReplay(maxTicket uint64, entry replayEntry, resp *servicepb.ApplyResponse) {
	if !sequencesEqual(logSequences(resp.GetLogs()), entry.logSeqs) {
		assert.Unreachable("singleton_driver_model: idempotency replay produced new log sequences", internal.Details{
			"ledgers":  bulkLedgers(entry.bulk),
			"key":      entry.key,
			"original": entry.logSeqs,
			"replayed": logSeqs(resp.GetLogs()),
		})

		return
	}

	if c.matchesModel(maxTicket, "REPLAY", func(base oracle.GlobalState) bool {
		res := base.Apply(entry.bulk)
		return res.OK && replayOrdersMatch(entry.bulk, res.Orders, resp.GetLogs())
	}) {
		// Coverage: prove the replay path is actually exercised — if this stops
		// firing, the driver has stopped tracking or re-sending keyed bulks.
		assert.Reachable("singleton_driver_model: idempotency replay exercised", internal.Details{})

		return
	}

	assert.Unreachable("singleton_driver_model: idempotency replay outside model", internal.Details{
		"ledgers": bulkLedgers(entry.bulk),
		"key":     entry.key,
		"kinds":   requestKinds(entry.bulk),
		"logSeqs": logSeqs(resp.GetLogs()),
	})
}

// logSequences extracts the log sequences from a response, in order.
func logSequences(logs []*commonpb.Log) []uint64 {
	out := make([]uint64, len(logs))
	for i, l := range logs {
		out[i] = l.GetSequence()
	}

	return out
}

// sequencesEqual reports whether two sequence slices are identical in order.
func sequencesEqual(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// replayOrdersMatch reports whether the server's response logs echo the frozen
// orders — the transaction identity that defines "same result": assigned ids,
// echoed references and postings, per-cell post-commit volumes, and stored
// metadata. It reuses the leaf predicates crossCheckCommit asserts on; the two
// stay separate because crossCheckCommit needs a distinct assert callsite per
// field (Antithesis catalogues by callsite) while a replay diverges as a whole.
func replayOrdersMatch(bulk oracle.Bulk, orders []oracle.OrderResult, logs []*commonpb.Log) bool {
	for i, order := range orders {
		if i >= len(logs) {
			return false
		}

		req := bulk.Requests[i]
		data := logs[i].GetPayload().GetApply().GetLog().GetData()

		switch {
		case order.Revert != nil:
			rt := data.GetRevertedTransaction()
			revTx := rt.GetRevertTransaction()
			if revTx.GetId() != order.TxID ||
				rt.GetRevertedTransactionId() != order.Revert.RevertedID() ||
				!postingsEqual(order.Revert.Postings(), revTx.GetPostings()) {
				return false
			}

		case order.TxID != 0:
			tx := data.GetCreatedTransaction().GetTransaction()
			ct := req.GetApply().GetAction().GetCreateTransaction()
			if tx.GetId() != order.TxID ||
				tx.GetReference() != ct.GetReference() ||
				!postingsEqual(ct.GetPostings(), tx.GetPostings()) ||
				!accountMetaMapEqual(ct.GetAccountMetadata(), data.GetCreatedTransaction().GetAccountMetadata()) {
				return false
			}
		}

		if order.PCV != nil {
			if !pcvMatches(order.PCV, serverPCV(data)) {
				return false
			}
		}

		if order.Meta != nil {
			if !metaMapEqual(order.Meta.Saved(), responseMetaEffect(req, logs[i])) {
				return false
			}
		}
	}

	return true
}

// pcvMatches reports whether every model cell equals the server's post-commit
// volume for that cell.
func pcvMatches(model map[oracle.VolumeKey]oracle.VolumePair, server *commonpb.PostCommitVolumes) bool {
	for key, vp := range model {
		gotIn, gotOut, ok := postCommitVolume(server, key)
		if !ok || vp.Input.Cmp(&gotIn) != 0 || vp.Output.Cmp(&gotOut) != 0 {
			return false
		}
	}

	return true
}

// serverPCV pulls the post-commit volumes off a create or revert response log.
// PCV rides on the transaction itself (CreatedTransaction.Transaction /
// RevertedTransaction.RevertTransaction) and is present on every committed
// transaction.
func serverPCV(data *commonpb.LedgerLogPayload) *commonpb.PostCommitVolumes {
	switch {
	case data.GetCreatedTransaction() != nil:
		return data.GetCreatedTransaction().GetTransaction().GetPostCommitVolumes()
	case data.GetRevertedTransaction() != nil:
		return data.GetRevertedTransaction().GetRevertTransaction().GetPostCommitVolumes()
	default:
		return nil
	}
}
