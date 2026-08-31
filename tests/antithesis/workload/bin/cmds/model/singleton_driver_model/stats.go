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

// statsUsage is the subset of the usagestore counters the model derives exactly.
// Each only ever grows along the audit chain, so the fully-folded value bounds a
// served one above however far the projection lags.
type statsUsage struct {
	postings   uint64
	references uint64
	reverts    uint64
}

// modelUsage counts what the usagebuilder counts (usagestore/keys.go): postings
// summed over every created and revert transaction, creates carrying a
// reference, and revert orders.
func modelUsage(ls oracle.LedgerState) statsUsage {
	var u statsUsage

	txs := ls.Txs()
	for i := range txs.Len() {
		rec := txs.Get(i)

		u.postings += uint64(len(rec.Postings()))

		if rec.Reference() != "" {
			u.references++
		}

		if rec.RevertsTransaction() != 0 {
			u.reverts++
		}
	}

	return u
}

// runLedgerStats issues a linearizable GetLedgerStats — usually on a fleet
// ledger, sometimes on an absent one — and checks the fields the model owns.
//
// The response stitches two stores read at independent fold points
// (DefaultController.GetLedgerStats): the transaction and log counts come from
// the ledger's boundaries on one main-store handle, then the usage counters come
// from a usagestore snapshot opened afterwards. The boundary pair is the only
// single consistent observation in the message, so it is the only part matched
// exactly; the usage counters get the relations that hold whatever prefix of the
// chain their projection has folded. The request carries no MinLogSequence, so
// linearizable routing — a ReadIndex barrier plus WaitForApplied in
// RoutedController.readCtrl — is what puts the boundary read at or beyond every
// bulk the model has observed committed.
func runLedgerStats(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stats, err := client.GetLedgerStats(readCtx, &servicepb.GetLedgerStatsRequest{Ledger: ledger})

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if absent && status.Code(err) == codes.NotFound {
			// Coverage: stats for a ledger outside the fleet must resolve NotFound.
			assert.Reachable("singleton_driver_model: ledger stats on an absent ledger returned NotFound", internal.Details{"ledger": ledger})

			return
		}

		assert.Unreachable("singleton_driver_model: GetLedgerStats returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"error":  err.Error(),
		})

		return
	}

	if absent {
		// The fleet never grows, so stats for a name outside it describe a ledger
		// the server holds but the model never created.
		assert.Unreachable("singleton_driver_model: ledger stats served a ledger outside the fleet", internal.Details{"ledger": ledger})

		return
	}

	c.validateLedgerStats(maxTicket, ledger, stats)
}

// validateLedgerStats checks one stats snapshot against the model's candidate
// bases. Boundaries and usage counters are matched separately because they are
// read from separate stores at separate fold points — requiring one base to
// explain both would reject a legal response the moment the usage projection
// lags the boundary handle, which it does by design.
func (c *Checker) validateLedgerStats(maxTicket uint64, ledger string, stats *commonpb.LedgerStats) {
	// The workload sends neither a Script nor a NumscriptReference, so no create
	// can count as a numscript execution.
	if stats.GetNumscriptExecutionCount() != 0 {
		assert.Unreachable("singleton_driver_model: ledger stats counted a numscript execution", internal.Details{
			"ledger": ledger,
			"count":  stats.GetNumscriptExecutionCount(),
		})

		return
	}

	txCount := stats.GetTransactionCount()
	logCount := stats.GetLogCount()

	// TransactionCount and LogCount are next_id-1 off one boundaries read, so the
	// pair is exactly some base's — neither field can come from a different state
	// than the other.
	if !c.matchesModel(maxTicket, "STATSBOUNDS", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)

		return txCount == uint64(ls.Txs().Len()) && logCount == uint64(len(ls.LogDates()))
	}) {
		assert.Unreachable("singleton_driver_model: ledger stats boundaries outside model", internal.Details{
			"ledger":         ledger,
			"serverTxCount":  txCount,
			"serverLogCount": logCount,
			"modelTxCount":   c.modelTxCount(ledger),
			"modelLogCount":  c.modelLogCount(ledger),
			"foldDiag":       c.foldDiag(maxTicket),
		})

		return
	}

	served := statsUsage{
		postings:   stats.GetPostingCount(),
		references: stats.GetReferenceCount(),
		reverts:    stats.GetRevertCount(),
	}

	// The usagestore snapshot is taken after the boundary handle and its builder
	// folds with no barrier, so which prefix it holds is unknown. Every counter
	// here only grows along the chain, so the fully-folded value bounds it above —
	// and a base satisfying the bound is exactly that, since the loosest bound any
	// base offers is the fully-folded one. An over-count (a double-applied bulk)
	// exceeds every base and is caught whatever the lag.
	if !c.matchesModel(maxTicket, "STATSUSAGE", func(base oracle.GlobalState) bool {
		m := modelUsage(base.Ledger(ledger))

		return served.postings <= m.postings &&
			served.references <= m.references &&
			served.reverts <= m.reverts
	}) {
		modelMax := c.modelUsageCommitted(ledger)

		assert.Unreachable("singleton_driver_model: ledger stats counter above the model", internal.Details{
			"ledger":           ledger,
			"serverPostings":   served.postings,
			"serverReferences": served.references,
			"serverReverts":    served.reverts,
			"modelPostings":    modelMax.postings,
			"modelReferences":  modelMax.references,
			"modelReverts":     modelMax.reverts,
			"foldDiag":         c.foldDiag(maxTicket),
		})

		return
	}

	// Coverage: a stats read matched the model's boundaries and stayed within the
	// counter bounds.
	assert.Reachable("singleton_driver_model: ledger stats validated", internal.Details{
		"ledger":   ledger,
		"txCount":  txCount,
		"logCount": logCount,
	})
}

// modelTxCount is the committed transaction count for finding diagnostics.
// Acquires c.mu.
func (c *Checker) modelTxCount(ledger string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.modelState.Ledger(ledger).Txs().Len()
}

// modelLogCount is the committed log count for finding diagnostics. Acquires
// c.mu.
func (c *Checker) modelLogCount(ledger string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.modelState.Ledger(ledger).LogDates())
}

// modelUsageCommitted is the counter set on the committed state — the floor of
// the per-base bounds — for finding diagnostics. Acquires c.mu.
func (c *Checker) modelUsageCommitted(ledger string) statsUsage {
	c.mu.Lock()
	defer c.mu.Unlock()

	return modelUsage(c.modelState.Ledger(ledger))
}
