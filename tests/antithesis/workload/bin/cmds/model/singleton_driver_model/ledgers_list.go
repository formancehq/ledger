package main

import (
	"context"
	"slices"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runLedgersList checks ListLedgers against the model: the fleet is created at
// setup and never deleted, so a linearizable listing serves exactly the fleet's
// ordered window for the page it was asked for, and each entry's chart and
// metadata are the ones some candidate base holds — the same comparison
// GetLedger is held to, over the whole page at once.
func runLedgersList(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	requestedPageSize, pageSize := queryPageSize()
	noteClampedPageSize(requestedPageSize, pageSize)
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	// The ledger cursor is the ledger name itself, so a fleet name resumes at a
	// real boundary and a name outside it exercises the skip on a key the
	// listing never holds.
	var cursor string
	if oneIn(2) {
		cursor = random.RandomChoice(c.ledgerNames)
		if oneIn(4) {
			cursor = "no-such-ledger"
		}
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListLedgers(readCtx, &servicepb.ListLedgersRequest{
		Options: &commonpb.ListOptions{
			PageSize: uint32(requestedPageSize),
			Cursor:   cursor,
			Reverse:  reverse,
		},
	})

	var (
		infos []*commonpb.LedgerInfo
		next  string
	)

	if err == nil {
		infos, err = drainStream(stream)
	}

	if err == nil {
		next = nextCursorOf(stream)
	}

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListLedgers returned unexpected error", internal.Details{
			"error": err.Error(),
		})

		return
	}

	served := make(map[string]*commonpb.LedgerInfo, len(infos))
	names := make([]string, 0, len(infos))

	for _, info := range infos {
		name := info.GetName()
		if _, dup := served[name]; dup {
			assert.Unreachable("singleton_driver_model: ledger listing repeated a ledger", internal.Details{"ledger": name})

			return
		}

		served[name] = info
		names = append(names, name)
	}

	want, more := ledgerWindow(c.ledgerNames, cursor, pageSize, reverse)
	if !slices.Equal(names, want) {
		assert.Unreachable("singleton_driver_model: ledger listing is not the fleet's window", internal.Details{
			"served":   strings.Join(names, ","),
			"want":     strings.Join(want, ","),
			"cursor":   cursor,
			"pageSize": pageSize,
			"reverse":  reverse,
		})

		return
	}

	if !nextCursorLegal(next, more, lastLedgerKey(names), len(names), pageSize) {
		assert.Unreachable("singleton_driver_model: ledger listing published the wrong resume token", internal.Details{
			"nextCursor": next,
			"served":     strings.Join(names, ","),
			"cursor":     cursor,
			"pageSize":   pageSize,
			"reverse":    reverse,
		})

		return
	}

	if !c.matchesModel(maxTicket, "LEDGERLIST", func(base oracle.GlobalState) bool {
		for name, info := range served {
			ls := base.Ledger(name)
			if !chartMatches(ls, info.GetAccountTypes()) || !ledgerMetaMatches(ls, info.GetMetadata()) {
				return false
			}
		}

		return true
	}) {
		details := internal.Details{"listed": len(served)}
		for name, info := range served {
			details["serverMeta:"+name] = renderMetaMap(info.GetMetadata())
			details["modelMeta:"+name] = c.modelLedgerMetaDump(name)
			details["serverChart:"+name] = renderChart(info.GetAccountTypes())
			details["modelChart:"+name] = c.modelChartDump(name)
		}

		assert.Unreachable("singleton_driver_model: ledger listing outside model", details)

		return
	}

	// Coverage: a listing named the fleet's window with the model's charts and
	// metadata.
	assert.Reachable("singleton_driver_model: ledger listing validated", internal.Details{"count": len(served)})

	if more == cursorRequired {
		// Coverage: a listing the page size cut short handed back a resume token.
		assert.Reachable("singleton_driver_model: ledger listing page truncated", internal.Details{"count": len(served)})
	}
}

// ledgerWindow is the page ListLedgers must serve: the fleet in name order —
// reversed when reverse — past the exclusive cursor and capped at pageSize,
// with the verdict on whether a further ledger is waiting. The fleet is created
// at setup and never deleted, so the window is exact rather than bounded.
func ledgerWindow(fleet []string, cursor string, pageSize int, reverse bool) ([]string, cursorMore) {
	window := slices.Sorted(slices.Values(fleet))

	if reverse {
		slices.Reverse(window)
	}

	if cursor != "" {
		kept := window[:0]
		for _, name := range window {
			if reverse && name >= cursor || !reverse && name <= cursor {
				continue
			}

			kept = append(kept, name)
		}

		window = kept
	}

	if len(window) > pageSize {
		return window[:pageSize], cursorRequired
	}

	return window, cursorForbidden
}

// lastLedgerKey is the cursor a ledger listing implies: the name of its last
// row, empty for a page that showed none.
func lastLedgerKey(names []string) string {
	if len(names) == 0 {
		return ""
	}

	return names[len(names)-1]
}
