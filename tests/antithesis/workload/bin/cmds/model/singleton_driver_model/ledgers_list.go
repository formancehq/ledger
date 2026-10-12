package main

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runLedgersList checks ListLedgers against the model: a linearizable listing
// serves exactly the live fleet's ordered window for the page it was asked for,
// and each entry is the whole LedgerInfo some candidate base holds — the same
// comparison GetLedger is held to, over the whole page at once.
func runLedgersList(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	requestedPageSize, pageSize := queryPageSize()
	noteClampedPageSize(requestedPageSize, pageSize)
	reverse := random.RandomChoice([]uint8{0, 1}) == 1
	filter, filtered := rollLedgerListFilter()

	// The ledger cursor is the ledger name itself, so a fleet name resumes at a
	// real boundary and a name outside it exercises the skip on a key the
	// listing never holds.
	var (
		cursor        string
		resumedAtGone bool
	)

	if oneIn(2) {
		cursor = random.RandomChoice(c.ledgerNamesSnapshot())

		// A tombstoned name is the boundary the skip is most likely to get
		// wrong: it was a row, and the page must resume past it without it.
		if deleted := c.deletedLedgerNames(); len(deleted) > 0 && oneIn(3) {
			cursor = random.RandomChoice(deleted)
			resumedAtGone = true
		}

		if oneIn(4) {
			cursor = "no-such-ledger"
			resumedAtGone = false
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
			Cursor:   pageToken(cursor),
			Reverse:  reverse,
			Filter:   filter,
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

	if err != nil && (internal.IsTransient(err) || isShutdownError(err)) {
		return
	}

	if filtered {
		handleLedgerListFilterOutcome(err, infos)

		return
	}

	if err != nil {
		assert.Unreachable("singleton_driver_model: ListLedgers returned unexpected error", internal.Details{
			"error": err.Error(),
		})

		return
	}

	served := make(map[string]*commonpb.LedgerInfo, len(infos))
	names := make([]string, 0, len(infos))
	ids := make(map[uint32]string, len(infos))

	for _, info := range infos {
		name := info.GetName()
		if _, dup := served[name]; dup {
			assert.Unreachable("singleton_driver_model: ledger listing repeated a ledger", internal.Details{"ledger": name})

			return
		}

		if violation := ledgerInfoStructureViolation(info); violation != "" {
			assert.Unreachable("singleton_driver_model: listed ledger is malformed", internal.Details{
				"ledger":    name,
				"violation": violation,
			})

			return
		}

		if owner, taken := ids[info.GetId()]; taken {
			assert.Unreachable("singleton_driver_model: ledger listing reused an id", internal.Details{
				"id":     info.GetId(),
				"ledger": name,
				"other":  owner,
			})

			return
		}

		ids[info.GetId()] = name
		served[name] = info
		names = append(names, name)
	}

	// A deleted ledger leaves the listing, so the window is the live fleet of
	// whichever base explains the page.
	windowMatches := func(base oracle.GlobalState) bool {
		return ledgerWindowMatches(base, names, cursor, pageSize, reverse, next)
	}

	// One base must explain the window and every row at once; the window-only
	// search below only picks which finding to report.
	pageMatches := c.matchesModel(maxTicket, "LEDGERLIST", func(base oracle.GlobalState) bool {
		return ledgerListingMatches(base, names, served, cursor, pageSize, reverse, next)
	})

	if !pageMatches && !c.matchesModel(maxTicket, "LEDGERWINDOW", windowMatches) {
		committed, _ := ledgerWindow(c.modelLiveLedgers(), cursor, pageSize, reverse)
		assert.Unreachable("singleton_driver_model: ledger listing is not the fleet's window", internal.Details{
			"served":     strings.Join(names, ","),
			"want":       strings.Join(committed, ","),
			"cursor":     cursor,
			"nextCursor": next,
			"pageSize":   pageSize,
			"reverse":    reverse,
		})

		return
	}

	if !pageMatches {
		details := internal.Details{"listed": len(served)}
		for name, info := range served {
			details["serverMeta:"+name] = renderMetaMap(info.GetMetadata())
			details["modelMeta:"+name] = c.modelLedgerMetaDump(name)
			details["serverChart:"+name] = renderChart(info.GetAccountTypes())
			details["modelChart:"+name] = c.modelChartDump(name)
			details["serverSchema:"+name] = renderSchema(info.GetMetadataSchema())
			details["modelSchema:"+name] = c.modelSchemaDump(name)
			details["serverMode:"+name] = info.GetMode().String()
			details["serverEnforcement:"+name] = info.GetDefaultEnforcementMode().String()
		}

		assert.Unreachable("singleton_driver_model: ledger listing outside model", details)

		return
	}

	if diverged := c.ledgerIdentityViolation(infos); diverged != "" {
		assert.Unreachable("singleton_driver_model: listed ledger identity is not the one creation reported", internal.Details{
			"ledger":    diverged,
			"servedId":  served[diverged].GetId(),
			"createdAt": served[diverged].GetCreatedAt().String(),
		})

		return
	}

	// Coverage: a listing named the fleet's window with the model's whole
	// LedgerInfo for every row.
	assert.Reachable("singleton_driver_model: ledger listing validated", internal.Details{"count": len(served)})

	if next != "" {
		// Coverage: a listing the page size cut short handed back a resume token.
		assert.Reachable("singleton_driver_model: ledger listing page truncated", internal.Details{"count": len(served)})
	}

	if resumedAtGone {
		// Coverage: a listing resumed past a ledger that has left the fleet.
		assert.Reachable("singleton_driver_model: ledger listing resumed at a deleted ledger", internal.Details{"cursor": cursor})
	}
}

// rollLedgerListFilter returns a filter ListLedgers must refuse, one read in
// four. The endpoint declares no filter support (ListOptionsSupport), so any
// filter is InvalidArgument — never a silently unfiltered page, which would look
// to a client like a fleet that happens to match. The rate is high because the
// listing is one arm of one slot of the read mix: a rarer roll leaves the
// rejection unexercised over a whole local run.
func rollLedgerListFilter() (*commonpb.QueryFilter, bool) {
	if !oneIn(4) {
		return nil, false
	}

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: poolAddress()},
	}}}, true
}

// handleLedgerListFilterOutcome validates the answer to a filter the endpoint
// does not implement: InvalidArgument, and no page.
func handleLedgerListFilterOutcome(err error, infos []*commonpb.LedgerInfo) {
	if status.Code(err) == codes.InvalidArgument {
		// Coverage: the listing refused a filter it does not implement.
		assert.Reachable("singleton_driver_model: ledger listing rejected an unsupported filter", nil)

		return
	}

	assert.Unreachable("singleton_driver_model: ledger listing accepted an unsupported filter", internal.Details{
		"error": fmt.Sprint(err),
		"rows":  len(infos),
	})
}

// ledgerWindow is the page ListLedgers must serve: the base's live ledgers in
// name order — reversed when reverse — past the exclusive cursor and capped at
// pageSize, with the verdict on whether a further ledger is waiting.
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

// modelLiveLedgers is LiveLedgers on the committed state, for a finding's
// diagnostics. Acquires c.mu.
func (c *Checker) modelLiveLedgers() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.modelState.LiveLedgers()
}

// deletedLedgerNames is the fleet's tombstoned names on the committed state.
// Acquires c.mu.
func (c *Checker) deletedLedgerNames() []string {
	c.mu.Lock()
	state := c.modelState
	c.mu.Unlock()

	_, deleted := partitionLifecycleLedgers(state, c.ledgerNamesSnapshot())

	return deleted
}

// ledgerWindowMatches reports whether names and next are the window base's live
// fleet gives this cursor, page size and direction.
func ledgerWindowMatches(base oracle.GlobalState, names []string, cursor string, pageSize int, reverse bool, next string) bool {
	want, more := ledgerWindow(base.LiveLedgers(), cursor, pageSize, reverse)

	return slices.Equal(names, want) && nextCursorLegal(next, more, lastLedgerKey(names), len(names), pageSize)
}

// ledgerListingMatches reports whether base alone explains the whole page: its
// window and every row's LedgerInfo.
func ledgerListingMatches(base oracle.GlobalState, names []string, served map[string]*commonpb.LedgerInfo, cursor string, pageSize int, reverse bool, next string) bool {
	if !ledgerWindowMatches(base, names, cursor, pageSize, reverse, next) {
		return false
	}

	for _, info := range served {
		if !ledgerInfoMatches(base, info) {
			return false
		}
	}

	return true
}
