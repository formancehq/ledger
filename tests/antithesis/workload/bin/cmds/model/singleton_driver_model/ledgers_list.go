package main

import (
	"context"
	"slices"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runLedgersList checks ListLedgers against the model: the fleet is created at
// setup and never deleted, so a linearizable listing names every fleet ledger
// and no other, and each entry's chart and metadata are the ones some
// candidate base holds — the same comparison GetLedger is held to, over the
// whole fleet at once.
func runLedgersList(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListLedgers(readCtx, &servicepb.ListLedgersRequest{})

	var infos []*commonpb.LedgerInfo
	if err == nil {
		infos, err = drainStream(stream)
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
	var outside []string

	for _, info := range infos {
		name := info.GetName()
		if _, dup := served[name]; dup {
			assert.Unreachable("singleton_driver_model: ledger listing repeated a ledger", internal.Details{"ledger": name})

			return
		}

		served[name] = info

		if !slices.Contains(c.ledgerNames, name) {
			outside = append(outside, name)
		}
	}

	var missing []string
	for _, name := range c.ledgerNames {
		if _, ok := served[name]; !ok {
			missing = append(missing, name)
		}
	}

	if len(missing) > 0 || len(outside) > 0 {
		assert.Unreachable("singleton_driver_model: ledger listing is not the fleet", internal.Details{
			"missing": strings.Join(missing, ","),
			"outside": strings.Join(outside, ","),
			"listed":  len(served),
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

	// Coverage: a listing named the whole fleet with the model's charts and
	// metadata.
	assert.Reachable("singleton_driver_model: ledger listing validated", internal.Details{"count": len(served)})
}
