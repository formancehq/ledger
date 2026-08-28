package main

import (
	"context"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runLedgersList checks ListLedgers against the one fact the model owns: the
// fleet is created at setup and never deleted, and every backup a restore
// cycle can install postdates setup — so a linearizable listing must contain
// every fleet ledger, whatever else the cluster holds.
func runLedgersList(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListLedgers(readCtx, &servicepb.ListLedgersRequest{})

	var names map[string]bool

	if err == nil {
		infos, drainErr := drainStream(stream)
		err = drainErr

		names = make(map[string]bool, len(infos))
		for _, info := range infos {
			names[info.GetName()] = true
		}
	}

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		assert.Unreachable("singleton_driver_model: ListLedgers returned unexpected error", internal.Details{
			"error": err.Error(),
		})

		return
	}

	var missing []string
	for _, name := range c.ledgerNames {
		if !names[name] {
			missing = append(missing, name)
		}
	}

	if len(missing) == 0 {
		// Coverage: a listing answered with the whole fleet present.
		assert.Reachable("singleton_driver_model: ledger listing covered the fleet", internal.Details{"count": len(names)})

		return
	}

	assert.Unreachable("singleton_driver_model: ledger listing missing fleet ledgers", internal.Details{
		"missing": strings.Join(missing, ","),
		"listed":  len(names),
	})
}
