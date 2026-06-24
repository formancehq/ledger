//go:build e2e

package cluster

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// numscriptTransfer is a minimal but valid Numscript used by the SaveNumscript /
// DeleteNumscript checks below.
const numscriptTransfer = `
	vars {
		account $from
		account $to
		monetary $amount
	}
	send $amount (
		source = $from allowing unbounded overdraft
		destination = $to
	)
`

// This test reproduces a bug where the FSM processors for prepared-query
// and numscript-library operations look up the ledger via s.GetLedger(name),
// which only checks the in-memory dual-generation cache. After enough Raft
// proposals advance the cache past 2 * rotation-threshold, the LedgerInfo
// is evicted from both generations. The admission layer's extractPreloadNeeds
// does NOT add the LedgerKey to p.Ledgers for these order types, so the
// LedgerInfo is never re-preloaded — and the FSM returns ErrLedgerNotFound
// ("ledger does not exist: <name>") even though the ledger is still present
// in Pebble and visible to ListLedgers.
//
// Apply / PromoteLedger / MirrorIngest / SaveLedgerMetadata / DeleteLedgerMetadata
// all correctly preload the ledger, so they are not affected.
var _ = Describe("Prepared query and numscript work after ledger cache eviction", Ordered, func() {
	const (
		httpPort          = 9260
		grpcPort          = 8260
		rotationThreshold = uint64(10)
		// Enough no-op proposals to push the LedgerInfo past gen1 of the
		// cache (needs > 2 * rotationThreshold beyond the CreateLedger
		// index). 50 is comfortably above that and tolerates whatever
		// startup proposals (cluster config, chapter bootstrap, ...) are
		// applied before the test's CreateLedger.
		barrierCount = 50
		ledgerName   = "preload-rotation"
	)

	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort,
			testserver.WithCacheRotationThreshold(rotationThreshold),
		)

		// 1. Create the ledger — populates LedgerInfo in Pebble (Global +
		//    attributes) and in the FSM's in-memory cache (current gen0).
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		// 2. Advance the Raft commit index past 2 * rotationThreshold using
		//    no-op Barrier proposals. Each Barrier has an empty preload set,
		//    so it does NOT refresh the LedgerInfo in the cache. After two
		//    rotations the entry is dropped from both gen0 and gen1.
		for i := 0; i < barrierCount; i++ {
			_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())
		}

		// Sanity check: the ledger is still in Pebble (the cache eviction
		// only affects in-memory state).
		ledgers, err := actions.ListLedgers(ctx, client)
		Expect(err).To(Succeed())
		Expect(ledgers).To(HaveKey(ledgerName))
	})

	It("CreatePreparedQuery must succeed after the LedgerInfo is evicted from cache", func() {
		_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
			Ledger: ledgerName,

			Query: &commonpb.PreparedQuery{
				Name:   "after-rotation",
				Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
				Filter: actions.AddressPrefixFilter("users:"),
			},
		})
		Expect(err).To(Succeed())
	})

	It("UpdatePreparedQuery must succeed after the LedgerInfo is evicted from cache", func() {
		// Run more barriers to evict the LedgerInfo that was just re-cached
		// by the preceding CreatePreparedQuery preload (PreparedQueries
		// preload alone keeps PreparedQuery in cache, not LedgerInfo —
		// but even without that, this is defence in depth for the test).
		for i := 0; i < barrierCount; i++ {
			_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())
		}

		_, err := client.UpdatePreparedQuery(ctx, &servicepb.UpdatePreparedQueryRequest{
			Ledger: ledgerName,
			Name:   "after-rotation",
			Filter: actions.AddressPrefixFilter("admins:"),
		})
		Expect(err).To(Succeed())
	})

	It("DeletePreparedQuery must succeed after the LedgerInfo is evicted from cache", func() {
		for i := 0; i < barrierCount; i++ {
			_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())
		}

		_, err := client.DeletePreparedQuery(ctx, &servicepb.DeletePreparedQueryRequest{
			Ledger: ledgerName,
			Name:   "after-rotation",
		})
		Expect(err).To(Succeed())
	})

	It("SaveNumscript must succeed after the LedgerInfo is evicted from cache", func() {
		for i := 0; i < barrierCount; i++ {
			_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())
		}

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SaveNumscript{
				SaveNumscript: &servicepb.SaveNumscriptRequest{
					Name:    "transfer",
					Content: numscriptTransfer,
					Version: "1.0.0",
					Ledger:  ledgerName,
				},
			},
		}))
		Expect(err).To(Succeed())
	})

	It("DeleteNumscript must succeed after the LedgerInfo is evicted from cache", func() {
		for i := 0; i < barrierCount; i++ {
			_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())
		}

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_DeleteNumscript{
				DeleteNumscript: &servicepb.DeleteNumscriptRequest{
					Name:   "transfer",
					Ledger: ledgerName,
				},
			},
		}))
		Expect(err).To(Succeed())
	})
})
