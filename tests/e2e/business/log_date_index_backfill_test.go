//go:build e2e

package business

import (
	"errors"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// listLedgerLogIDs drains one ListLogs page into per-ledger log ids, reporting
// a transport error through g so a polling caller retries instead of failing.
func listLedgerLogIDs(g Gomega, ledger string, filter *commonpb.QueryFilter) []uint64 {
	stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: 100,
			Filter:   filter,
		},
	})
	g.Expect(err).To(Succeed())

	var ids []uint64

	for {
		l, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}

		g.Expect(recvErr).To(Succeed())

		ids = append(ids, ledgerLogID(l))
	}

	return ids
}

// The log-date index is opt-in, so creating it on a ledger that already has
// logs schedules a backfill over that history. Every ledger log carries a date,
// and the unfiltered ListLogs universe holds every one of them, so a date range
// covering all of time must return exactly the unfiltered listing — whichever
// side of the CreateIndex log each entry landed on, and whatever its payload
// kind is.
var _ = Describe("Log date index backfill", Ordered, func() {
	const ledgerName = "log-date-backfill"

	var beforeIndex, afterIndex []uint64

	listLogIDs := func(g Gomega, filter *commonpb.QueryFilter) []uint64 {
		return listLedgerLogIDs(g, ledgerName, filter)
	}

	BeforeAll(func() {
		// History with no log-date index yet: schema logs and a data log, so the
		// backfill has both kinds to replay.
		resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerAction(ledgerName, nil),
			actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier", commonpb.MetadataType_METADATA_TYPE_STRING),
			actions.AddAccountTypeAction(ledgerName, "customer", "customer:{id}"),
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "customer:alice", big.NewInt(100), "USD")}, nil),
		))
		Expect(err).To(Succeed())

		// CreateLedger carries no per-ledger log id and is not part of the
		// ledger's log listing, so it is not one of the rows under test.
		for _, l := range resp.GetLogs() {
			if id := ledgerLogID(l); id != 0 {
				beforeIndex = append(beforeIndex, id)
			}
		}

		Expect(beforeIndex).To(HaveLen(3), "the pre-index history must hold both schema logs and the data log")

		// Turn the index on: its own log, then the backfill over the history.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateLogBuiltinIndexAction(ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)))
		Expect(err).To(Succeed())

		Expect(actions.WaitForLogBuiltinIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)).To(Succeed())

		// Post-index logs, of both kinds again: these take the live fold.
		resp, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.AddAccountTypeAction(ledgerName, "merchant", "merchant:{id}"),
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "merchant:bob", big.NewInt(200), "USD")}, nil),
		))
		Expect(err).To(Succeed())

		for _, l := range resp.GetLogs() {
			if id := ledgerLogID(l); id != 0 {
				afterIndex = append(afterIndex, id)
			}
		}

		Expect(afterIndex).To(HaveLen(2))
	})

	It("Should serve every log of the ledger through an all-covering date range", func() {
		// Log dates are HLC-adjusted proposal dates in microseconds, so a
		// lower bound of 1 admits every one of them.
		everySince := uint64(1)
		allDates := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond:  &commonpb.UintCondition{Min: &everySince},
				},
			},
		}

		Eventually(func(g Gomega) {
			unfiltered := listLogIDs(g, nil)
			g.Expect(unfiltered).To(ContainElements(beforeIndex), "the unfiltered universe holds the pre-index history")
			g.Expect(unfiltered).To(ContainElements(afterIndex))

			g.Expect(listLogIDs(g, allDates)).To(ConsistOf(unfiltered),
				"a date range covering all of time must return the unfiltered listing")
		}).Within(20 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	// An index declared while its ledger is still born-empty — no data log yet
	// — has no entity history to replay, but the ledger can already carry
	// config-mutation logs, and the CreateIndex log is one itself. Their dates
	// belong in the index like any other log's.
	It("Should serve the logs of a ledger whose index was declared at birth", func() {
		const bornEmpty = "log-date-born-empty"

		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerAction(bornEmpty, nil),
			actions.SetMetadataFieldTypeAction(bornEmpty, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier", commonpb.MetadataType_METADATA_TYPE_STRING),
			actions.CreateLogBuiltinIndexAction(bornEmpty, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		))
		Expect(err).To(Succeed())

		Expect(actions.WaitForLogBuiltinIndexReady(sharedCtx, sharedClient, bornEmpty, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)).To(Succeed())

		everSince := uint64(1)
		allDates := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond:  &commonpb.UintCondition{Min: &everSince},
				},
			},
		}

		Eventually(func(g Gomega) {
			unfiltered := listLedgerLogIDs(g, bornEmpty, nil)
			g.Expect(unfiltered).To(HaveLen(2), "the schema declaration and the index creation are the ledger's logs")

			g.Expect(listLedgerLogIDs(g, bornEmpty, allDates)).To(ConsistOf(unfiltered),
				"a date range covering all of time must return the unfiltered listing")
		}).Within(20 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should return nothing for the complement of a date range every log satisfies", func() {
		// The complement is served as the log universe minus the date-index
		// matches, so every log satisfying the range is the same statement as
		// its complement being empty — and it holds only if the index carries
		// every log.
		everSince := uint64(1)
		everyLogMatches := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_LogBuiltinUint{
					LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
						Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
						Cond:  &commonpb.UintCondition{Min: &everSince},
					},
				},
			}}},
		}

		Consistently(func(g Gomega) {
			g.Expect(listLogIDs(g, everyLogMatches)).To(BeEmpty(),
				"every log's date is at or above the bound, so its complement is empty")
		}).Within(3 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
	})
})
