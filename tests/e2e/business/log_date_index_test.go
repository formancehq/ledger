//go:build e2e

package business

import (
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// collectLogs drains a ListLogs gRPC stream into a slice.
func collectLogs(stream servicepb.BucketService_ListLogsClient) []*commonpb.Log {
	var logs []*commonpb.Log

	for {
		log, err := stream.Recv()
		if err == io.EOF {
			break
		}

		Expect(err).To(Succeed())

		logs = append(logs, log)
	}

	return logs
}

var _ = Describe("Log date index", Ordered, func() {
	const ledgerName = "log-date-idx"

	// nowRef captures the approximate wall-clock time of the server when
	// transactions are created. Log dates are HLC-adjusted proposal dates
	// (close to wall clock), NOT the custom transaction timestamps.
	var nowRef time.Time

	BeforeAll(func() {
		// Create ledger with the date index enabled.
		// The per-ledger log index is always-on (no explicit CreateIndex needed).
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil),
			actions.CreateLogBuiltinIndexAction(ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)))
		Expect(err).To(Succeed())

		// Create 3 transactions. Log dates will be close to wall-clock time.
		nowRef = time.Now()

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "alice", big.NewInt(100), "USD")}, nil),
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "bob", big.NewInt(200), "USD")}, nil),
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "carol", big.NewInt(300), "USD")}, nil)))
		Expect(err).To(Succeed())

		// Wait for the date index to be ready.
		Expect(actions.WaitForLogBuiltinIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)).To(Succeed())
	})

	It("Should show log date index as READY in ListIndexes", func() {
		indexes, err := listLedgerIndexes(sharedCtx, sharedClient, ledgerName)
		Expect(err).To(Succeed())
		idx := findLogBuiltinIndex(indexes, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
		Expect(idx).NotTo(BeNil())
		Expect(idx.GetBuildStatus()).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
	})

	It("Should list all logs without date filter", func() {
		// List all logs for the ledger (no date filter).
		Eventually(func(g Gomega) {
			stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{
				Ledger: ledgerName,
				Options: &commonpb.ListOptions{
					PageSize: 100,
				},
			})
			g.Expect(err).To(Succeed())

			logs := collectLogs(stream)
			// Expected logs: 1 create_ledger + 2 create_index + 3 transactions = 6.
			// IndexReady updates are no longer orders and produce no log entries.
			// The log ledger index is populated incrementally: processLogs indexes
			// logs arriving after the index is created, and the backfill task
			// indexes historical logs. At minimum 4 logs are immediately visible
			// (those processed after the CreateIndex log).
			g.Expect(len(logs)).To(BeNumerically(">=", 4))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should filter logs by date range returning logs in the range", func() {
		// Log dates are close to wall clock. Use a wide range around nowRef to
		// capture all logs, then verify the filter actually works by also testing
		// a range that excludes them.
		startTs := uint64(nowRef.Add(-1 * time.Minute).UnixMicro())
		endTs := uint64(nowRef.Add(5 * time.Minute).UnixMicro())

		dateFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond: &commonpb.UintCondition{
						Min:          &startTs,
						Max:          &endTs,
						MaxExclusive: true,
					},
				},
			},
		}

		Eventually(func(g Gomega) {
			stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{
				Ledger: ledgerName,
				Options: &commonpb.ListOptions{
					PageSize: 100,
					Filter:   dateFilter,
				},
			})
			g.Expect(err).To(Succeed())

			logs := collectLogs(stream)
			// All logs (create ledger + 2 index creations + 3 transactions = 6) should be in range.
			g.Expect(len(logs)).To(BeNumerically(">=", 3))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should return empty results for a date range with no logs", func() {
		// Filter: date range far in the future.
		futureTs := uint64(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro())
		futureEndTs := uint64(time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro())

		dateFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogBuiltinUint{
				LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
					Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
					Cond: &commonpb.UintCondition{
						Min:          &futureTs,
						Max:          &futureEndTs,
						MaxExclusive: true,
					},
				},
			},
		}

		stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{
			Ledger: ledgerName,
			Options: &commonpb.ListOptions{
				PageSize: 100,
				Filter:   dateFilter,
			},
		})
		Expect(err).To(Succeed())

		logs := collectLogs(stream)
		Expect(logs).To(BeEmpty())
	})

	It("Should combine date filter with log ID filter using AND", func() {
		// Combine a date range around now with log_id > 1.
		startTs := uint64(nowRef.Add(-1 * time.Minute).UnixMicro())
		endTs := uint64(nowRef.Add(5 * time.Minute).UnixMicro())
		afterLogID := uint64(1)

		combinedFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{
					Filters: []*commonpb.QueryFilter{
						{
							Filter: &commonpb.QueryFilter_LogBuiltinUint{
								LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
									Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
									Cond: &commonpb.UintCondition{
										Min:          &startTs,
										Max:          &endTs,
										MaxExclusive: true,
									},
								},
							},
						},
						{
							Filter: &commonpb.QueryFilter_LogId{
								LogId: &commonpb.LogIdCondition{
									Cond: &commonpb.UintCondition{
										Min:          &afterLogID,
										MinExclusive: true,
									},
								},
							},
						},
					},
				},
			},
		}

		Eventually(func(g Gomega) {
			stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{
				Ledger: ledgerName,
				Options: &commonpb.ListOptions{
					PageSize: 100,
					Filter:   combinedFilter,
				},
			})
			g.Expect(err).To(Succeed())

			logs := collectLogs(stream)
			// After log ID 1 with date in range: should get at least 1 log.
			g.Expect(len(logs)).To(BeNumerically(">=", 1))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
