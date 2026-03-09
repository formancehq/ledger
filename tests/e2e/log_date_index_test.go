//go:build e2e

package e2e

import (
	"context"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// createLogBuiltinIndexAction creates a request for creating a log builtin index.
func createLogBuiltinIndexAction(ledger string, index commonpb.LogBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_LogBuiltin{
					LogBuiltin: index,
				},
			},
		},
	}
}

// waitForLogBuiltinIndexReady waits until a log builtin index reaches READY status.
func waitForLogBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.LogBuiltinIndex) {
	Eventually(func(g Gomega) {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		g.Expect(err).To(Succeed())
		g.Expect(info.LogBuiltinIndexes).NotTo(BeNil())

		switch index {
		case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER:
			g.Expect(info.LogBuiltinIndexes.LedgerStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
			g.Expect(info.LogBuiltinIndexes.DateStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		}
	}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
}

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
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort   = testSingleHTTPPort
		grpcPort   = testSingleGRPCPort
		ledgerName = "log-date-idx"
	)

	// nowRef captures the approximate wall-clock time of the server when
	// transactions are created. Log dates are HLC-adjusted proposal dates
	// (close to wall clock), NOT the custom transaction timestamps.
	var nowRef time.Time

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)

		// Create ledger with both ledger log index and date index enabled.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createLedgerAction(ledgerName, nil),
				createLogBuiltinIndexAction(ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER),
				createLogBuiltinIndexAction(ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
			},
		})
		Expect(err).To(Succeed())

		// Create 3 transactions. Log dates will be close to wall-clock time.
		nowRef = time.Now()

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "alice", big.NewInt(100), "USD")}, nil),
				createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "bob", big.NewInt(200), "USD")}, nil),
				createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "carol", big.NewInt(300), "USD")}, nil),
			},
		})
		Expect(err).To(Succeed())

		// Wait for both indexes to be ready.
		waitForLogBuiltinIndexReady(ctx, client, ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER)
		waitForLogBuiltinIndexReady(ctx, client, ledgerName, commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)
	})

	It("Should show log date index as READY in GetLedger", func() {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
		Expect(err).To(Succeed())
		Expect(info.LogBuiltinIndexes).NotTo(BeNil())
		Expect(info.LogBuiltinIndexes.Date).To(BeTrue())
		Expect(info.LogBuiltinIndexes.DateStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
	})

	It("Should list all logs without date filter", func() {
		// List all logs for the ledger (no date filter).
		ledgerFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Ledger{
				Ledger: &commonpb.LedgerCondition{
					Cond: &commonpb.StringCondition{
						Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledgerName},
					},
				},
			},
		}

		Eventually(func(g Gomega) {
			stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
				PageSize: 100,
				Filter:   ledgerFilter,
			})
			g.Expect(err).To(Succeed())

			logs := collectLogs(stream)
			// We expect at least the 3 transaction logs + 2 index creation logs = 5 logs.
			// There may also be the create ledger log, so check >= 5.
			g.Expect(len(logs)).To(BeNumerically(">=", 5))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should filter logs by date range returning logs in the range", func() {
		// Log dates are close to wall clock. Use a wide range around nowRef to
		// capture all logs, then verify the filter actually works by also testing
		// a range that excludes them.
		startTs := uint64(nowRef.Add(-1 * time.Minute).UnixMicro())
		endTs := uint64(nowRef.Add(5 * time.Minute).UnixMicro())

		dateFilter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{
					Filters: []*commonpb.QueryFilter{
						{
							Filter: &commonpb.QueryFilter_Ledger{
								Ledger: &commonpb.LedgerCondition{
									Cond: &commonpb.StringCondition{
										Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledgerName},
									},
								},
							},
						},
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
					},
				},
			},
		}

		Eventually(func(g Gomega) {
			stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
				PageSize: 100,
				Filter:   dateFilter,
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
			Filter: &commonpb.QueryFilter_And{
				And: &commonpb.AndFilter{
					Filters: []*commonpb.QueryFilter{
						{
							Filter: &commonpb.QueryFilter_Ledger{
								Ledger: &commonpb.LedgerCondition{
									Cond: &commonpb.StringCondition{
										Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledgerName},
									},
								},
							},
						},
						{
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
						},
					},
				},
			},
		}

		stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
			PageSize: 100,
			Filter:   dateFilter,
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
							Filter: &commonpb.QueryFilter_Ledger{
								Ledger: &commonpb.LedgerCondition{
									Cond: &commonpb.StringCondition{
										Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledgerName},
									},
								},
							},
						},
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
			stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
				PageSize: 100,
				Filter:   combinedFilter,
			})
			g.Expect(err).To(Succeed())

			logs := collectLogs(stream)
			// After log ID 1 with date in range: should get at least 1 log.
			g.Expect(len(logs)).To(BeNumerically(">=", 1))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
