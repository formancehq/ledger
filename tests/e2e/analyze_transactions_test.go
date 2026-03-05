//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// analyzeTransactions calls the streaming AnalyzeTransactions RPC and returns the final result.
func analyzeTransactions(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.AnalyzeTransactionsRequest) (*servicepb.AnalyzeTransactionsResponse, error) {
	stream, err := client.AnalyzeTransactions(ctx, req)
	if err != nil {
		return nil, err
	}
	var result *servicepb.AnalyzeTransactionsResponse
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if r := event.GetResult(); r != nil {
			result = r
		}
	}
	return result, nil
}

var _ = Describe("AnalyzeTransactions", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("When analyzing an empty ledger", Ordered, func() {
		var ledgerName = "analyze-tx-empty"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should return zero transactions and no patterns", func() {
			resp, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.TotalTransactions).To(BeZero())
			Expect(resp.TotalReverted).To(BeZero())
			Expect(resp.FlowPatterns).To(BeEmpty())
		})
	})

	Context("When analyzing simple transactions (world -> bank:main)", Ordered, func() {
		var ledgerName = "analyze-tx-simple"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank:main", big.NewInt(1000), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank:main", big.NewInt(2000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should discover a single SIMPLE pattern", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.TotalTransactions).To(Equal(uint64(2)))
				g.Expect(resp.FlowPatterns).To(HaveLen(1))
				g.Expect(resp.FlowPatterns[0].Structure).To(Equal(servicepb.PostingStructure_POSTING_STRUCTURE_SIMPLE))
				g.Expect(resp.FlowPatterns[0].TransactionCount).To(Equal(uint64(2)))
			}).Should(Succeed())
		})
	})

	Context("When analyzing multi-destination transactions (fee split)", Ordered, func() {
		var ledgerName = "analyze-tx-multidest"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank:main", big.NewInt(10000), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
						newPosting("bank:main", "users:alice", big.NewInt(990), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should discover a MULTI_DESTINATION pattern", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				var found bool
				for _, fp := range resp.FlowPatterns {
					if fp.Structure == servicepb.PostingStructure_POSTING_STRUCTURE_MULTI_DESTINATION {
						found = true
						break
					}
				}
				g.Expect(found).To(BeTrue(), "expected to find a MULTI_DESTINATION pattern")
			}).Should(Succeed())
		})
	})

	Context("When analyzing transactions with variable addresses (12+ users)", Ordered, func() {
		var ledgerName = "analyze-tx-variable"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund world first, then create 12 user transactions
			requests := make([]*servicepb.Request, 0, 13)
			requests = append(requests, createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "bank:main", big.NewInt(1000000), "USD"),
			}, nil, nil))

			for i := range 12 {
				userID := fmt.Sprintf("%08d-%04d-%04d-%04d-%012d", i+1, 0, 0, 0, i+1)
				requests = append(requests, createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("bank:main", fmt.Sprintf("users:%s:main", userID), big.NewInt(100), "USD"),
				}, nil, nil))
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should normalize variable user addresses in flow signatures", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.TotalTransactions).To(Equal(uint64(13)))

				// The 12 user transactions should be grouped into a single pattern
				var userPattern *servicepb.FlowPattern
				for _, fp := range resp.FlowPatterns {
					if fp.TransactionCount == 12 {
						userPattern = fp
						break
					}
				}
				g.Expect(userPattern).NotTo(BeNil(), "expected a pattern with 12 transactions")
				// The signature should contain a normalized placeholder
				g.Expect(userPattern.Signature).To(ContainSubstring("{"))
			}).Should(Succeed())
		})
	})

	Context("When analyzing transactions with volume stats", Ordered, func() {
		var ledgerName = "analyze-tx-volumes"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank:main", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank:main", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should have coherent volume stats (total = sum of postings)", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.FlowPatterns).To(HaveLen(1))

				vs := resp.FlowPatterns[0].VolumeStats
				g.Expect(vs).To(HaveLen(1))
				g.Expect(vs[0].Asset).To(Equal("USD"))
				g.Expect(vs[0].TotalVolume).To(Equal("400"))
			}).Should(Succeed())
		})
	})

	Context("When analyzing a non-existent ledger", func() {
		It("Should return a NotFound error", func() {
			_, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
				Ledger: "non-existent-tx-ledger",
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})

	Context("When analyzing a realistic ledger with multiple flow types", Ordered, func() {
		var ledgerName = "analyze-tx-realistic"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			requests := make([]*servicepb.Request, 0)

			// Funding flow: world -> bank:main
			requests = append(requests,
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "bank:main", big.NewInt(1000000), "USD"),
				}, nil, nil),
				createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", "bank:main", big.NewInt(500000), "EUR"),
				}, nil, nil),
			)

			// Distribution flow: bank:main -> users + bank:fees (multi-destination)
			for i := range 3 {
				requests = append(requests,
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank:main", fmt.Sprintf("users:user%d", i), big.NewInt(1000), "USD"),
						newPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
					}, nil, nil),
				)
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should discover multiple distinct flow patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeTransactions(ctx, client, &servicepb.AnalyzeTransactionsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.TotalTransactions).To(Equal(uint64(5)))
				// At least 2 patterns: funding (world->bank) and distribution (bank->users+fees)
				g.Expect(len(resp.FlowPatterns)).To(BeNumerically(">=", 2))
			}).Should(Succeed())
		})
	})
})
