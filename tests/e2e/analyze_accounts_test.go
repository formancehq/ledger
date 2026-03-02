//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("AnalyzeAccounts", Ordered, func() {
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
		var ledgerName = "analyze-empty"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should return zero accounts and no patterns", func() {
			resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.TotalAccounts).To(BeZero())
			Expect(resp.Patterns).To(BeEmpty())
		})
	})

	Context("When analyzing a ledger with simple fixed accounts", Ordered, func() {
		var ledgerName = "analyze-fixed"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions that produce fixed accounts: world, bank:main, bank:fees
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bank:main", big.NewInt(1000), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return correct total accounts", func() {
			// Index builder processes logs asynchronously; poll until indexes are up to date.
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// world + bank:main + bank:fees = 3
				g.Expect(resp.TotalAccounts).To(Equal(uint64(3)))
			}).Should(Succeed())
		})

		It("Should discover patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.Patterns).NotTo(BeEmpty())
			}).Should(Succeed())
		})

		It("Should suggest a chart with fixed segments", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.SuggestedChart).NotTo(BeNil())
				g.Expect(resp.SuggestedChart.Segments).NotTo(BeEmpty())

				// Find the "bank" segment in the chart
				var bankSeg *commonpb.ChartSegment
				for _, seg := range resp.SuggestedChart.Segments {
					if seg.FixedValue == "bank" {
						bankSeg = seg
						break
					}
				}
				g.Expect(bankSeg).NotTo(BeNil(), "expected 'bank' segment in chart")
				// bank should have children (main, fees)
				g.Expect(bankSeg.Children).NotTo(BeEmpty())
			}).Should(Succeed())
		})
	})

	Context("When analyzing a ledger with variable (UUID) accounts", Ordered, func() {
		var ledgerName = "analyze-variable"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 15 user accounts with UUID-like IDs to trigger variable detection
			requests := make([]*servicepb.Request, 0, 15)
			for i := range 15 {
				userAddr := fmt.Sprintf("users:%08d-%04d-%04d-%04d-%012d", i, i, i, i, i)
				requests = append(requests, createTransactionAction(ledgerName, []*commonpb.Posting{
					newPosting("world", userAddr, big.NewInt(int64(100+i)), "USD"),
				}, nil, nil))
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should detect variable segments", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// 15 user accounts + world = 16
				g.Expect(resp.TotalAccounts).To(Equal(uint64(16)))

				// Chart should have a "users" segment with a variable child
				g.Expect(resp.SuggestedChart).NotTo(BeNil())
				var usersSeg *commonpb.ChartSegment
				for _, seg := range resp.SuggestedChart.Segments {
					if seg.FixedValue == "users" {
						usersSeg = seg
						break
					}
				}
				g.Expect(usersSeg).NotTo(BeNil(), "expected 'users' segment in chart")
				g.Expect(usersSeg.Children).To(HaveLen(1))
				g.Expect(usersSeg.Children[0].Variable).NotTo(BeNil(), "expected variable child under 'users'")
			}).Should(Succeed())
		})

		It("Should include patterns with variable segments", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				// Find the users pattern
				var usersPattern *servicepb.AccountPattern
				for _, p := range resp.Patterns {
					for _, s := range p.Segments {
						if s.Type == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
							usersPattern = p
							break
						}
					}
					if usersPattern != nil {
						break
					}
				}
				g.Expect(usersPattern).NotTo(BeNil(), "expected a pattern with variable segments")
				g.Expect(usersPattern.AccountCount).To(Equal(uint64(15)))
				g.Expect(usersPattern.Assets).To(ContainElement("USD"))
			}).Should(Succeed())
		})
	})

	Context("When analyzing with a custom threshold", Ordered, func() {
		var ledgerName = "analyze-threshold"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 5 distinct child accounts under "dept:"
			for i := range 5 {
				_, err = client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("dept:%d", 1000+i), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should treat children as fixed with default threshold", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger:            ledgerName,
					VariableThreshold: 0, // default = 10
				})
				g.Expect(err).To(Succeed())

				// 5 children < 10 threshold, so they should be fixed
				g.Expect(resp.SuggestedChart).NotTo(BeNil())
				var deptSeg *commonpb.ChartSegment
				for _, seg := range resp.SuggestedChart.Segments {
					if seg.FixedValue == "dept" {
						deptSeg = seg
						break
					}
				}
				g.Expect(deptSeg).NotTo(BeNil())
				// All children should be fixed (no variable)
				for _, child := range deptSeg.Children {
					g.Expect(child.Variable).To(BeNil(), "expected fixed children with default threshold")
				}
			}).Should(Succeed())
		})

		It("Should treat children as variable with low threshold", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger:            ledgerName,
					VariableThreshold: 3, // 5 children > 3 threshold → variable
				})
				g.Expect(err).To(Succeed())

				g.Expect(resp.SuggestedChart).NotTo(BeNil())
				var deptSeg *commonpb.ChartSegment
				for _, seg := range resp.SuggestedChart.Segments {
					if seg.FixedValue == "dept" {
						deptSeg = seg
						break
					}
				}
				g.Expect(deptSeg).NotTo(BeNil())
				// With threshold=3, 5 children should be classified as variable
				g.Expect(deptSeg.Children).To(HaveLen(1))
				g.Expect(deptSeg.Children[0].Variable).NotTo(BeNil(), "expected variable child with low threshold")
			}).Should(Succeed())
		})
	})

	Context("When analyzing a non-existent ledger", func() {
		It("Should return a NotFound error", func() {
			_, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
				Ledger: "non-existent-ledger",
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})

	Context("When analyzing a ledger with metadata", Ordered, func() {
		var ledgerName = "analyze-metadata"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create accounts and add metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "users:bob", big.NewInt(200), "EUR"),
					}, nil, nil),
					saveAccountMetadataAction(ledgerName, "users:alice", map[string]string{
						"role": "admin",
						"tier": "premium",
					}),
					saveAccountMetadataAction(ledgerName, "users:bob", map[string]string{
						"role": "user",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should include metadata keys in patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				// Collect all metadata keys across all patterns
				allMetadataKeys := make(map[string]bool)
				for _, p := range resp.Patterns {
					for _, k := range p.MetadataKeys {
						allMetadataKeys[k] = true
					}
				}
				g.Expect(allMetadataKeys).To(HaveKey("role"))
			}).Should(Succeed())
		})

		It("Should include multiple assets in patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				// Collect all assets across all patterns
				allAssets := make(map[string]bool)
				for _, p := range resp.Patterns {
					for _, a := range p.Assets {
						allAssets[a] = true
					}
				}
				g.Expect(allAssets).To(HaveKey("USD"))
				g.Expect(allAssets).To(HaveKey("EUR"))
			}).Should(Succeed())
		})
	})

	Context("When analyzing a realistic ledger (world + users + bank)", Ordered, func() {
		var ledgerName = "analyze-realistic"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Build a realistic account structure:
			// world, bank:main, bank:fees, users:{id}:main, users:{id}:savings
			requests := make([]*servicepb.Request, 0)

			// Fund bank:main from world
			requests = append(requests, createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("world", "bank:main", big.NewInt(1000000), "USD"),
			}, nil, nil))

			// Create 12 users with main and savings accounts
			for i := range 12 {
				userID := fmt.Sprintf("%08d-%04d-%04d-%04d-%012d", i+1, 0, 0, 0, i+1)
				requests = append(requests,
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank:main", fmt.Sprintf("users:%s:main", userID), big.NewInt(1000), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("bank:main", fmt.Sprintf("users:%s:savings", userID), big.NewInt(500), "USD"),
					}, nil, nil),
				)
			}

			// Collect fees
			requests = append(requests, createTransactionAction(ledgerName, []*commonpb.Posting{
				newPosting("bank:main", "bank:fees", big.NewInt(50), "USD"),
			}, nil, nil))

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should return the correct total accounts", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// world + bank:main + bank:fees + 12*2 user accounts = 27
				g.Expect(resp.TotalAccounts).To(Equal(uint64(27)))
			}).Should(Succeed())
		})

		It("Should produce a chart with both fixed and variable segments", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.SuggestedChart).NotTo(BeNil())

				// Should have at least "bank", "users", and "world" top-level segments
				segNames := make(map[string]bool)
				for _, seg := range resp.SuggestedChart.Segments {
					segNames[seg.FixedValue] = true
				}
				g.Expect(segNames).To(HaveKey("bank"))
				g.Expect(segNames).To(HaveKey("users"))
				g.Expect(segNames).To(HaveKey("world"))
			}).Should(Succeed())
		})

		It("Should detect variable user IDs under users segment", func() {
			Eventually(func(g Gomega) {
				resp, err := client.AnalyzeAccounts(ctx, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				var usersSeg *commonpb.ChartSegment
				for _, seg := range resp.SuggestedChart.Segments {
					if seg.FixedValue == "users" {
						usersSeg = seg
						break
					}
				}
				g.Expect(usersSeg).NotTo(BeNil())
				// users should have exactly one variable child (the user ID)
				g.Expect(usersSeg.Children).To(HaveLen(1))
				g.Expect(usersSeg.Children[0].Variable).NotTo(BeNil())
				// Under the variable user ID, there should be fixed children: main, savings
				g.Expect(usersSeg.Children[0].Children).NotTo(BeEmpty())
			}).Should(Succeed())
		})
	})
})
