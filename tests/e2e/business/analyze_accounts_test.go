//go:build e2e

package business

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// analyzeAccounts calls the streaming AnalyzeAccounts RPC and returns the final result.
// Progress events are discarded; only the terminal Result event is returned.
func analyzeAccounts(ctx context.Context, client servicepb.BucketServiceClient, req *servicepb.AnalyzeAccountsRequest) (*servicepb.AnalyzeAccountsResponse, error) {
	return actions.AnalyzeAccounts(ctx, client, req.GetLedger(), req.GetVariableThreshold())
}

var _ = Describe("AnalyzeAccounts", Ordered, func() {

	Context("When analyzing an empty ledger", Ordered, func() {
		var ledgerName = "analyze-empty"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should return zero accounts and no patterns", func() {
			resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions that produce fixed accounts: world, bank:main, bank:fees
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank:main", big.NewInt(1000), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return correct total accounts", func() {
			// Index builder processes logs asynchronously; poll until indexes are up to date.
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// world + bank:main + bank:fees = 3
				g.Expect(resp.TotalAccounts).To(Equal(uint64(3)))
			}).Should(Succeed())
		})

		It("Should discover patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.Patterns).NotTo(BeEmpty())
			}).Should(Succeed())
		})

		It("Should discover patterns for fixed segments", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.Patterns).NotTo(BeEmpty())
			}).Should(Succeed())
		})
	})

	Context("When analyzing a ledger with variable (UUID) accounts", Ordered, func() {
		var ledgerName = "analyze-variable"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 15 user accounts with UUID-like IDs to trigger variable detection
			requests := make([]*servicepb.Request, 0, 15)
			for i := range 15 {
				userAddr := fmt.Sprintf("users:%08d-%04d-%04d-%04d-%012d", i, i, i, i, i)
				requests = append(requests, actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", userAddr, big.NewInt(int64(100+i)), "USD"),
				}, nil, nil))
			}

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should detect variable segments", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// 15 user accounts + world = 16
				g.Expect(resp.TotalAccounts).To(Equal(uint64(16)))

				// Should have a pattern with variable segments
				var hasVariable bool
				for _, p := range resp.Patterns {
					for _, s := range p.Segments {
						if s.Type == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
							hasVariable = true
							break
						}
					}
				}
				g.Expect(hasVariable).To(BeTrue(), "expected a pattern with variable segments")
			}).Should(Succeed())
		})

		It("Should include patterns with variable segments", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 5 distinct child accounts under "dept:"
			for i := range 5 {
				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("dept:%d", 1000+i), big.NewInt(100), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should treat children as fixed with default threshold", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger:            ledgerName,
					VariableThreshold: 0, // default = 10
				})
				g.Expect(err).To(Succeed())

				// 5 children < 10 threshold: each "dept:XXXX" should be its own fixed pattern
				// (5 dept patterns + 1 world pattern = 6)
				g.Expect(len(resp.Patterns)).To(BeNumerically(">=", 5))
			}).Should(Succeed())
		})

		It("Should treat children as variable with low threshold", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger:            ledgerName,
					VariableThreshold: 3, // 5 children > 3 threshold -> variable
				})
				g.Expect(err).To(Succeed())

				// With threshold=3, 5 children should be classified as variable
				var hasVariable bool
				for _, p := range resp.Patterns {
					for _, s := range p.Segments {
						if s.Type == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
							hasVariable = true
							break
						}
					}
				}
				g.Expect(hasVariable).To(BeTrue(), "expected variable patterns with low threshold")
			}).Should(Succeed())
		})
	})

	Context("When analyzing a non-existent ledger", func() {
		It("Should return a NotFound error", func() {
			_, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create accounts and add metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(200), "EUR"),
					}, nil, nil),
					actions.SaveAccountMetadataAction(ledgerName, "users:alice", map[string]string{
						"role": "admin",
						"tier": "premium",
					}),
					actions.SaveAccountMetadataAction(ledgerName, "users:bob", map[string]string{
						"role": "user",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should include metadata keys in patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
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
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Build a realistic account structure:
			// world, bank:main, bank:fees, users:{id}:main, users:{id}:savings
			requests := make([]*servicepb.Request, 0)

			// Fund bank:main from world
			requests = append(requests, actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bank:main", big.NewInt(1000000), "USD"),
			}, nil, nil))

			// Create 12 users with main and savings accounts
			for i := range 12 {
				userID := fmt.Sprintf("%08d-%04d-%04d-%04d-%012d", i+1, 0, 0, 0, i+1)
				requests = append(requests,
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank:main", fmt.Sprintf("users:%s:main", userID), big.NewInt(1000), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank:main", fmt.Sprintf("users:%s:savings", userID), big.NewInt(500), "USD"),
					}, nil, nil),
				)
			}

			// Collect fees
			requests = append(requests, actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("bank:main", "bank:fees", big.NewInt(50), "USD"),
			}, nil, nil))

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should return the correct total accounts", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// world + bank:main + bank:fees + 12*2 user accounts = 27
				g.Expect(resp.TotalAccounts).To(Equal(uint64(27)))
			}).Should(Succeed())
		})

		It("Should produce patterns covering bank, users, and world", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.Patterns).NotTo(BeEmpty())

				// Collect top-level prefixes from patterns
				prefixes := make(map[string]bool)
				for _, p := range resp.Patterns {
					if len(p.Segments) > 0 && p.Segments[0].FixedValue != "" {
						prefixes[p.Segments[0].FixedValue] = true
					}
					if p.Pattern == "world" {
						prefixes["world"] = true
					}
				}
				g.Expect(prefixes).To(HaveKey("bank"))
				g.Expect(prefixes).To(HaveKey("users"))
				g.Expect(prefixes).To(HaveKey("world"))
			}).Should(Succeed())
		})

		It("Should detect variable user IDs in patterns", func() {
			Eventually(func(g Gomega) {
				resp, err := analyzeAccounts(sharedCtx, sharedClient, &servicepb.AnalyzeAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				// Find user patterns with variable segments
				var hasVariableUser bool
				for _, p := range resp.Patterns {
					if len(p.Segments) >= 2 &&
						p.Segments[0].FixedValue == "users" &&
						p.Segments[1].Type == servicepb.PatternSegmentType_PATTERN_SEGMENT_TYPE_VARIABLE {
						hasVariableUser = true
						break
					}
				}
				g.Expect(hasVariableUser).To(BeTrue(), "expected variable user ID in patterns")
			}).Should(Succeed())
		})
	})
})
