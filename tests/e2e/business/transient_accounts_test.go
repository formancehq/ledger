//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("TransientAccounts", Ordered, func() {

	Context("When transient accounts are used within a batch and zeroed out", Ordered, func() {
		const ledgerName = "transient-zero-balance"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Add transient account type for staging accounts
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:        "staging",
									Pattern:     "staging:{id}",
									Status:      commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
								},
							},
						},
					},
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:    "wallet",
									Pattern: "wallet:{id}",
									Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
								},
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())

			// Batch: world → staging:tx1 100 USD, staging:tx1 → wallet:main 100 USD
			// staging:tx1 ends at zero balance (input=100, output=100)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:tx1", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("staging:tx1", "wallet:main", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should not persist transient account volumes", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "staging:tx1",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.GetVolumes()).To(BeEmpty(),
					"transient account should have no persisted volumes")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should persist normal account volumes", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "wallet:main",
				})
				g.Expect(err).To(Succeed())

				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue(), "expected USD volumes on wallet:main")
				g.Expect(usdVol.GetInput()).To(Equal("100"))
				g.Expect(usdVol.GetBalance()).To(Equal("100"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When a transient account has non-zero balance at end of batch", Ordered, func() {
		const ledgerName = "transient-non-zero"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:        "staging",
									Pattern:     "staging:{id}",
									Status:      commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
								},
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject the batch", func() {
			// world → staging:tx1 100 USD but staging:tx1 is never drained
			// staging:tx1 ends with non-zero balance → batch must be rejected
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:tx1", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(status.Code(err)).To(Equal(codes.FailedPrecondition))
		})
	})

	Context("When an account type is changed to transient after volumes exist", Ordered, func() {
		const ledgerName = "transient-pre-existing"

		BeforeAll(func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Fund staging:a with 100 USD (no account type yet → normal persistence)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:a", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify staging:a has input=100 before marking transient
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "staging:a",
				})
				g.Expect(err).To(Succeed())
				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue())
				g.Expect(usdVol.GetInput()).To(Equal("100"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Now mark staging:{id} as transient
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:        "staging",
									Pattern:     "staging:{id}",
									Status:      commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
								},
							},
						},
					},
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:    "wallet",
									Pattern: "wallet:{id}",
									Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
								},
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())

			// Transfer staging:a → wallet:b 100 USD
			// staging:a is now transient; within this batch output=100, so delta nets to zero
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("staging:a", "wallet:b", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should preserve pre-existing volumes on staging:a after transient eviction", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "staging:a",
				})
				g.Expect(err).To(Succeed())
				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue(), "staging:a should still have USD volumes from before it was transient")
				g.Expect(usdVol.GetInput()).To(Equal("100"))
				g.Expect(usdVol.GetOutput()).To(Equal("100"))
				g.Expect(usdVol.GetBalance()).To(Equal("0"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should persist wallet:b volumes normally", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "wallet:b",
				})
				g.Expect(err).To(Succeed())
				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue())
				g.Expect(usdVol.GetInput()).To(Equal("100"))
				g.Expect(usdVol.GetBalance()).To(Equal("100"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When pre-existing volumes are evicted from cache before transient batch", Ordered, func() {
		const ledgerName = "transient-cache-evicted"

		BeforeAll(func() {
			// Create ledger and account types upfront
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())

			// Fund staging:x with 200 USD (normal persistence, no account type yet)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:x", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Generate many transactions on a different account to force cache rotation
			// past the snapshot threshold (default=10). This evicts staging:x from
			// the in-memory cache, forcing a Pebble re-read on the next batch.
			for i := 0; i < 20; i++ {
				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("filler:%d", i), big.NewInt(1), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Now mark staging as transient and add wallet type
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:        "staging",
									Pattern:     "staging:{id}",
									Status:      commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT,
								},
							},
						},
					},
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:    "wallet",
									Pattern: "wallet:{id}",
									Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
								},
							},
						},
					},
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:    "filler",
									Pattern: "filler:{id}",
									Status:  commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
								},
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())

			// Transfer staging:x → wallet:y 200 USD
			// staging:x base was evicted from cache → must be re-read from Pebble
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("staging:x", "wallet:y", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should preserve staging:x volumes after cache-evicted transient batch", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "staging:x",
				})
				g.Expect(err).To(Succeed())
				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue(), "staging:x should still have USD volumes")
				g.Expect(usdVol.GetInput()).To(Equal("200"))
				g.Expect(usdVol.GetOutput()).To(Equal("200"))
				g.Expect(usdVol.GetBalance()).To(Equal("0"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should persist wallet:y volumes", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "wallet:y",
				})
				g.Expect(err).To(Succeed())
				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue())
				g.Expect(usdVol.GetInput()).To(Equal("200"))
				g.Expect(usdVol.GetBalance()).To(Equal("200"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
