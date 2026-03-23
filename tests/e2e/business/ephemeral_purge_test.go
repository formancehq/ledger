//go:build e2e

package business

import (
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("EphemeralPurge", Ordered, func() {

	Context("When a transit account with ephemeral type reaches zero balance", Ordered, func() {
		const ledgerName = "ephemeral-purge"

		BeforeAll(func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Add ephemeral account type for clearing accounts
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:            "clearing",
									Pattern:         "clearing:{id}",
									Status:          commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
									Ephemeral:       true,
								},
							},
						},
					},
					// Also add a non-ephemeral type for bank accounts
					{
						Type: &servicepb.Request_AddAccountType{
							AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
								Ledger: ledgerName,
								AccountType: &commonpb.AccountType{
									Name:            "bank",
									Pattern:         "bank:{id}",
									Status:          commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
								},
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())

			// Transaction: world → clearing:tx1 100 USD (leg 1)
			// Transaction: clearing:tx1 → bank:main 100 USD (leg 2)
			// After both legs, clearing:tx1 has input=100, output=100 → zero balance
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "clearing:tx1", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("clearing:tx1", "bank:main", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should purge the clearing account volumes", func() {
			// The clearing:tx1 account had input=100, output=100 → zero balance.
			// Since the account type is ephemeral, its volumes should be purged.
			// GetAccount should either not return volumes or return zero volumes.
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "clearing:tx1",
				})
				g.Expect(err).To(Succeed())

				// Purged: volumes map should be empty (no persisted volumes)
				g.Expect(account.GetVolumes()).To(BeEmpty(),
					"ephemeral account with zero balance should have purged volumes")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should keep the bank account volumes", func() {
			// bank:main is NOT ephemeral, so its volumes should be kept.
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "bank:main",
				})
				g.Expect(err).To(Succeed())

				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue(), "expected USD volumes on bank:main")
				g.Expect(usdVol.GetInput()).To(Equal("100"))
				g.Expect(usdVol.GetBalance()).To(Equal("100"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should allow reusing a purged ephemeral account", func() {
			// Send money through clearing:tx1 again
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "clearing:tx1", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Now clearing:tx1 has non-zero balance (input=50, output=0), so it should be visible
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "clearing:tx1",
				})
				g.Expect(err).To(Succeed())

				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue(), "expected USD volumes after reuse")
				g.Expect(usdVol.GetInput()).To(Equal("50"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When a non-ephemeral account reaches zero balance", Ordered, func() {
		const ledgerName = "ephemeral-purge-non"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// world → alice 100 USD, alice → bob 100 USD
			// alice ends with input=100, output=100 (zero balance) but no ephemeral type
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("alice", "bob", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should keep volumes even at zero balance", func() {
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "alice",
				})
				g.Expect(err).To(Succeed())

				usdVol, ok := account.GetVolumes()["USD"]
				g.Expect(ok).To(BeTrue(), "expected USD volumes on non-ephemeral account")
				g.Expect(usdVol.GetInput()).To(Equal("100"))
				g.Expect(usdVol.GetOutput()).To(Equal("100"))
				g.Expect(usdVol.GetBalance()).To(Equal("0"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
