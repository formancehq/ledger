//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
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

	Context("When the audit entry records transient accounts", Ordered, func() {
		const ledgerName = "transient-audit"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
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
					{
						Type: &servicepb.Request_SetDefaultEnforcementMode{
							SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
								Ledger:          ledgerName,
								EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should include transient accounts in the audit entry", func() {
			// Batch: world → staging:audit1 50 USD, staging:audit1 → dest 50 USD
			// staging:audit1 ends at zero balance → transient
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:audit1", big.NewInt(50), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("staging:audit1", "dest", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Find the audit entry for this batch
			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(entries).NotTo(BeEmpty())

			// The last success entry should contain transient_accounts
			var found bool
			for i := len(entries) - 1; i >= 0; i-- {
				success := entries[i].GetSuccess()
				if success == nil {
					continue
				}

				ta := success.GetTransientAccounts()
				if ta == nil {
					continue
				}

				accountList, ok := ta[ledgerName]
				if !ok || accountList == nil {
					continue
				}

				Expect(accountList.GetAccounts()).To(ContainElement("staging:audit1"),
					"audit entry should list staging:audit1 as transient")
				found = true

				break
			}

			Expect(found).To(BeTrue(), "should find an audit entry with transient_accounts")
		})
	})

	Context("When the audit entry records purged ephemeral accounts", Ordered, func() {
		const ledgerName = "ephemeral-audit"

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
									Name:        "clearing",
									Pattern:     "clearing:{id}",
									Status:      commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE,
									Persistence: commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL,
								},
							},
						},
					},
					{
						Type: &servicepb.Request_SetDefaultEnforcementMode{
							SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
								Ledger:          ledgerName,
								EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should include purged accounts in the audit entry", func() {
			// Batch: world → clearing:ep1 75 USD, clearing:ep1 → dest 75 USD
			// clearing:ep1 ends at zero → purged
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "clearing:ep1", big.NewInt(75), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("clearing:ep1", "dest", big.NewInt(75), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			entries, err := collectAuditEntries(sharedCtx, sharedClient, &servicepb.ListAuditEntriesRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(entries).NotTo(BeEmpty())

			var found bool
			for i := len(entries) - 1; i >= 0; i-- {
				success := entries[i].GetSuccess()
				if success == nil {
					continue
				}

				pa := success.GetPurgedAccounts()
				if pa == nil {
					continue
				}

				accountList, ok := pa[ledgerName]
				if !ok || accountList == nil {
					continue
				}

				Expect(accountList.GetAccounts()).To(ContainElement("clearing:ep1"),
					"audit entry should list clearing:ep1 as purged")
				found = true

				break
			}

			Expect(found).To(BeTrue(), "should find an audit entry with purged_accounts")
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

		It("Should purge staging:a once the pre-existing balance is fully drained", func() {
			// The rebalancing batch (staging:a → wallet:b 100) brings the running
			// cumulative back to zero balance. Mirroring ephemeral, the 0xF1 entry
			// is purged at that point — staging:a returns to "fresh transient"
			// state and GetAccount returns empty.
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "staging:a",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.GetVolumes()).To(BeEmpty(),
					"staging:a should be purged after the rebalancing transfer")
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

		It("Should purge staging:x after the cache-evicted rebalancing transient batch", func() {
			// The rebalancing transfer (staging:x → wallet:y 200) rebuilds Old
			// from Pebble (since staging:x was evicted from cache), sees
			// {200, 0}, computes New = {200, 200}, and the partition mirrors
			// ephemeral: 0xF1 purged, KS.M zeroed. GetAccount must return empty.
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "staging:x",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.GetVolumes()).To(BeEmpty(),
					"staging:x should be purged after the rebalancing transfer")
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

	// Re-touching the same transient address in a later batch must not accumulate.
	// The transient slot must read {0, 0} into the second batch's PCV — anything
	// else means the prior cumulative value leaked through the cache.
	Context("When the same transient address is re-touched across batches", Ordered, func() {
		const ledgerName = "transient-cross-batch-pcv"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.AddAccountTypeWithPersistenceAction(ledgerName, "staging", "staging:{id}",
						commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
					actions.AddAccountTypeAction(ledgerName, "wallet", "wallet:{id}"),
				},
			})
			Expect(err).To(Succeed())

			// Batch 1: world → staging:reuse 100 USD, staging:reuse → wallet:a 100 USD.
			// Zero-balance bulk on staging:reuse.
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:reuse", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("staging:reuse", "wallet:a", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should report fresh PCV (not cumulative) on the second batch", func() {
			// Batch 2: same staging:reuse, balanced again with a different amount.
			// expandVolumes on both postings so we can read the PCV from the response.
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithExpandVolumes(actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "staging:reuse", big.NewInt(50), "USD"),
					}, nil)),
					actions.WithExpandVolumes(actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("staging:reuse", "wallet:b", big.NewInt(50), "USD"),
					}, nil, nil)),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))

			// First transaction's PCV: world → staging:reuse 50.
			// staging:reuse should read {50, 0} — fresh, not cumulative {150, 100}.
			pcv1 := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv1).To(HaveKey("staging:reuse"))
			Expect(pcv1["staging:reuse"].Volumes["USD"].Input).To(Equal("50"),
				"transient input should reflect this batch only, not accumulate across batches")
			Expect(pcv1["staging:reuse"].Volumes["USD"].Output).To(Equal("0"))

			// Second transaction's PCV: staging:reuse → wallet:b 50.
			// staging:reuse now {50, 50} — the per-batch zero-balance — not {150, 150}.
			pcv2 := resp.Logs[1].Payload.GetApply().Log.Data.GetCreatedTransaction().PostCommitVolumes.VolumesByAccount
			Expect(pcv2).To(HaveKey("staging:reuse"))
			Expect(pcv2["staging:reuse"].Volumes["USD"].Input).To(Equal("50"))
			Expect(pcv2["staging:reuse"].Volumes["USD"].Output).To(Equal("50"))

			// And the wallet sees its fresh +50.
			Expect(pcv2["wallet:b"].Volumes["USD"].Input).To(Equal("50"))
			Expect(pcv2["wallet:b"].Volumes["USD"].Output).To(Equal("0"))
		})
	})
})
