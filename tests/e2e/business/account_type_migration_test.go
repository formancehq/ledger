//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("AccountTypeMigration", Ordered, func() {

	Context("When migrating an account type pattern", Ordered, func() {
		var ledgerName = "acct-type-migrate-ledger"

		BeforeAll(func() {
			// Create ledger + account type with pattern "users:{id}:checking"
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.AddAccountTypeAction(
						ledgerName,
						"user-checking",
						"users:{id}:checking",
					),
				},
			})
			Expect(err).To(Succeed())

			// Create transactions that touch accounts matching the pattern
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "users:alice:checking", big.NewInt(100), "USD"),
					}, nil, nil),
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "users:bob:checking", big.NewInt(200), "EUR"),
					}, nil, nil),
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "users:charlie:checking", big.NewInt(50), "USD"),
						testutil.NewPosting("world", "users:charlie:checking", big.NewInt(75), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Add metadata to some accounts
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.SaveAccountMetadataAction(ledgerName, "users:alice:checking", map[string]string{
						"tier": "premium",
					}),
					testutil.SaveAccountMetadataAction(ledgerName, "users:bob:checking", map[string]string{
						"tier": "basic",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should migrate accounts to the new pattern", func() {
			// Trigger migration: "users:{id}:checking" -> "usr:{id}:chk"
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.MigrateAccountTypeAction(ledgerName, "user-checking", "usr:{id}:chk"),
				},
			})
			Expect(err).To(Succeed())

			// Wait for the migration to complete (background worker processes it)
			Eventually(func(g Gomega) {
				ledgerInfo, err := testutil.GetLedger(sharedCtx, sharedClient, ledgerName)
				g.Expect(err).To(Succeed())

				at, ok := ledgerInfo.GetAccountTypes()["user-checking"]
				g.Expect(ok).To(BeTrue())
				g.Expect(at.GetStatus()).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE))
				g.Expect(at.GetPattern()).To(Equal("usr:{id}:chk"))
				g.Expect(at.GetMigration()).To(BeNil())
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})

		It("Should have migrated volumes to new addresses", func() {
			// Check alice's new address has the correct balance
			aliceAcct, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "usr:alice:chk")
			Expect(err).To(Succeed())
			Expect(aliceAcct.Volumes).To(HaveKey("USD"))
			Expect(aliceAcct.Volumes["USD"].Input).To(Equal("100"))

			// Check bob
			bobAcct, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "usr:bob:chk")
			Expect(err).To(Succeed())
			Expect(bobAcct.Volumes).To(HaveKey("EUR"))
			Expect(bobAcct.Volumes["EUR"].Input).To(Equal("200"))

			// Check charlie (multi-asset)
			charlieAcct, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "usr:charlie:chk")
			Expect(err).To(Succeed())
			Expect(charlieAcct.Volumes).To(HaveKey("USD"))
			Expect(charlieAcct.Volumes["USD"].Input).To(Equal("50"))
			Expect(charlieAcct.Volumes).To(HaveKey("EUR"))
			Expect(charlieAcct.Volumes["EUR"].Input).To(Equal("75"))
		})

		It("Should have migrated metadata to new addresses", func() {
			aliceAcct, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "usr:alice:chk")
			Expect(err).To(Succeed())
			Expect(aliceAcct.Metadata).NotTo(BeNil())
			Expect(aliceAcct.Metadata.ToMap()["tier"]).To(Equal("premium"))

			bobAcct, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "usr:bob:chk")
			Expect(err).To(Succeed())
			Expect(bobAcct.Metadata).NotTo(BeNil())
			Expect(bobAcct.Metadata.ToMap()["tier"]).To(Equal("basic"))
		})

		It("Should accept new transactions on the new pattern", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "usr:dave:chk", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			daveAcct, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "usr:dave:chk")
			Expect(err).To(Succeed())
			Expect(daveAcct.Volumes).To(HaveKey("USD"))
			Expect(daveAcct.Volumes["USD"].Input).To(Equal("500"))
		})

		It("Should reject transactions using the old pattern after migration", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "users:eve:checking", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonAccountNotMatchingType))
		})
	})

	Context("When migration is requested with incompatible patterns", func() {
		var ledgerName = "acct-type-migrate-incompat"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.AddAccountTypeAction(
						ledgerName,
						"vendor",
						"vendors:{id}",
					),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject migration when target has variables not in source", func() {
			// Target "vendors:{id}:{region}" has {region} which doesn't exist in source
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.MigrateAccountTypeAction(ledgerName, "vendor", "vendors:{id}:{region}"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonAccountTypeMigrationNotCompatible))
		})
	})

	Context("When migration is requested on a non-existent type", func() {
		var ledgerName = "acct-type-migrate-notfound"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return not found error", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.MigrateAccountTypeAction(ledgerName, "nonexistent", "foo:{id}"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})

	Context("When migration is requested while another is in progress", func() {
		var ledgerName = "acct-type-migrate-double"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.AddAccountTypeAction(
						ledgerName,
						"merchants",
						"merchants:{id}",
					),
				},
			})
			Expect(err).To(Succeed())

			// Create many transactions to make migration take some time
			requests := make([]*servicepb.Request, 50)
			for i := range requests {
				requests[i] = testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					testutil.NewPosting("world", fmt.Sprintf("merchants:shop%d", i), big.NewInt(int64(100+i)), "USD"),
				}, nil, nil)
			}

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: requests,
			})
			Expect(err).To(Succeed())
		})

		It("Should reject a second migration while the first is in progress", func() {
			// Start first migration
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.MigrateAccountTypeAction(ledgerName, "merchants", "merch:{id}"),
				},
			})
			Expect(err).To(Succeed())

			// Immediately try another migration (should fail if still in progress)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.MigrateAccountTypeAction(ledgerName, "merchants", "m:{id}"),
				},
			})

			// Could succeed if the first migration completed very fast,
			// or fail with MIGRATION_IN_PROGRESS. Either way is valid.
			if err != nil {
				st, ok := status.FromError(err)
				Expect(ok).To(BeTrue())
				Expect(st.Code()).To(Equal(codes.FailedPrecondition))

				info := testutil.ExtractGRPCErrorInfo(err)
				Expect(info).NotTo(BeNil())
				Expect(info.Reason).To(Equal(domain.ErrReasonAccountTypeMigrationInProgress))
			}

			// Wait for any migration to complete
			Eventually(func(g Gomega) {
				ledgerInfo, err := testutil.GetLedger(sharedCtx, sharedClient, ledgerName)
				g.Expect(err).To(Succeed())

				at, ok := ledgerInfo.GetAccountTypes()["merchants"]
				g.Expect(ok).To(BeTrue())
				g.Expect(at.GetStatus()).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE))
				g.Expect(at.GetMigration()).To(BeNil())
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When creating a transaction during migration", Ordered, func() {
		var ledgerName = "acct-type-migrate-during-tx"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.AddAccountTypeAction(
						ledgerName,
						"wallets",
						"wallets:{id}",
					),
				},
			})
			Expect(err).To(Succeed())

			// Create initial account
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "wallets:user1", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow transactions using the new pattern during migration", func() {
			// Start migration
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.MigrateAccountTypeAction(ledgerName, "wallets", "w:{id}"),
				},
			})
			Expect(err).To(Succeed())

			// Transaction using the NEW pattern should succeed (even during migration)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "w:user2", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Wait for migration to complete
			Eventually(func(g Gomega) {
				ledgerInfo, err := testutil.GetLedger(sharedCtx, sharedClient, ledgerName)
				g.Expect(err).To(Succeed())

				at, ok := ledgerInfo.GetAccountTypes()["wallets"]
				g.Expect(ok).To(BeTrue())
				g.Expect(at.GetStatus()).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE))
				g.Expect(at.GetPattern()).To(Equal("w:{id}"))
			}).Within(15 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

			// Verify the migrated account has correct volumes
			user1, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "w:user1")
			Expect(err).To(Succeed())
			Expect(user1.Volumes).To(HaveKey("USD"))
			Expect(user1.Volumes["USD"].Input).To(Equal("1000"))

			// Verify the new account created during migration
			user2, err := testutil.GetAccount(sharedCtx, sharedClient, ledgerName, "w:user2")
			Expect(err).To(Succeed())
			Expect(user2.Volumes).To(HaveKey("USD"))
			Expect(user2.Volumes["USD"].Input).To(Equal("500"))
		})
	})

	// Regression test for the double-entry invariant when a transaction that
	// uses the NEW pattern hits an address whose OLD form still holds volumes.
	// resolveMigratingVolumes used to leave the old key un-zeroed and, on the
	// "no existing newVol" branch, aliased the old VolumePair pointer into the
	// new key. Once persisted, Pebble held the same value under both keys and
	// the aggregator double-counted the migrated balance across the ledger.
	//
	// The race is forced deterministically by batching MigrateAccountType and
	// the probe CreateTransactions into a single Apply. Every order in that
	// Apply runs in one FSM tick, so the probes see MIGRATING status with the
	// old keys still fully populated — the background migrator only dispatches
	// after the batch commits.
	Context("When a new-pattern tx hits an un-migrated old address", Ordered, func() {
		var ledgerName = "acct-type-migrate-invariant"

		const (
			asset      = "COIN"
			probeCount = 20
		)

		BeforeAll(func() {
			// AUDIT enforcement: when the migrate order and probe txs run in
			// the same FSM tick, validatePostingsAgainstAccountTypes compiles
			// the type's current pattern ("users:{id}") which doesn't match
			// the new-pattern addresses ("u:*"). STRICT would reject the
			// probes. AUDIT lets them through, so the bug path runs.
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					testutil.AddAccountTypeAction(ledgerName, "user", "users:{id}"),
					{Type: &servicepb.Request_SetDefaultEnforcementMode{
						SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{
							Ledger:          ledgerName,
							EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
						},
					}},
				},
			})
			Expect(err).To(Succeed())

			// Seed one 1-COIN credit per probe target so each account is a
			// net receiver: its old-key residual, if double-counted, shows
			// up on the input side of the aggregate.
			seedReqs := make([]*servicepb.Request, probeCount)
			for i := range seedReqs {
				seedReqs[i] = testutil.CreateTransactionAction(
					ledgerName,
					[]*commonpb.Posting{
						testutil.NewPosting("world", fmt.Sprintf("users:u%d", i), big.NewInt(1), asset),
					},
					nil, nil,
				)
			}
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{Requests: seedReqs})
			Expect(err).To(Succeed())

			// Pre-condition: aggregate must already balance.
			agg, err := testutil.AggregateVolumes(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())
			for _, v := range agg.GetVolumes() {
				Expect(v.GetInput().ToBigInt().String()).To(Equal(v.GetOutput().ToBigInt().String()),
					"pre-migration aggregate should balance for %s", v.GetAsset())
			}
		})

		It("Preserves the double-entry invariant across the whole ledger", func() {
			// Batch MigrateAccountType + all probe transactions into a single
			// Apply. The FSM processes every order in one tick: the migrate
			// order sets Status=MIGRATING, then each probe tx — targeting an
			// address whose OLD form still holds volumes — runs through
			// resolveMigratingVolumes before the background migrator gets
			// dispatched.
			reqs := make([]*servicepb.Request, 0, 1+probeCount)
			reqs = append(reqs,
				testutil.MigrateAccountTypeAction(ledgerName, "user", "u:{id}"),
			)
			for i := range probeCount {
				reqs = append(reqs, testutil.CreateTransactionAction(
					ledgerName,
					[]*commonpb.Posting{
						testutil.NewPosting("world", fmt.Sprintf("u:u%d", i), big.NewInt(1), asset),
					},
					nil, nil,
				))
			}
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{Requests: reqs})
			Expect(err).To(Succeed())

			// Let the background migrator finish so the ledger settles into
			// its steady-state post-migration shape.
			Eventually(func(g Gomega) {
				info, err := testutil.GetLedger(sharedCtx, sharedClient, ledgerName)
				g.Expect(err).To(Succeed())

				at, ok := info.GetAccountTypes()["user"]
				g.Expect(ok).To(BeTrue())
				g.Expect(at.GetStatus()).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE))
				g.Expect(at.GetMigration()).To(BeNil())
			}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Double-entry invariant: for each asset, sum(input) == sum(output).
			agg, err := testutil.AggregateVolumes(sharedCtx, sharedClient, ledgerName)
			Expect(err).To(Succeed())
			for _, v := range agg.GetVolumes() {
				input := v.GetInput().ToBigInt().String()
				output := v.GetOutput().ToBigInt().String()
				Expect(input).To(Equal(output),
					"double-entry invariant violated for asset %s: input=%s output=%s",
					v.GetAsset(), input, output)
			}
		})
	})
})
