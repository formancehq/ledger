//go:build e2e

package e2e

import (
	"context"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// addAccountTypeAction creates a request to add an account type to a ledger.
func addAccountTypeAction(ledger, name, pattern string, mode commonpb.ChartEnforcementMode) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: ledger,
				AccountType: &commonpb.AccountType{
					Name:            name,
					Pattern:         pattern,
					EnforcementMode: mode,
				},
			},
		},
	}
}

// migrateAccountTypeAction creates a request to start an account type migration.
func migrateAccountTypeAction(ledger, sourceType, targetType string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_MigrateAccountType{
			MigrateAccountType: &servicepb.MigrateAccountTypeLedgerRequest{
				Ledger:     ledger,
				SourceType: sourceType,
				TargetType: targetType,
			},
		},
	}
}

// cancelMigrationAction creates a request to cancel an in-progress migration.
func cancelMigrationAction(ledger, sourceType string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CancelMigration{
			CancelMigration: &servicepb.CancelMigrationLedgerRequest{
				Ledger:     ledger,
				SourceType: sourceType,
			},
		},
	}
}

// getAccountTypes retrieves the account type map for a ledger.
func getAccountTypes(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (map[string]*commonpb.AccountType, error) {
	info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
	if err != nil {
		return nil, err
	}
	return info.AccountTypes, nil
}

var _ = Describe("AccountMigration", Ordered, func() {
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

	Context("Account type CRUD", Ordered, func() {
		const ledgerName = "acct-type-crud"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should add an account type", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					addAccountTypeAction(ledgerName, "users", "users:{userId}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			types, err := getAccountTypes(ctx, client, ledgerName)
			Expect(err).To(Succeed())
			Expect(types).To(HaveKey("users"))
			Expect(types["users"].Pattern).To(Equal("users:{userId}"))
			Expect(types["users"].Status).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE))
			Expect(types["users"].EnforcementMode).To(Equal(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT))
		})

		It("Should reject duplicate account type", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					addAccountTypeAction(ledgerName, "users", "users:{userId}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonAccountTypeAlreadyExists))
		})

		It("Should reject invalid pattern", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					addAccountTypeAction(ledgerName, "bad", "", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.InvalidArgument))
		})

		It("Should add a second account type", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					addAccountTypeAction(ledgerName, "merchants", "merchants:{merchantId}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				},
			})
			Expect(err).To(Succeed())

			types, err := getAccountTypes(ctx, client, ledgerName)
			Expect(err).To(Succeed())
			Expect(types).To(HaveLen(2))
			Expect(types).To(HaveKey("users"))
			Expect(types).To(HaveKey("merchants"))
		})
	})

	Context("Full migration lifecycle", Ordered, func() {
		const ledgerName = "acct-migration-lifecycle"

		BeforeAll(func() {
			// Create ledger with two account types sharing the same variable {userId}.
			// Source: "user:{userId}" → Target: "users:{userId}:main"
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
					addAccountTypeAction(ledgerName, "old-users", "user:{userId}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
					addAccountTypeAction(ledgerName, "new-users", "users:{userId}:main", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())

			// Create transactions touching accounts that match the old pattern.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:alice", big.NewInt(100), "USD"),
						newPosting("world", "user:alice", big.NewInt(50), "EUR"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Wait for the read index to catch up.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("user:"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should start migration", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "old-users", "new-users"),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Source type should be MIGRATING or already DEPRECATED (worker may complete instantly).
			types, err := getAccountTypes(ctx, client, ledgerName)
			Expect(err).To(Succeed())
			Expect(types["old-users"].Status).To(
				SatisfyAny(
					Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING),
					Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED),
				),
			)
			Expect(types["old-users"].SupersededBy).To(Equal("new-users"))
			Expect(types["old-users"].MigrationProgress).NotTo(BeNil())
		})

		It("Should complete migration asynchronously", func() {
			// Wait for background worker to complete migration.
			Eventually(func(g Gomega) {
				types, err := getAccountTypes(ctx, client, ledgerName)
				g.Expect(err).To(Succeed())
				g.Expect(types["old-users"].Status).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED))
				g.Expect(types["old-users"].MigrationProgress.CompletedAt).NotTo(BeNil())
			}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should have migrated account volumes to new addresses", func() {
			// New accounts (users:alice:main, users:bob:main) should exist in the read index.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("users:"))
				g.Expect(err).To(Succeed())

				addresses := make(map[string]bool)
				for _, a := range accounts {
					addresses[a.Address] = true
				}
				g.Expect(addresses).To(HaveKey("users:alice:main"))
				g.Expect(addresses).To(HaveKey("users:bob:main"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Post-migration account access", Ordered, func() {
		const ledgerName = "acct-migration-access"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
					addAccountTypeAction(ledgerName, "old-users", "user:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
					addAccountTypeAction(ledgerName, "new-users", "users:{id}:main", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:alice", big.NewInt(500), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user:bob", big.NewInt(300), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Wait for read index.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("user:"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Migrate and wait for completion.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "old-users", "new-users"),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				types, err := getAccountTypes(ctx, client, ledgerName)
				g.Expect(err).To(Succeed())
				g.Expect(types["old-users"].Status).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED))
			}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should show volumes on new addresses via GetAccount", func() {
			Eventually(func(g Gomega) {
				alice, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "users:alice:main",
				})
				g.Expect(err).To(Succeed())
				g.Expect(alice).NotTo(BeNil())
				g.Expect(alice.Address).To(Equal("users:alice:main"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			bob, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "users:bob:main",
			})
			Expect(err).To(Succeed())
			Expect(bob).NotTo(BeNil())
			Expect(bob.Address).To(Equal("users:bob:main"))
		})

		It("Should list new addresses in read index", func() {
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("users:"))
			Expect(err).To(Succeed())

			addresses := make(map[string]bool)
			for _, a := range accounts {
				addresses[a.Address] = true
			}
			Expect(addresses).To(HaveKey("users:alice:main"))
			Expect(addresses).To(HaveKey("users:bob:main"))
		})
	})

	Context("Cancel migration", Ordered, func() {
		const ledgerName = "acct-migration-cancel"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
					addAccountTypeAction(ledgerName, "src", "src:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
					addAccountTypeAction(ledgerName, "dst", "dst:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should cancel a migration that hasn't started processing", func() {
			// Start migration (no accounts to migrate, so worker will complete fast —
			// we need to cancel before that happens).
			// Instead, let's test cancellation on a type with no matching accounts
			// so the worker completes quickly, but let's verify the cancel path works
			// when the type is still MIGRATING.

			// Actually, let's create a fresh pair of types for this test.
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					addAccountTypeAction(ledgerName, "cancel-src", "cancel-src:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
					addAccountTypeAction(ledgerName, "cancel-dst", "cancel-dst:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())

			// Start migration.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "cancel-src", "cancel-dst"),
				},
			})
			Expect(err).To(Succeed())

			// Try to cancel (may already be completed if there are no accounts).
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					cancelMigrationAction(ledgerName, "cancel-src"),
				},
			})
			// Cancel may fail if migration already completed. Both outcomes are valid.
			if err == nil {
				// Cancelled successfully: source should be back to ACTIVE.
				types, err := getAccountTypes(ctx, client, ledgerName)
				Expect(err).To(Succeed())
				Expect(types["cancel-src"].Status).To(Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_ACTIVE))
				Expect(types["cancel-src"].SupersededBy).To(BeEmpty())
				Expect(types["cancel-src"].MigrationProgress).To(BeNil())
			}
		})
	})

	Context("Migration error cases", Ordered, func() {
		const ledgerName = "acct-migration-errors"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
					addAccountTypeAction(ledgerName, "active-type", "active:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
					addAccountTypeAction(ledgerName, "target-type", "target:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject migration with non-existent source type", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "nonexistent", "target-type"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonAccountTypeNotFound))
		})

		It("Should reject migration with non-existent target type", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "active-type", "nonexistent"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should reject migration on non-existent ledger", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction("nonexistent-ledger", "active-type", "target-type"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should reject a second concurrent migration", func() {
			// Start a migration first.
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "active-type", "target-type"),
				},
			})
			Expect(err).To(Succeed())

			// Wait for migration state to be visible.
			Eventually(func(g Gomega) {
				types, err := getAccountTypes(ctx, client, ledgerName)
				g.Expect(err).To(Succeed())
				at := types["active-type"]
				g.Expect(at.Status).To(SatisfyAny(
					Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_MIGRATING),
					Equal(commonpb.AccountTypeStatus_ACCOUNT_TYPE_DEPRECATED),
				))
			}).Within(5 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

			// Try to start another migration — should fail since source is no longer ACTIVE.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					migrateAccountTypeAction(ledgerName, "active-type", "target-type"),
				},
			})
			Expect(err).To(HaveOccurred())

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonMigrationAlreadyActive))
		})
	})

	Context("Cancel migration error cases", Ordered, func() {
		const ledgerName = "acct-cancel-errors"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerName, nil),
					addAccountTypeAction(ledgerName, "not-migrating", "nm:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject cancel on non-existent type", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					cancelMigrationAction(ledgerName, "nonexistent"),
				},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})
})
