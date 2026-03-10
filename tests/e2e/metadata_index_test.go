//go:build e2e

package e2e

import (
	"context"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// These tests validate that metadata index updates correctly remove old values
// from the index when metadata is modified or deleted. This is critical because
// the indexbuilder relies on previous_value fields in the log to clean up stale
// forward index entries.

var _ = Describe("MetadataIndexConsistency", Ordered, func() {
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

	// ========================================================================
	// Account metadata: update value and verify old value is no longer indexed
	// ========================================================================
	Context("Account metadata update removes old indexed value", Ordered, func() {
		const ledgerName = "idx-acct-meta-update"

		BeforeAll(func() {
			// Create ledger with a string metadata field + index
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			// Create index on the "role" field
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
				},
			})
			Expect(err).To(Succeed())
			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")
		})

		It("Should find account by initial metadata value", func() {
			// Create account with role=admin
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
				},
			})
			Expect(err).To(Succeed())

			// Query: role=admin should return alice
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("role", "admin"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should update metadata and no longer find account by old value", func() {
			// Update alice's role from admin to viewer
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "viewer"}),
				},
			})
			Expect(err).To(Succeed())

			// Query: role=viewer should return alice
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("role", "viewer"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Query: role=admin should return NO results (old value removed from index)
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("role", "admin"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(BeEmpty(), "old value 'admin' should no longer be indexed")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should handle multiple updates and only find by latest value", func() {
			// Update alice's role several times
			for _, newRole := range []string{"editor", "moderator", "superadmin"} {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						saveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": newRole}),
					},
				})
				Expect(err).To(Succeed())
			}

			// Only the latest value should be indexed
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("role", "superadmin"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// All previous values should be gone from the index
			for _, oldRole := range []string{"admin", "viewer", "editor", "moderator"} {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("role", oldRole))
				Expect(err).To(Succeed())
				Expect(accounts).To(BeEmpty(), "old value %q should no longer be indexed", oldRole)
			}
		})
	})

	// ========================================================================
	// Account metadata: delete and verify value is no longer indexed
	// ========================================================================
	Context("Account metadata delete removes indexed value", Ordered, func() {
		const ledgerName = "idx-acct-meta-delete"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "category",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category"),
				},
			})
			Expect(err).To(Succeed())
			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category")
		})

		It("Should no longer find account after metadata key is deleted", func() {
			// Create account with category=vip
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(50), "EUR"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "bob", map[string]string{"category": "vip"}),
				},
			})
			Expect(err).To(Succeed())

			// Verify bob is found by category=vip
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("category", "vip"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Delete the metadata key
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					deleteAccountMetadataAction(ledgerName, "bob", "category"),
				},
			})
			Expect(err).To(Succeed())

			// category=vip should return NO results
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("category", "vip"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(BeEmpty(), "deleted value 'vip' should no longer be indexed")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Transaction metadata: update and verify old value no longer indexed
	// ========================================================================
	Context("Transaction metadata update removes old indexed value", Ordered, func() {
		const ledgerName = "idx-tx-meta-update"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "status",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "status"),
				},
			})
			Expect(err).To(Succeed())
			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "status")
		})

		It("Should update transaction metadata and no longer find by old value", func() {
			// Create transaction with status=pending
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "merchant", big.NewInt(1000), "USD"),
					}, map[string]string{"status": "pending"}),
				},
			})
			Expect(err).To(Succeed())
			txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Verify transaction found by status=pending
			Eventually(func(g Gomega) {
				txs, err := listAllTransactions(ctx, client, ledgerName, 0, 0, stringFilter("status", "pending"))
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(1))
				g.Expect(txs[0].Id).To(Equal(txID))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Update status to completed
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, txID, map[string]string{"status": "completed"}),
				},
			})
			Expect(err).To(Succeed())

			// status=completed should return the transaction
			Eventually(func(g Gomega) {
				txs, err := listAllTransactions(ctx, client, ledgerName, 0, 0, stringFilter("status", "completed"))
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(1))
				g.Expect(txs[0].Id).To(Equal(txID))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// status=pending should return NO results
			Eventually(func(g Gomega) {
				txs, err := listAllTransactions(ctx, client, ledgerName, 0, 0, stringFilter("status", "pending"))
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(BeEmpty(), "old value 'pending' should no longer be indexed")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Transaction metadata: delete and verify value no longer indexed
	// ========================================================================
	Context("Transaction metadata delete removes indexed value", Ordered, func() {
		const ledgerName = "idx-tx-meta-delete"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "tag",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tag"),
				},
			})
			Expect(err).To(Succeed())
			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "tag")
		})

		It("Should no longer find transaction after metadata key is deleted", func() {
			// Create transaction with tag=urgent
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "ops", big.NewInt(500), "EUR"),
					}, map[string]string{"tag": "urgent"}),
				},
			})
			Expect(err).To(Succeed())
			txID := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction().Transaction.Id

			// Verify found
			Eventually(func(g Gomega) {
				txs, err := listAllTransactions(ctx, client, ledgerName, 0, 0, stringFilter("tag", "urgent"))
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Delete the metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					deleteTransactionMetadataAction(ledgerName, txID, "tag"),
				},
			})
			Expect(err).To(Succeed())

			// tag=urgent should return NO results
			Eventually(func(g Gomega) {
				txs, err := listAllTransactions(ctx, client, ledgerName, 0, 0, stringFilter("tag", "urgent"))
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(BeEmpty(), "deleted value 'urgent' should no longer be indexed")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Multiple accounts: update one, verify the other is unaffected
	// ========================================================================
	Context("Metadata update on one account does not affect others", Ordered, func() {
		const ledgerName = "idx-acct-meta-multi"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "tier",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier"),
				},
			})
			Expect(err).To(Succeed())
			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")
		})

		It("Should only remove old value for the updated account", func() {
			// Create two accounts both with tier=gold
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user1", big.NewInt(100), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "user2", big.NewInt(100), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "user1", map[string]string{"tier": "gold"}),
					saveAccountMetadataAction(ledgerName, "user2", map[string]string{"tier": "gold"}),
				},
			})
			Expect(err).To(Succeed())

			// Both should be found
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("tier", "gold"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Update user1 to tier=platinum
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "user1", map[string]string{"tier": "platinum"}),
				},
			})
			Expect(err).To(Succeed())

			// tier=gold should now return only user2
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("tier", "gold"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("user2"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// tier=platinum should return user1
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("tier", "platinum"))
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(1))
			Expect(accounts[0].Address).To(Equal("user1"))
		})
	})

	// ========================================================================
	// Account metadata set at transaction creation (via account_metadata)
	// ========================================================================
	Context("Account metadata from transaction creation is indexed correctly", Ordered, func() {
		const ledgerName = "idx-acct-meta-txcreate"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "source",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "source"),
				},
			})
			Expect(err).To(Succeed())
			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "source")
		})

		It("Should index account metadata set via transaction creation and update correctly", func() {
			// Create transaction with account metadata source=api
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "merchant", big.NewInt(1000), "USD"),
					}, nil, map[string]*commonpb.MetadataSet{
						"merchant": {
							Metadata: []*commonpb.Metadata{
								{Key: "source", Value: commonpb.NewStringValue("api")},
							},
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			// Verify source=api returns merchant
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("source", "api"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("merchant"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Update source to webhook via SaveMetadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "merchant", map[string]string{"source": "webhook"}),
				},
			})
			Expect(err).To(Succeed())

			// source=webhook should return merchant
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("source", "webhook"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("merchant"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// source=api should return NO results
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("source", "api"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(BeEmpty(), "old value 'api' should no longer be indexed")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
