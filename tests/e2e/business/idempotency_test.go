//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Idempotency Keys", Ordered, func() {

	Context("When using idempotency keys for ledger creation", func() {
		It("Should create a ledger with idempotency key", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(actions.CreateLedgerAction("idempotent-ledger", nil), "create-ledger-key-1"),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the ledger was created
			ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
				Ledger: "idempotent-ledger",
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal("idempotent-ledger"))
		})

		It("Should return same result for duplicate request with same idempotency key", func() {
			idempotencyKey := "duplicate-create-ledger-key"

			// First request
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(actions.CreateLedgerAction("duplicate-ledger", nil), idempotencyKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp1).NotTo(BeNil())
			Expect(resp1.Logs).To(HaveLen(1))
			firstLogSequence := resp1.Logs[0].Sequence

			// Second request with same idempotency key and same content
			resp2, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(actions.CreateLedgerAction("duplicate-ledger", nil), idempotencyKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp2).NotTo(BeNil())
			Expect(resp2.Logs).To(HaveLen(1))

			// Should return the same log sequence (reference to original)
			Expect(resp2.Logs[0].Sequence).To(Equal(firstLogSequence))
		})

		It("Should fail when reusing idempotency key with different content", func() {
			idempotencyKey := "conflict-ledger-key"

			// First request - create ledger "ledger-a"
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(actions.CreateLedgerAction("ledger-a", nil), idempotencyKey),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp1).NotTo(BeNil())

			// Second request with same idempotency key but different content (different ledger name)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(actions.CreateLedgerAction("ledger-b", nil), idempotencyKey),
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("idempotency key conflict"))
		})
	})

	Context("When using idempotency keys for transactions", Ordered, func() {
		var ledgerName = "idempotency-tx-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should create a transaction with idempotency key", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"tx-key-1",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should return same result for duplicate transaction with same idempotency key", func() {
			idempotencyKey := "duplicate-tx-key"

			// First request
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-dup", big.NewInt(100), "USD"),
						}, nil, nil),
						idempotencyKey,
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp1).NotTo(BeNil())
			Expect(resp1.Logs).To(HaveLen(1))
			firstLogSequence := resp1.Logs[0].Sequence

			// Second request with same idempotency key and same content
			resp2, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-dup", big.NewInt(100), "USD"),
						}, nil, nil),
						idempotencyKey,
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp2).NotTo(BeNil())
			Expect(resp2.Logs).To(HaveLen(1))

			// Should return the same log sequence (reference to original)
			Expect(resp2.Logs[0].Sequence).To(Equal(firstLogSequence))

			// Verify the account balance - should only have 100, not 200
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-dup",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Input).To(Equal("100"))
		})

		It("Should fail when reusing idempotency key with different transaction content", func() {
			idempotencyKey := "conflict-tx-key"

			// First request - transfer 100 USD
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-conflict", big.NewInt(100), "USD"),
						}, nil, nil),
						idempotencyKey,
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp1).NotTo(BeNil())

			// Second request with same idempotency key but different amount (200 instead of 100)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-conflict", big.NewInt(200), "USD"),
						}, nil, nil),
						idempotencyKey,
					),
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("idempotency key conflict"))
		})

		It("Should allow same content with different idempotency keys", func() {
			// First request with key-a
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-multi", big.NewInt(100), "USD"),
						}, nil, nil),
						"key-a",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp1).NotTo(BeNil())

			// Second request with key-b (same content, different key)
			resp2, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "account-multi", big.NewInt(100), "USD"),
						}, nil, nil),
						"key-b",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp2).NotTo(BeNil())

			// Both transactions should be processed - different log sequences
			Expect(resp2.Logs[0].Sequence).NotTo(Equal(resp1.Logs[0].Sequence))

			// Account should have 200 (100 + 100)
			account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "account-multi",
			})
			Expect(err).To(Succeed())
			Expect(account.Volumes["USD"].Input).To(Equal("200"))
		})
	})

	Context("When using idempotency keys in bulk operations", Ordered, func() {
		var ledgerName = "idempotency-bulk-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should handle idempotency for multiple requests in bulk", func() {
			// First bulk request with multiple transactions
			resp1, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "bulk-account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"bulk-tx-1",
					),
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "bulk-account-2", big.NewInt(200), "USD"),
						}, nil, nil),
						"bulk-tx-2",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp1).NotTo(BeNil())
			Expect(resp1.Logs).To(HaveLen(2))

			// Second bulk request - replay the same
			resp2, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "bulk-account-1", big.NewInt(100), "USD"),
						}, nil, nil),
						"bulk-tx-1",
					),
					actions.WithIdempotencyKey(
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", "bulk-account-2", big.NewInt(200), "USD"),
						}, nil, nil),
						"bulk-tx-2",
					),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp2).NotTo(BeNil())
			Expect(resp2.Logs).To(HaveLen(2))

			// Should return same log sequences
			Expect(resp2.Logs[0].Sequence).To(Equal(resp1.Logs[0].Sequence))
			Expect(resp2.Logs[1].Sequence).To(Equal(resp1.Logs[1].Sequence))

			// Verify balances are correct (not doubled)
			account1, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bulk-account-1",
			})
			Expect(err).To(Succeed())
			Expect(account1.Volumes["USD"].Input).To(Equal("100"))

			account2, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bulk-account-2",
			})
			Expect(err).To(Succeed())
			Expect(account2.Volumes["USD"].Input).To(Equal("200"))
		})
	})
})
