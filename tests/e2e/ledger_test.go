//go:build e2e

package e2e

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Ledger", Ordered, func() {
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

	Context("When saving account metadata via direct endpoint", Ordered, func() {
		var ledgerName = "test-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "test-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save account metadata successfully", func() {
			metadata := map[string]string{
				"account_type": "asset",
				"label":        "Test Account",
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", metadata)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should merge metadata with existing account metadata", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "merge-account", big.NewInt(50), "USD"),
					}, nil, map[string]*commonpb.MetadataSet{
						"merge-account": commonpb.MetadataSetFromMap(map[string]string{
							"key1": "value1",
							"key2": "value2",
						}),
					}),
				},
			})
			Expect(err).To(Succeed())

			metadata := map[string]string{
				"key3": "value3",
				"key2": "updated_value2",
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "merge-account", metadata)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should delete account metadata successfully", func() {
			metadata := map[string]string{
				"to_delete": "value",
			}

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveAccountMetadataAction(ledgerName, "test-account", metadata)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			deleteResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteAccountMetadataAction(ledgerName, "test-account", "to_delete")},
			})
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
			Expect(deleteResp.Logs).To(HaveLen(1))
		})
	})

	Context("When saving account metadata via bulk endpoint", Ordered, func() {
		var ledgerName = "bulk-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save account metadata via bulk endpoint", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{
						"account_type": "asset",
						"label":        "Bulk Account",
					}),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should handle multiple metadata operations in bulk", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account-2", big.NewInt(50), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{"key1": "value1"}),
					saveAccountMetadataAction(ledgerName, "bulk-account-2", map[string]string{"key2": "value2"}),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should delete account metadata via bulk endpoint", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{"to_delete": "value"}),
					deleteAccountMetadataAction(ledgerName, "bulk-account", "to_delete"),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("When creating ledgers", func() {
		var ledgerName = "test-ledger-create"

		It("Should create a ledger successfully", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed(), "Failed to create ledger")
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("Should return ALREADY_EXISTS with LEDGER_ALREADY_EXISTS reason when creating a duplicate ledger", func() {
			// Create ledger
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction("dup-ledger", nil)},
			})
			Expect(err).To(Succeed())

			// Try to create the same ledger again
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction("dup-ledger", nil)},
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(processing.ErrReasonLedgerAlreadyExists))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["name"]).To(Equal("dup-ledger"))
		})
	})

	Context("When saving transaction metadata via direct endpoint", Ordered, func() {
		var ledgerName = "transaction-metadata-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata successfully", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-metadata-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			metadata := map[string]string{
				"reason": "adjustment",
				"source": "support",
			}

			saveResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{saveTransactionMetadataAction(ledgerName, transactionID, metadata)},
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(1))
		})

		It("Should delete transaction metadata successfully", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-metadata-account", big.NewInt(100), "USD"),
					}, map[string]string{"to_delete": "value"}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			deleteResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteTransactionMetadataAction(ledgerName, transactionID, "to_delete")},
			})
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
			Expect(deleteResp.Logs).To(HaveLen(1))
		})
	})

	Context("When saving transaction metadata via bulk endpoint", Ordered, func() {
		var ledgerName = "transaction-metadata-bulk-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata via bulk endpoint", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-bulk-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			saveResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
						"category": "bulk",
						"reason":   "reconciliation",
					}),
				},
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(1))
		})

		It("Should delete transaction metadata via bulk endpoint", func() {
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-bulk-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			saveResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{"to_delete": "value"}),
					deleteTransactionMetadataAction(ledgerName, transactionID, "to_delete"),
				},
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(2))
		})
	})
})
