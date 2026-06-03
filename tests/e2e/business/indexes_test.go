//go:build e2e

package business

import (
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

var _ = Describe("UserConfigurableIndexes", Ordered, func() {

	// ========================================================================
	// Metadata index lifecycle: create → query → drop
	// ========================================================================
	Context("Metadata index lifecycle", Ordered, func() {
		const ledgerName = "idx-metadata"

		BeforeAll(func() {
			// Create ledger with schema but no indexes
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "category",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject queries on non-indexed metadata fields", func() {
			// Create a prepared query that filters on a non-indexed field
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "category-filter",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.StringMetadataFilter("category", "premium"),
				},
			})
			Expect(err).To(Succeed())

			// Execution should fail with index not found
			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "category-filter",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should succeed after creating index and data", func() {
			// Create the metadata index, then add data (index builder only indexes forward)
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateAccountMetadataIndexAction(ledgerName, "category"),
				},
			})
			Expect(err).To(Succeed())

			// Create data AFTER the index exists
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"category": "premium"}),
				},
			})
			Expect(err).To(Succeed())

			// Wait for the index builder to catch up and query to succeed
			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "category-filter",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
				g.Expect(result.GetCursor().AccountData[0].GetAddress()).To(Equal("alice"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should show index in GetLedger response", func() {
			ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(ledger.MetadataSchema).NotTo(BeNil())
			field, ok := ledger.MetadataSchema.AccountFields["category"]
			Expect(ok).To(BeTrue())
			Expect(field.Indexed).To(BeTrue())
		})

		It("Should reject queries after dropping the index", func() {
			// Drop the metadata index
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DropAccountMetadataIndexAction(ledgerName, "category"),
				},
			})
			Expect(err).To(Succeed())

			// Query should fail again
			Eventually(func(g Gomega) {
				_, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "category-filter",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(HaveOccurred())
				st, ok := status.FromError(err)
				g.Expect(ok).To(BeTrue())
				g.Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Address index lifecycle
	// ========================================================================
	Context("Address index lifecycle", Ordered, func() {
		const ledgerName = "idx-address"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should create and use address index", func() {
			// Create address index (any role)
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
				},
			})
			Expect(err).To(Succeed())

			// Verify GetLedger shows the index
			Eventually(func(g Gomega) {
				ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(ledger.BuiltinIndexes).NotTo(BeNil())
				g.Expect(ledger.BuiltinIndexes.Address).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should create source and destination indexes", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_SOURCE),
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(ledger.BuiltinIndexes).NotTo(BeNil())
				g.Expect(ledger.BuiltinIndexes.SourceAddress).To(BeTrue())
				g.Expect(ledger.BuiltinIndexes.DestAddress).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should drop address index", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DropAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(ledger.BuiltinIndexes).NotTo(BeNil())
				g.Expect(ledger.BuiltinIndexes.Address).To(BeFalse())
				// Source and destination should still be enabled
				g.Expect(ledger.BuiltinIndexes.SourceAddress).To(BeTrue())
				g.Expect(ledger.BuiltinIndexes.DestAddress).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Reference builtin index lifecycle
	// ========================================================================
	Context("Reference index lifecycle", Ordered, func() {
		const ledgerName = "idx-builtin-ref"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject reference filter queries when index does not exist", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-reference",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.ReferenceFilter("pay-001"),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-reference",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			Expect(actions.ExtractGRPCErrorInfo(err).Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should create reference index and query transactions by reference", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithReference(actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil), "pay-001"),
					actions.WithReference(actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil), "pay-002"),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "charlie", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForBuiltinIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-reference",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor().TransactionData).To(HaveLen(1))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should show reference index as READY in GetLedger", func() {
			info, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(info.BuiltinIndexes).NotTo(BeNil())
			Expect(info.BuiltinIndexes.Reference).To(BeTrue())
			Expect(info.BuiltinIndexes.ReferenceStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		})

		It("Should reject reference filter queries after dropping the index", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DropBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				_, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-reference",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(HaveOccurred())
				st, ok := status.FromError(err)
				g.Expect(ok).To(BeTrue())
				g.Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Timestamp builtin index lifecycle
	// ========================================================================
	Context("Timestamp index lifecycle", Ordered, func() {
		const ledgerName = "idx-builtin-ts"

		var ts1, ts2, ts3 time.Time

		BeforeAll(func() {
			ts1 = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
			ts2 = time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
			ts3 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject timestamp filter queries when index does not exist", func() {
			minTs, maxTs := uint64(ts1.UnixMicro()), uint64(ts3.UnixMicro())
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-timestamp",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.TimestampRangeFilter(minTs, maxTs),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-timestamp",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			Expect(actions.ExtractGRPCErrorInfo(err).Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should create timestamp index and query transactions by time range", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.WithTimestamp(actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a", big.NewInt(10), "USD")}, nil), ts1),
					actions.WithTimestamp(actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "b", big.NewInt(20), "USD")}, nil), ts2),
					actions.WithTimestamp(actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "c", big.NewInt(30), "USD")}, nil), ts3),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForBuiltinIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)).To(Succeed())

			// Full range ts1..ts3 → 3 transactions
			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-timestamp",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor().TransactionData).To(HaveLen(3))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return only transactions in a narrower timestamp range", func() {
			minTs, maxTs := uint64(ts1.UnixMicro()), uint64(ts2.UnixMicro())
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-timestamp-narrow",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.TimestampRangeFilter(minTs, maxTs),
				},
			})
			Expect(err).To(Succeed())

			result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-timestamp-narrow",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())
			Expect(result.GetCursor().TransactionData).To(HaveLen(2))
		})

		It("Should show timestamp index as READY in GetLedger", func() {
			info, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(info.BuiltinIndexes).NotTo(BeNil())
			Expect(info.BuiltinIndexes.Timestamp).To(BeTrue())
			Expect(info.BuiltinIndexes.TimestampStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		})
	})

	// ========================================================================
	// InsertedAt builtin index lifecycle
	// ========================================================================
	Context("InsertedAt index lifecycle", Ordered, func() {
		const ledgerName = "idx-builtin-iat"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject inserted_at filter queries when index does not exist", func() {
			// Use a wide range that covers any possible insertion time.
			minTs, maxTs := uint64(0), uint64(time.Now().Add(time.Hour).UnixMicro())
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-inserted-at",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.InsertedAtRangeFilter(minTs, maxTs),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-inserted-at",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			Expect(actions.ExtractGRPCErrorInfo(err).Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should create inserted_at index and query transactions by creation time range", func() {
			// Record time before creating transactions.
			beforeCreate := time.Now()

			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
				},
			})
			Expect(err).To(Succeed())

			// Create transactions — their inserted_at will be ~now (wall clock).
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a", big.NewInt(10), "USD")}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "b", big.NewInt(20), "USD")}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "c", big.NewInt(30), "USD")}, nil),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForBuiltinIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT)).To(Succeed())

			// Query all transactions created between beforeCreate and now+1h.
			minTs := uint64(beforeCreate.UnixMicro())
			maxTs := uint64(time.Now().Add(time.Hour).UnixMicro())
			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-inserted-at-all",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.InsertedAtRangeFilter(minTs, maxTs),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-inserted-at-all",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor().TransactionData).To(HaveLen(3))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return no results for a past time range (before any transaction was created)", func() {
			// A range far in the past should match nothing.
			pastMin := uint64(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro())
			pastMax := uint64(time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC).UnixMicro())
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-inserted-at-past",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.InsertedAtRangeFilter(pastMin, pastMax),
				},
			})
			Expect(err).To(Succeed())

			result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-inserted-at-past",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())
			Expect(result.GetCursor().TransactionData).To(BeEmpty())
		})

		It("Should show inserted_at index as READY in GetLedger", func() {
			info, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(info.BuiltinIndexes).NotTo(BeNil())
			Expect(info.BuiltinIndexes.InsertedAt).To(BeTrue())
			Expect(info.BuiltinIndexes.InsertedAtStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		})

		It("Should reject inserted_at filter queries after dropping the index", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.DropBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				_, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-inserted-at",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(HaveOccurred())
				st, ok := status.FromError(err)
				g.Expect(ok).To(BeTrue())
				g.Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// ID filter — no index required
	// ========================================================================
	Context("ID filter (no index required)", Ordered, func() {
		const ledgerName = "idx-id-filter"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 5 transactions — IDs will be 1..5
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a1", big.NewInt(10), "USD")}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a2", big.NewInt(20), "USD")}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a3", big.NewInt(30), "USD")}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a4", big.NewInt(40), "USD")}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{actions.NewPosting("world", "a5", big.NewInt(50), "USD")}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should filter by exact ID", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-id-exact",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.TxIDExactFilter(3),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-id-exact",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(transactionIDs(result.GetCursor().TransactionData)).To(ConsistOf(uint64(3)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter by ID range", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-id-range",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.TxIDRangeFilter(2, 4),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-id-range",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(transactionIDs(result.GetCursor().TransactionData)).To(ConsistOf(uint64(2), uint64(3), uint64(4)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return empty for a non-existent ID", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-id-missing",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.TxIDExactFilter(999),
				},
			})
			Expect(err).To(Succeed())

			result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-id-missing",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())
			Expect(result.GetCursor().TransactionData).To(BeEmpty())
		})
	})
})
