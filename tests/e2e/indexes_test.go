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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createBuiltinIndexAction creates a request for creating a builtin transaction index.
func createBuiltinIndexAction(ledger string, index commonpb.TransactionBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_Builtin{
					Builtin: index,
				},
			},
		},
	}
}

// dropBuiltinIndexAction creates a request for dropping a builtin transaction index.
func dropBuiltinIndexAction(ledger string, index commonpb.TransactionBuiltinIndex) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DropIndex{
			DropIndex: &servicepb.DropIndexRequest{
				Ledger: ledger,
				Index: &servicepb.DropIndexRequest_Builtin{
					Builtin: index,
				},
			},
		},
	}
}

// createForceTransactionWithRefAction creates a force transaction with a reference.
func createForceTransactionWithRefAction(ledgerName string, postings []*commonpb.Posting, reference string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings:  postings,
						Force:     true,
						Reference: reference,
					},
				},
			},
		},
	}
}

// waitForBuiltinIndexReady waits until a builtin index reaches READY status.
func waitForBuiltinIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, index commonpb.TransactionBuiltinIndex) {
	Eventually(func(g Gomega) {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		g.Expect(err).To(Succeed())
		g.Expect(info.BuiltinIndexes).NotTo(BeNil())
		switch index {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
			g.Expect(info.BuiltinIndexes.ReferenceStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
			g.Expect(info.BuiltinIndexes.TimestampStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		}
	}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
}

// createIndexAction creates a request for creating an index on a ledger.
func createAddressIndexAction(ledger string, role commonpb.AddressRole) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_AddressRole{
					AddressRole: role,
				},
			},
		},
	}
}

// createMetadataIndexAction creates a request for creating a metadata index.
func createMetadataIndexAction(ledger string, target commonpb.TargetType, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateIndex{
			CreateIndex: &servicepb.CreateIndexRequest{
				Ledger: ledger,
				Index: &servicepb.CreateIndexRequest_Metadata{
					Metadata: &commonpb.MetadataIndexTarget{
						Target: target,
						Key:    key,
					},
				},
			},
		},
	}
}

// dropAddressIndexAction creates a request for dropping an address index.
func dropAddressIndexAction(ledger string, role commonpb.AddressRole) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DropIndex{
			DropIndex: &servicepb.DropIndexRequest{
				Ledger: ledger,
				Index: &servicepb.DropIndexRequest_AddressRole{
					AddressRole: role,
				},
			},
		},
	}
}

// dropMetadataIndexAction creates a request for dropping a metadata index.
func dropMetadataIndexAction(ledger string, target commonpb.TargetType, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DropIndex{
			DropIndex: &servicepb.DropIndexRequest{
				Ledger: ledger,
				Index: &servicepb.DropIndexRequest_Metadata{
					Metadata: &commonpb.MetadataIndexTarget{
						Target: target,
						Key:    key,
					},
				},
			},
		},
	}
}

// waitForMetadataIndexReady waits until a metadata index reaches READY status.
func waitForMetadataIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, target commonpb.TargetType, key string) {
	Eventually(func(g Gomega) {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		g.Expect(err).To(Succeed())
		g.Expect(info.MetadataSchema).NotTo(BeNil())
		var fields map[string]*commonpb.MetadataFieldSchema
		switch target {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			fields = info.MetadataSchema.AccountFields
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			fields = info.MetadataSchema.TransactionFields
		}
		g.Expect(fields).To(HaveKey(key))
		g.Expect(fields[key].IndexBuildStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
	}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
}

// waitForAddressIndexReady waits until an address index reaches READY status.
func waitForAddressIndexReady(ctx context.Context, client servicepb.BucketServiceClient, ledger string, role commonpb.AddressRole) {
	Eventually(func(g Gomega) {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		g.Expect(err).To(Succeed())
		g.Expect(info.AddressIndexes).NotTo(BeNil())
		switch role {
		case commonpb.AddressRole_ADDRESS_ROLE_ANY:
			g.Expect(info.AddressIndexes.AddressStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
			g.Expect(info.AddressIndexes.SourceStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
			g.Expect(info.AddressIndexes.DestinationStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		}
	}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
}

var _ = Describe("UserConfigurableIndexes", Ordered, func() {
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
	// Metadata index lifecycle: create → query → drop
	// ========================================================================
	Context("Metadata index lifecycle", Ordered, func() {
		const ledgerName = "idx-metadata"

		BeforeAll(func() {
			// Create ledger with schema but no indexes
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
		})

		It("Should reject queries on non-indexed metadata fields", func() {
			// Create a prepared query that filters on a non-indexed field
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "category-filter",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: stringFilter("category", "premium"),
				},
			})
			Expect(err).To(Succeed())

			// Execution should fail with index not found
			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "category-filter",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := extractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should succeed after creating index and data", func() {
			// Create the metadata index, then add data (index builder only indexes forward)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category"),
				},
			})
			Expect(err).To(Succeed())

			// Create data AFTER the index exists
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"category": "premium"}),
				},
			})
			Expect(err).To(Succeed())

			// Wait for the index builder to catch up and query to succeed
			Eventually(func(g Gomega) {
				result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "category-filter",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
				g.Expect(result.GetCursor().AccountData[0]).To(Equal("alice"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should show index in GetLedger response", func() {
			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(ledger.MetadataSchema).NotTo(BeNil())
			field, ok := ledger.MetadataSchema.AccountFields["category"]
			Expect(ok).To(BeTrue())
			Expect(field.Indexed).To(BeTrue())
		})

		It("Should reject queries after dropping the index", func() {
			// Drop the metadata index
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					dropMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "category"),
				},
			})
			Expect(err).To(Succeed())

			// Query should fail again
			Eventually(func(g Gomega) {
				_, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
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
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should create and use address index", func() {
			// Create address index (any role)
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
				},
			})
			Expect(err).To(Succeed())

			// Verify GetLedger shows the index
			Eventually(func(g Gomega) {
				ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(ledger.AddressIndexes).NotTo(BeNil())
				g.Expect(ledger.AddressIndexes.Address).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should create source and destination indexes", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_SOURCE),
					createAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(ledger.AddressIndexes).NotTo(BeNil())
				g.Expect(ledger.AddressIndexes.Source).To(BeTrue())
				g.Expect(ledger.AddressIndexes.Destination).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should drop address index", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					dropAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(ledger.AddressIndexes).NotTo(BeNil())
				g.Expect(ledger.AddressIndexes.Address).To(BeFalse())
				// Source and destination should still be enabled
				g.Expect(ledger.AddressIndexes.Source).To(BeTrue())
				g.Expect(ledger.AddressIndexes.Destination).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Reference builtin index lifecycle
	// ========================================================================
	Context("Reference index lifecycle", Ordered, func() {
		const ledgerName = "idx-builtin-ref"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject reference filter queries when index does not exist", func() {
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-reference",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: referenceFilter("pay-001"),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-reference",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			Expect(extractGRPCErrorInfo(err).Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should create reference index and query transactions by reference", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createBuiltinIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionWithRefAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, "pay-001"),
					createForceTransactionWithRefAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, "pay-002"),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "charlie", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			waitForBuiltinIndexReady(ctx, client, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)

			Eventually(func(g Gomega) {
				result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-reference",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor().TransactionData).To(HaveLen(1))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should show reference index as READY in GetLedger", func() {
			info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(info.BuiltinIndexes).NotTo(BeNil())
			Expect(info.BuiltinIndexes.Reference).To(BeTrue())
			Expect(info.BuiltinIndexes.ReferenceStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		})

		It("Should reject reference filter queries after dropping the index", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					dropBuiltinIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				_, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
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

			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject timestamp filter queries when index does not exist", func() {
			minTs, maxTs := uint64(ts1.UnixMicro()), uint64(ts3.UnixMicro())
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-timestamp",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: timestampRangeFilter(minTs, maxTs),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-timestamp",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			Expect(extractGRPCErrorInfo(err).Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should create timestamp index and query transactions by time range", func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createBuiltinIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					withTimestamp(createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "a", big.NewInt(10), "USD")}, nil), ts1),
					withTimestamp(createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "b", big.NewInt(20), "USD")}, nil), ts2),
					withTimestamp(createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "c", big.NewInt(30), "USD")}, nil), ts3),
				},
			})
			Expect(err).To(Succeed())

			waitForBuiltinIndexReady(ctx, client, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)

			// Full range ts1..ts3 → 3 transactions
			Eventually(func(g Gomega) {
				result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
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
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-timestamp-narrow",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: timestampRangeFilter(minTs, maxTs),
				},
			})
			Expect(err).To(Succeed())

			result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-timestamp-narrow",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())
			Expect(result.GetCursor().TransactionData).To(HaveLen(2))
		})

		It("Should show timestamp index as READY in GetLedger", func() {
			info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			Expect(info.BuiltinIndexes).NotTo(BeNil())
			Expect(info.BuiltinIndexes.Timestamp).To(BeTrue())
			Expect(info.BuiltinIndexes.TimestampStatus).To(Equal(commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY))
		})
	})

	// ========================================================================
	// ID filter — no index required
	// ========================================================================
	Context("ID filter (no index required)", Ordered, func() {
		const ledgerName = "idx-id-filter"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 5 transactions — IDs will be 1..5
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "a1", big.NewInt(10), "USD")}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "a2", big.NewInt(20), "USD")}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "a3", big.NewInt(30), "USD")}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "a4", big.NewInt(40), "USD")}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{newPosting("world", "a5", big.NewInt(50), "USD")}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should filter by exact ID", func() {
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-id-exact",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: txIDExactFilter(3),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-id-exact",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor().TransactionData).To(ConsistOf(uint64(3)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter by ID range", func() {
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-id-range",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: txIDRangeFilter(2, 4),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-id-range",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor().TransactionData).To(ConsistOf(uint64(2), uint64(3), uint64(4)))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return empty for a non-existent ID", func() {
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-id-missing",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: txIDExactFilter(999),
				},
			})
			Expect(err).To(Succeed())

			result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "by-id-missing",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())
			Expect(result.GetCursor().TransactionData).To(BeEmpty())
		})
	})
})
