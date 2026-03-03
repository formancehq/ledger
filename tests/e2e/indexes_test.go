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
})
