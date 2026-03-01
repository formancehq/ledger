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

// intFilter creates a QueryFilter for a metadata integer range condition.
func intFilter(metaKey string, min, max *int64, minExclusive, maxExclusive bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: metaKey},
				Condition: &commonpb.FieldCondition_IntCond{
					IntCond: &commonpb.IntCondition{
						Min:          min,
						Max:          max,
						MinExclusive: minExclusive,
						MaxExclusive: maxExclusive,
					},
				},
			},
		},
	}
}

// boolFilter creates a QueryFilter for a metadata boolean condition.
func boolFilter(metaKey string, value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field: &commonpb.FieldRef{Metadata: metaKey},
				Condition: &commonpb.FieldCondition_BoolCond{
					BoolCond: &commonpb.BoolCondition{
						Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: value},
					},
				},
			},
		},
	}
}

// existsFilter creates a QueryFilter that checks for metadata key existence.
func existsFilter(metaKey string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: metaKey},
				Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
			},
		},
	}
}

var _ = Describe("FilterSchemaValidation", Ordered, func() {
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
	// Prepared query: type mismatch errors
	// ========================================================================
	Context("Prepared query type mismatch errors", Ordered, func() {
		const ledgerName = "pq-schema-mismatch"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "age",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "name",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "active",
							Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			// Create an account so execution has something to scan
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"age": "25", "name": "Alice", "active": "true",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when using string filter on int64 field", func() {
			// Create prepared query: string filter on int64 "age"
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "bad-string-on-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: stringFilter("age", "hello"),
				},
			})
			Expect(err).To(Succeed())

			// Execution should fail with type mismatch error
			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "bad-string-on-int",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should error when using int filter on string field", func() {
			val := int64(42)
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "bad-int-on-string",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("name", &val, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "bad-int-on-string",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use integer condition"))
		})

		It("Should error when using bool filter on int64 field", func() {
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "bad-bool-on-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: boolFilter("age", true),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "bad-bool-on-int",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use bool condition"))
		})

		It("Should allow exists filter on any typed field", func() {
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "exists-on-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: existsFilter("age"),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "exists-on-int",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
				g.Expect(result.GetCursor().AccountData[0]).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// Prepared query: int-to-uint auto-coercion
	// ========================================================================
	Context("Prepared query int-to-uint auto-coercion", Ordered, func() {
		const ledgerName = "pq-schema-coerce"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "counter",
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			// Create accounts with uint64 metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"counter": "10"}),
					saveAccountMetadataAction(ledgerName, "bob", map[string]string{"counter": "50"}),
					saveAccountMetadataAction(ledgerName, "charlie", map[string]string{"counter": "100"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should auto-coerce int filter to uint and return correct results", func() {
			// Use IntCondition (the parser's default for integer literals) on a uint64 field.
			// The compiler should auto-coerce to UintCondition.
			minVal := int64(30)
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "counter-gte-30",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("counter", &minVal, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "counter-gte-30",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(result.GetCursor().AccountData).To(ConsistOf("bob", "charlie"))
		})

		It("Should error when int filter has negative bound on uint field", func() {
			negVal := int64(-1)
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "counter-neg",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("counter", &negVal, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "counter-neg",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("unsigned"))
			Expect(err.Error()).To(ContainSubstring("negative"))
		})
	})

	// ========================================================================
	// ListAccounts: schema validation through direct filter
	// ========================================================================
	Context("ListAccounts schema validation", Ordered, func() {
		const ledgerName = "list-schema-val"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "score",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "visits",
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"score": "80", "visits": "5",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when ListAccounts uses string filter on int field", func() {
			_, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("score", "high"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should auto-coerce and succeed when ListAccounts uses int filter on uint field", func() {
			minVal := int64(3)
			filter := intFilter("visits", &minVal, nil, false, false)

			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// ListTransactions: schema validation through direct filter
	// ========================================================================
	Context("ListTransactions schema validation", Ordered, func() {
		const ledgerName = "list-tx-schema-val"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "priority",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
				},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, map[string]string{"priority": "5"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when ListTransactions uses string filter on int field", func() {
			_, err := listAllTransactions(ctx, client, ledgerName, 0, 0, stringFilter("priority", "high"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should succeed with correct int filter on int field", func() {
			val := int64(5)
			filter := intFilter("priority", &val, &val, false, false)

			Eventually(func(g Gomega) {
				txns, err := listAllTransactions(ctx, client, ledgerName, 0, 0, filter)
				g.Expect(err).To(Succeed())
				g.Expect(txns).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// No schema: validation is skipped (backward compat)
	// ========================================================================
	Context("No schema — validation skipped", Ordered, func() {
		const ledgerName = "pq-no-schema"

		BeforeAll(func() {
			// Create ledger WITHOUT schema
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"anything": "hello"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should allow any filter type on unschema'd fields", func() {
			// Use int filter on a field that has no schema — should NOT error
			val := int64(42)
			_, err := client.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "no-schema-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("anything", &val, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			// Execution should succeed (no type validation), even if it finds no results
			_, err = client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "no-schema-int",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())
		})

		It("Should allow string filter on ListAccounts without schema", func() {
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", stringFilter("anything", "hello"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
