//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
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

	// ========================================================================
	// Prepared query: type mismatch errors
	// ========================================================================
	Context("Prepared query type mismatch errors", Ordered, func() {
		const ledgerName = "pq-schema-mismatch"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age"),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "name"),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "active"),
				},
			})
			Expect(err).To(Succeed())

			// Wait for indexes to become READY (backfill must complete)
			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")
			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "name")
			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "active")

			// Create an account so execution has something to scan
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"age": "25", "name": "Alice", "active": "true",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when using string filter on int64 field", func() {
			// Create prepared query: string filter on int64 "age"
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "bad-string-on-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: stringFilter("age", "hello"),
				},
			})
			Expect(err).To(Succeed())

			// Execution should fail with type mismatch error
			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "bad-string-on-int",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should error when using int filter on string field", func() {
			val := int64(42)
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "bad-int-on-string",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("name", &val, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "bad-int-on-string",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use integer condition"))
		})

		It("Should error when using bool filter on int64 field", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "bad-bool-on-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: boolFilter("age", true),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "bad-bool-on-int",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use bool condition"))
		})

		It("Should allow exists filter on any typed field", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "exists-on-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: existsFilter("age"),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				result, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "counter",
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					}),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "counter"),
				},
			})
			Expect(err).To(Succeed())

			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "counter")

			// Create accounts with uint64 metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil),
					testutil.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"counter": "10"}),
					testutil.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"counter": "50"}),
					testutil.SaveAccountMetadataAction(ledgerName, "charlie", map[string]string{"counter": "100"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should auto-coerce int filter to uint and return correct results", func() {
			// Use IntCondition (the parser's default for integer literals) on a uint64 field.
			// The compiler should auto-coerce to UintCondition.
			minVal := int64(30)
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
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
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
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
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "counter-neg",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("counter", &negVal, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score"),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "visits"),
				},
			})
			Expect(err).To(Succeed())

			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")
			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "visits")

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"score": "80", "visits": "5",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when ListAccounts uses string filter on int field", func() {
			_, err := listAllAccounts(sharedCtx, sharedClient, ledgerName, 0, "", stringFilter("score", "high"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should auto-coerce and succeed when ListAccounts uses int filter on uint field", func() {
			minVal := int64(3)
			filter := intFilter("visits", &minVal, nil, false, false)

			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(sharedCtx, sharedClient, ledgerName, 0, "", filter)
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
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "priority",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "priority"),
				},
			})
			Expect(err).To(Succeed())

			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "priority")

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, map[string]string{"priority": "5"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when ListTransactions uses string filter on int field", func() {
			_, err := listAllTransactions(sharedCtx, sharedClient, ledgerName, 0, 0, stringFilter("priority", "high"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should succeed with correct int filter on int field", func() {
			val := int64(5)
			filter := intFilter("priority", &val, &val, false, false)

			Eventually(func(g Gomega) {
				txns, err := listAllTransactions(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
				g.Expect(err).To(Succeed())
				g.Expect(txns).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// No schema: validation is skipped (backward compat)
	// ========================================================================
	Context("Auto-created schema via index — type validation applies", Ordered, func() {
		const ledgerName = "pq-auto-schema"

		BeforeAll(func() {
			// Create ledger without explicit schema, but with a metadata index.
			// Creating a metadata index auto-creates a STRING schema field.
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction(ledgerName, nil),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "anything"),
				},
			})
			Expect(err).To(Succeed())

			waitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "anything")

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					testutil.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"anything": "hello"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should reject int filter on auto-created STRING field", func() {
			// Auto-created schema type is STRING, so int filter should fail with type mismatch
			val := int64(42)
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "auto-schema-int",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: intFilter("anything", &val, nil, false, false),
				},
			})
			Expect(err).To(Succeed())

			// Execution should fail with type mismatch
			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "auto-schema-int",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use integer condition"))
		})

		It("Should allow string filter on ListAccounts with auto-created schema", func() {
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(sharedCtx, sharedClient, ledgerName, 0, "", stringFilter("anything", "hello"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
