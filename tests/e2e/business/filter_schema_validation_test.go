//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("FilterSchemaValidation", Ordered, func() {

	// ========================================================================
	// Prepared query: type mismatch errors
	// ========================================================================
	Context("Prepared query type mismatch errors", Ordered, func() {
		const ledgerName = "pq-schema-mismatch"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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
					actions.CreateAccountMetadataIndexAction(ledgerName, "age"),
					actions.CreateAccountMetadataIndexAction(ledgerName, "name"),
					actions.CreateAccountMetadataIndexAction(ledgerName, "active"),
				},
			})
			Expect(err).To(Succeed())

			// Wait for indexes to become READY (backfill must complete)
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "name")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "active")).To(Succeed())

			// Create an account so execution has something to scan
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{
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
					Filter: actions.StringMetadataFilter("age", "hello"),
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
					Filter: actions.Int64RangeMetadataFilterExclusive("name", &val, nil, false, false),
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
					Filter: actions.BoolMetadataFilter("age", true),
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
					Filter: actions.ExistsMetadataFilter("age"),
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
				g.Expect(result.GetCursor().AccountData[0].GetAddress()).To(Equal("alice"))
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
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "counter",
							Type:       commonpb.MetadataType_METADATA_TYPE_UINT64,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "counter"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "counter")).To(Succeed())

			// Create accounts with uint64 metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"counter": "10"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"counter": "50"}),
					actions.SaveAccountMetadataAction(ledgerName, "charlie", map[string]string{"counter": "100"}),
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
					Filter: actions.Int64RangeMetadataFilterExclusive("counter", &minVal, nil, false, false),
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

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("bob", "charlie"))
		})

		It("Should error when int filter has negative bound on uint field", func() {
			negVal := int64(-1)
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "counter-neg",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.Int64RangeMetadataFilterExclusive("counter", &negVal, nil, false, false),
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
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
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
					actions.CreateAccountMetadataIndexAction(ledgerName, "score"),
					actions.CreateAccountMetadataIndexAction(ledgerName, "visits"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "visits")).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"score": "80", "visits": "5",
					}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when ListAccounts uses string filter on int field", func() {
			_, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", actions.StringMetadataFilter("score", "high"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should auto-coerce and succeed when ListAccounts uses int filter on uint field", func() {
			minVal := int64(3)
			filter := actions.Int64RangeMetadataFilterExclusive("visits", &minVal, nil, false, false)

			Eventually(func(g Gomega) {
				accounts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", filter)
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
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_TRANSACTION,
							Key:        "priority",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
					actions.CreateTransactionMetadataIndexAction(ledgerName, "priority"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_TRANSACTION, "priority")).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, map[string]string{"priority": "5"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should error when ListTransactions uses string filter on int field", func() {
			_, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, actions.StringMetadataFilter("priority", "high"))
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot use string condition"))
		})

		It("Should succeed with correct int filter on int field", func() {
			val := int64(5)
			filter := actions.Int64RangeMetadataFilterExclusive("priority", &val, &val, false, false)

			Eventually(func(g Gomega) {
				txns, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, filter)
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
					actions.CreateLedgerAction(ledgerName, nil),
					actions.CreateAccountMetadataIndexAction(ledgerName, "anything"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "anything")).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"anything": "hello"}),
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
					Filter: actions.Int64RangeMetadataFilterExclusive("anything", &val, nil, false, false),
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
				accounts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", actions.StringMetadataFilter("anything", "hello"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
