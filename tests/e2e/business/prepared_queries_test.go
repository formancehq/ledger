//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"
	"sort"
	"time"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// accountAddresses extracts addresses from a slice of Account objects.
func accountAddresses(accounts []*commonpb.Account) []string {
	addrs := make([]string, len(accounts))
	for i, a := range accounts {
		addrs[i] = a.GetAddress()
	}

	return addrs
}

// transactionIDs extracts IDs from a slice of Transaction objects.
func transactionIDs(txns []*commonpb.Transaction) []uint64 {
	ids := make([]uint64, len(txns))
	for i, tx := range txns {
		ids[i] = tx.GetId()
	}

	return ids
}

var _ = Describe("PreparedQueries", Ordered, func() {

	// ========================================================================
	// CRUD Operations
	// ========================================================================
	Context("CRUD Operations", Ordered, func() {
		const ledgerName = "pq-crud"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())
		})

		It("Should create a prepared query", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "admins",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.StringMetadataFilter("role", "admin"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should list prepared queries and find the created one", func() {
			resp, err := sharedClient.ListPreparedQueries(sharedCtx, &servicepb.ListPreparedQueriesRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.Queries).To(HaveLen(1))
			Expect(resp.Queries[0].Name).To(Equal("admins"))
			Expect(resp.Queries[0].Target).To(Equal(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS))
		})

		It("Should reject duplicate prepared query creation", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "admins",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.StringMetadataFilter("role", "admin"),
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))
		})

		It("Should update the prepared query filter", func() {
			_, err := sharedClient.UpdatePreparedQuery(sharedCtx, &servicepb.UpdatePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   "admins",
				Filter: actions.StringMetadataFilter("role", "superadmin"),
			})
			Expect(err).To(Succeed())
		})

		It("Should list and verify the updated filter", func() {
			resp, err := sharedClient.ListPreparedQueries(sharedCtx, &servicepb.ListPreparedQueriesRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.Queries).To(HaveLen(1))
			fieldCond := resp.Queries[0].Filter.GetField()
			Expect(fieldCond).NotTo(BeNil())
			Expect(fieldCond.GetStringCond().GetHardcoded()).To(Equal("superadmin"))
		})

		It("Should delete the prepared query", func() {
			_, err := sharedClient.DeletePreparedQuery(sharedCtx, &servicepb.DeletePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   "admins",
			})
			Expect(err).To(Succeed())
		})

		It("Should list empty after deletion", func() {
			resp, err := sharedClient.ListPreparedQueries(sharedCtx, &servicepb.ListPreparedQueriesRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.Queries).To(BeEmpty())
		})

		It("Should return NOT_FOUND when deleting a non-existent query", func() {
			_, err := sharedClient.DeletePreparedQuery(sharedCtx, &servicepb.DeletePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   "nonexistent",
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})

	// ========================================================================
	// Execute LIST mode — account metadata string filter
	// ========================================================================
	Context("Execute LIST mode — account metadata string filter", Ordered, func() {
		const ledgerName = "pq-exec-string"

		BeforeAll(func() {
			// Create ledger with schema and index
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())

			// Create transactions to establish accounts
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
				},
			})
			Expect(err).To(Succeed())

			// Set metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user"}),
					actions.SaveAccountMetadataAction(ledgerName, "charlie", map[string]string{"role": "admin"}),
				},
			})
			Expect(err).To(Succeed())

			// Create prepared query
			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "find-admins",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.StringMetadataFilter("role", "admin"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return accounts matching the filter", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "find-admins",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				cursor := result.GetCursor()
				g.Expect(cursor).NotTo(BeNil())
				g.Expect(cursor.AccountData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			addrs := accountAddresses(result.GetCursor().AccountData)
			sort.Strings(addrs)
			Expect(addrs).To(Equal([]string{"alice", "charlie"}))
		})

		It("Should paginate with page_size=1", func() {
			var firstPage *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				firstPage, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "find-admins",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
					PageSize:  1,
				})
				g.Expect(err).To(Succeed())
				cursor := firstPage.GetCursor()
				g.Expect(cursor).NotTo(BeNil())
				g.Expect(cursor.AccountData).To(HaveLen(1))
				g.Expect(cursor.HasMore).To(BeTrue())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			cursor := firstPage.GetCursor()
			Expect(cursor.AccountData[0].GetAddress()).To(Equal("alice"))
			Expect(cursor.Next).NotTo(BeEmpty())

			// Fetch second page using cursor
			secondPage, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "find-admins",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				PageSize:  1,
				Cursor:    cursor.Next,
			})
			Expect(err).To(Succeed())
			cursor2 := secondPage.GetCursor()
			Expect(cursor2).NotTo(BeNil())
			Expect(cursor2.AccountData).To(HaveLen(1))
			Expect(cursor2.AccountData[0].GetAddress()).To(Equal("charlie"))
			Expect(cursor2.HasMore).To(BeFalse())
		})
	})

	// ========================================================================
	// Execute LIST mode — address prefix filter
	// ========================================================================
	Context("Execute LIST mode — address prefix filter", Ordered, func() {
		const ledgerName = "pq-exec-addr-prefix"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(200), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "merchants:shop1", big.NewInt(300), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "users-by-prefix",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.AddressPrefixFilter("users:"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return only accounts matching the address prefix", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "users-by-prefix",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				cursor := result.GetCursor()
				g.Expect(cursor).NotTo(BeNil())
				g.Expect(cursor.AccountData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			addrs := accountAddresses(result.GetCursor().AccountData)
			sort.Strings(addrs)
			Expect(addrs).To(Equal([]string{"users:alice", "users:bob"}))
		})
	})

	// ========================================================================
	// Execute LIST mode — AND/OR/NOT filters
	// ========================================================================
	Context("Execute LIST mode — AND/OR/NOT filters", Ordered, func() {
		const ledgerName = "pq-exec-logic"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "tier",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
					actions.CreateAccountMetadataIndexAction(ledgerName, "tier"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")).To(Succeed())

			// Create accounts
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
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "diana", big.NewInt(400), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Set metadata: alice(admin,premium), bob(user,premium), charlie(admin,basic), diana(user,basic)
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin", "tier": "premium"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user", "tier": "premium"}),
					actions.SaveAccountMetadataAction(ledgerName, "charlie", map[string]string{"role": "admin", "tier": "basic"}),
					actions.SaveAccountMetadataAction(ledgerName, "diana", map[string]string{"role": "user", "tier": "basic"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("AND: should return intersection (admin AND premium)", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "admin-premium",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.AndFilter(
						actions.StringMetadataFilter("role", "admin"),
						actions.StringMetadataFilter("tier", "premium"),
					),
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "admin-premium",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice"))
		})

		It("OR: should return union (admin OR user = all with role)", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "admin-or-user",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.OrFilter(
						actions.StringMetadataFilter("role", "admin"),
						actions.StringMetadataFilter("role", "user"),
					),
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "admin-or-user",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(4))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice", "bob", "charlie", "diana"))
		})

		It("NOT: should return complement (NOT admin = user accounts)", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "not-admin",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.NotFilter(actions.StringMetadataFilter("role", "admin")),
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "not-admin",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				// NOT admin returns all accounts that are not admin (bob, diana, world)
				g.Expect(result.GetCursor().AccountData).To(HaveLen(3))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ContainElements("bob", "diana"))
		})
	})

	// ========================================================================
	// Execute LIST mode — parameterized filter
	// ========================================================================
	Context("Execute LIST mode — parameterized filter", Ordered, func() {
		const ledgerName = "pq-exec-params"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())

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

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user"}),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "by-role",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.ParamStringMetadataFilter("role", "role_value"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return admin accounts when param role_value=admin", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:     ledgerName,
					QueryName:  "by-role",
					Mode:       commonpb.QueryMode_QUERY_MODE_LIST,
					Parameters: map[string]*commonpb.ParameterValue{"role_value": actions.StringParam("admin")},
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice"))
		})

		It("Should return user accounts when param role_value=user", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:     ledgerName,
					QueryName:  "by-role",
					Mode:       commonpb.QueryMode_QUERY_MODE_LIST,
					Parameters: map[string]*commonpb.ParameterValue{"role_value": actions.StringParam("user")},
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("bob"))
		})

		// #249 regression: a CLI that always sends string values must work
		// against a string-typed param even when the raw string looks like
		// an int. Pre-fix, the CLI inferred "0000...0" as int64(0) and the
		// server rejected with "expected string, got int64". Post-fix, the
		// CLI sends StringValue and extractString accepts it unchanged.
		It("Should accept a string param whose value looks like a number", func() {
			// Add a third account with a numeric-looking role so we can
			// verify the lookup actually returns it.
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "carol", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "carol", map[string]string{"role": "0000"}),
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "by-role",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
					Parameters: map[string]*commonpb.ParameterValue{
						"role_value": actions.StringParam("0000"),
					},
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("carol"))
		})
	})

	// ========================================================================
	// #249: server-side coercion of string params to typed scalars.
	// The CLI sends every --param value as a StringValue because it doesn't
	// know the target type. extractInt64 / extractUint64 / extractBool must
	// parse the string into the declared scalar at compile time.
	// ========================================================================
	Context("Execute LIST mode — string param coerced to int64 range filter", Ordered, func() {
		const ledgerName = "pq-exec-coerce-int"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "score",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "score"),
				},
			})
			Expect(err).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "score")).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(100), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"score": "42"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"score": "100"}),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "score-in-range",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.ParamInt64RangeMetadataFilter("score", "min", "max"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should coerce string params to int64 when the filter declares int64 range", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				// Note: StringParam, not Int64Param. This is what the CLI
				// sends post-fix — server's extractInt64 must coerce.
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "score-in-range",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
					Parameters: map[string]*commonpb.ParameterValue{
						"min": actions.StringParam("40"),
						"max": actions.StringParam("50"),
					},
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice"))
		})

		It("Should reject a string param that doesn't parse as int", func() {
			_, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "score-in-range",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				Parameters: map[string]*commonpb.ParameterValue{
					"min": actions.StringParam("not-a-number"),
					"max": actions.StringParam("50"),
				},
			})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("cannot parse"))
		})
	})

	// ========================================================================
	// Execute LIST mode — TRANSACTIONS target
	// ========================================================================
	Context("Execute LIST mode — TRANSACTIONS target", Ordered, func() {
		const ledgerName = "pq-exec-tx"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY)).To(Succeed())

			// tx0: world→alice, tx1: world→bob, tx2: alice→charlie
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("alice", "charlie", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "alice-txs",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.AddressExactFilter("alice"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return transactions involving alice", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "alice-txs",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().TransactionData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			txIDs := transactionIDs(result.GetCursor().TransactionData)
			sort.Slice(txIDs, func(i, j int) bool { return txIDs[i] < txIDs[j] })
			// tx1 (world→alice) and tx3 (alice→charlie) should be returned
			// (transaction IDs are 1-based)
			Expect(txIDs).To(HaveLen(2))
			Expect(txIDs[0]).To(Equal(uint64(1)))
			Expect(txIDs[1]).To(Equal(uint64(3)))
		})
	})

	// ========================================================================
	// Execute AGGREGATE_VOLUMES mode
	// ========================================================================
	Context("Execute AGGREGATE_VOLUMES mode", Ordered, func() {
		const ledgerName = "pq-exec-agg"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())

			// world→alice 100 USD, world→alice 50 EUR, world→bob 200 USD
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(50), "EUR"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user"}),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "admin-volumes",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.StringMetadataFilter("role", "admin"),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return aggregated volumes for admin accounts", func() {
			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var err error
				result, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "admin-volumes",
					Mode:      commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
				})
				g.Expect(err).To(Succeed())
				agg := result.GetAggregate()
				g.Expect(agg).NotTo(BeNil())
				g.Expect(agg.Volumes).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			agg := result.GetAggregate()

			// Build a map of asset → volumes for easier assertion
			volumesByAsset := make(map[string]*commonpb.AggregatedVolume)
			for _, v := range agg.Volumes {
				volumesByAsset[v.Asset] = v
			}

			// alice received 100 USD (input=100, output=0)
			usdVol, ok := volumesByAsset["USD"]
			Expect(ok).To(BeTrue(), "expected USD volumes")
			Expect(usdVol.Input.ToBigInt().Int64()).To(Equal(int64(100)))
			Expect(usdVol.Output.ToBigInt().Int64()).To(Equal(int64(0)))

			// alice received 50 EUR (input=50, output=0)
			eurVol, ok := volumesByAsset["EUR"]
			Expect(ok).To(BeTrue(), "expected EUR volumes")
			Expect(eurVol.Input.ToBigInt().Int64()).To(Equal(int64(50)))
			Expect(eurVol.Output.ToBigInt().Int64()).To(Equal(int64(0)))
		})
	})

	// ========================================================================
	// Error handling
	// ========================================================================
	Context("Error handling", Ordered, func() {
		const ledgerName = "pq-errors"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())
		})

		It("Should return NOT_FOUND when executing a non-existent query", func() {
			_, err := sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "nonexistent",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should return error when missing a required parameter", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "param-query",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: actions.ParamStringMetadataFilter("role", "role_value"),
				},
			})
			Expect(err).To(Succeed())

			// Execute without providing the required parameter
			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "param-query",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should return error for AGGREGATE_VOLUMES on TRANSACTIONS target", func() {
			_, err := sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "tx-query",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: actions.AddressExactFilter("alice"),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "tx-query",
				Mode:      commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should return NOT_FOUND when updating a non-existent query", func() {
			_, err := sharedClient.UpdatePreparedQuery(sharedCtx, &servicepb.UpdatePreparedQueryRequest{
				Ledger: ledgerName,
				Name:   "does-not-exist",
				Filter: actions.StringMetadataFilter("role", "admin"),
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})

	// ========================================================================
	// Execute LIST mode — metadata in filter (desugared from "in" operator)
	// ========================================================================
	Context("Execute LIST mode — metadata in filter", Ordered, func() {
		const ledgerName = "pq-exec-in"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "role",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "tier",
							Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "role"),
					actions.CreateAccountMetadataIndexAction(ledgerName, "tier"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role")).To(Succeed())
			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")).To(Succeed())

			// alice(admin,gold), bob(user,silver), charlie(admin,bronze), diana(viewer,gold)
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
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "diana", big.NewInt(400), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin", "tier": "gold"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user", "tier": "silver"}),
					actions.SaveAccountMetadataAction(ledgerName, "charlie", map[string]string{"role": "admin", "tier": "bronze"}),
					actions.SaveAccountMetadataAction(ledgerName, "diana", map[string]string{"role": "viewer", "tier": "gold"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return accounts matching any role in the list", func() {
			// metadata[role] in (admin, viewer) → alice, charlie, diana
			filter, err := filterexpr.Parse(`metadata[role] in (admin, viewer)`)
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "roles-in",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: filter,
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var execErr error
				result, execErr = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "roles-in",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(execErr).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(3))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice", "charlie", "diana"))
		})

		It("Should combine in filter with AND on different fields", func() {
			// metadata[role] in (admin, viewer) and metadata[tier] in (gold) → alice, diana
			filter, err := filterexpr.Parse(`metadata[role] in (admin, viewer) and metadata[tier] in (gold)`)
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "roles-in-and-tier-in",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: filter,
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var execErr error
				result, execErr = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "roles-in-and-tier-in",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(execErr).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice", "diana"))
		})

		It("Should support in with quoted string values", func() {
			// metadata[tier] in ("gold", "silver") → alice, bob, diana
			filter, err := filterexpr.Parse(`metadata[tier] in ("gold", "silver")`)
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "tier-in-quoted",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: filter,
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var execErr error
				result, execErr = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "tier-in-quoted",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(execErr).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(3))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("alice", "bob", "diana"))
		})
	})

	// ========================================================================
	// Execute LIST mode — address in filter on transactions
	// ========================================================================
	Context("Execute LIST mode — address in filter on transactions", Ordered, func() {
		const ledgerName = "pq-exec-addr-in"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
					actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY)).To(Succeed())

			// tx0: world→alice, tx1: world→bob, tx2: alice→charlie
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil),
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("alice", "charlie", big.NewInt(50), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return transactions involving any address in the list", func() {
			// address in ("alice", "charlie") → tx0 (world→alice), tx2 (alice→charlie)
			filter, err := filterexpr.Parse(`address in ("alice", "charlie")`)
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "addr-in-txs",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
					Filter: filter,
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var execErr error
				result, execErr = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "addr-in-txs",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(execErr).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().TransactionData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			txIDs := transactionIDs(result.GetCursor().TransactionData)
			sort.Slice(txIDs, func(i, j int) bool { return txIDs[i] < txIDs[j] })
			Expect(txIDs[0]).To(Equal(uint64(1))) // tx0: world→alice
			Expect(txIDs[1]).To(Equal(uint64(3))) // tx2: alice→charlie
		})
	})

	Context("Execute LIST mode — between filter", Ordered, func() {
		const ledgerName = "pq-exec-between"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "age",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
					actions.CreateAccountMetadataIndexAction(ledgerName, "age"),
				},
			})
			Expect(err).To(Succeed())

			Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")).To(Succeed())

			// alice=20, bob=35, charlie=50, dave=65
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
					actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "dave", big.NewInt(400), "USD"),
					}, nil),
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"age": "20"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"age": "35"}),
					actions.SaveAccountMetadataAction(ledgerName, "charlie", map[string]string{"age": "50"}),
					actions.SaveAccountMetadataAction(ledgerName, "dave", map[string]string{"age": "65"}),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should store a between filter in a prepared query and execute it", func() {
			// `between 30 and 60` matches bob(35) and charlie(50).
			// Proves the IntCondition survives store + retrieve through the
			// prepared-query proto round trip.
			filter, err := filterexpr.Parse("metadata[age] between 30 and 60")
			Expect(err).To(Succeed())

			_, err = sharedClient.CreatePreparedQuery(sharedCtx, &servicepb.CreatePreparedQueryRequest{
				Query: &commonpb.PreparedQuery{
					Name:   "age-between-30-60",
					Ledger: ledgerName,
					Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
					Filter: filter,
				},
			})
			Expect(err).To(Succeed())

			var result *servicepb.ExecutePreparedQueryResponse
			Eventually(func(g Gomega) {
				var execErr error
				result, execErr = sharedClient.ExecutePreparedQuery(sharedCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "age-between-30-60",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(execErr).To(Succeed())
				g.Expect(result.GetCursor()).NotTo(BeNil())
				g.Expect(result.GetCursor().AccountData).To(HaveLen(2))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			Expect(accountAddresses(result.GetCursor().AccountData)).To(ConsistOf("bob", "charlie"))
		})
	})
})
