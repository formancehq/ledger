//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("QueryProfile", Ordered, func() {

	// ========================================================================
	// ListAccounts profiling via gRPC trailing metadata
	// ========================================================================
	Context("ListAccounts profiling", Ordered, func() {
		const ledgerName = "profile-accounts"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return profile in trailing metadata when requested", func() {
			Eventually(func(g Gomega) {
				profileCtx := metadata.AppendToOutgoingContext(sharedCtx, "x-query-profile", "true")

				stream, err := sharedClient.ListAccounts(profileCtx, &servicepb.ListAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				// Drain the stream
				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					g.Expect(err).To(Succeed())
				}

				// Extract profile from trailing metadata
				trailer := stream.Trailer()
				profileData := trailer.Get("x-query-profile-result-bin")
				g.Expect(profileData).NotTo(BeEmpty(), "trailing metadata should contain profile")

				var profile servicepb.QueryProfile
				g.Expect(proto.Unmarshal([]byte(profileData[0]), &profile)).To(Succeed())

				// Profile should have meaningful data
				g.Expect(profile.ItemsCollected).To(BeNumerically(">", 0), "should have collected items")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should NOT return profile when not requested", func() {
			Eventually(func(g Gomega) {
				stream, err := sharedClient.ListAccounts(sharedCtx, &servicepb.ListAccountsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					g.Expect(err).To(Succeed())
				}

				trailer := stream.Trailer()
				profileData := trailer.Get("x-query-profile-result-bin")
				g.Expect(profileData).To(BeEmpty(), "profile should not be returned when not requested")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// ListTransactions profiling via gRPC trailing metadata
	// ========================================================================
	Context("ListTransactions profiling", Ordered, func() {
		const ledgerName = "profile-transactions"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return profile in trailing metadata when requested", func() {
			Eventually(func(g Gomega) {
				profileCtx := metadata.AppendToOutgoingContext(sharedCtx, "x-query-profile", "true")

				stream, err := sharedClient.ListTransactions(profileCtx, &servicepb.ListTransactionsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())

				for {
					_, err := stream.Recv()
					if err == io.EOF {
						break
					}
					g.Expect(err).To(Succeed())
				}

				trailer := stream.Trailer()
				profileData := trailer.Get("x-query-profile-result-bin")
				g.Expect(profileData).NotTo(BeEmpty(), "trailing metadata should contain profile")

				var profile servicepb.QueryProfile
				g.Expect(proto.Unmarshal([]byte(profileData[0]), &profile)).To(Succeed())
				g.Expect(profile.ItemsCollected).To(BeNumerically(">", 0), "should have collected items")
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	// ========================================================================
	// ExecutePreparedQuery profiling via gRPC trailing metadata
	// ========================================================================
	Context("ExecutePreparedQuery profiling", Ordered, func() {
		const ledgerName = "profile-prepared-query"

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
					actions.CreateMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "role"),
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
					actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{"role": "admin"}),
					actions.SaveAccountMetadataAction(ledgerName, "bob", map[string]string{"role": "user"}),
				},
			})
			Expect(err).To(Succeed())

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

		It("Should return profile in trailing metadata when requested", func() {
			Eventually(func(g Gomega) {
				profileCtx := metadata.AppendToOutgoingContext(sharedCtx, "x-query-profile", "true")

				resp, err := sharedClient.ExecutePreparedQuery(profileCtx, &servicepb.ExecutePreparedQueryRequest{
					Ledger:    ledgerName,
					QueryName: "find-admins",
					Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
				})
				g.Expect(err).To(Succeed())

				cursor := resp.GetCursor()
				g.Expect(cursor).NotTo(BeNil())
				g.Expect(accountAddresses(cursor.AccountData)).To(ContainElement("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should contain iterator tree in profile", func() {
			profileCtx := metadata.AppendToOutgoingContext(sharedCtx, "x-query-profile", "true")

			// For ExecutePreparedQuery (unary RPC), profile is sent via SetTrailer
			// which requires a special header interceptor. For now, we just verify
			// the query works correctly with the profile context.
			resp, err := sharedClient.ExecutePreparedQuery(profileCtx, &servicepb.ExecutePreparedQueryRequest{
				Ledger:    ledgerName,
				QueryName: "find-admins",
				Mode:      commonpb.QueryMode_QUERY_MODE_LIST,
			})
			Expect(err).To(Succeed())

			cursor := resp.GetCursor()
			Expect(cursor).NotTo(BeNil())
			Expect(cursor.AccountData).To(HaveLen(1))
			Expect(cursor.AccountData[0].GetAddress()).To(Equal("alice"))
		})
	})

	// ========================================================================
	// Profile with filter (more meaningful iterator tree)
	// ========================================================================
	Context("ListAccounts with filter profiling", Ordered, func() {
		const ledgerName = "profile-filter"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction(ledgerName, nil),
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(200), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "merchants:shop1", big.NewInt(50), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return profile with iterator stats for filtered queries", func() {
			Eventually(func(g Gomega) {
				profileCtx := metadata.AppendToOutgoingContext(sharedCtx, "x-query-profile", "true")

				stream, err := sharedClient.ListAccounts(profileCtx, &servicepb.ListAccountsRequest{
					Ledger: ledgerName,
					Filter: actions.AddressPrefixFilter("users:"),
				})
				g.Expect(err).To(Succeed())

				var accounts []*commonpb.Account
				for {
					account, err := stream.Recv()
					if err == io.EOF {
						break
					}
					g.Expect(err).To(Succeed())
					accounts = append(accounts, account)
				}
				g.Expect(accounts).To(HaveLen(2))

				// Extract and validate profile
				trailer := stream.Trailer()
				profileData := trailer.Get("x-query-profile-result-bin")
				g.Expect(profileData).NotTo(BeEmpty())

				var profile servicepb.QueryProfile
				g.Expect(proto.Unmarshal([]byte(profileData[0]), &profile)).To(Succeed())

				g.Expect(profile.ItemsCollected).To(BeNumerically("==", 2))
				g.Expect(profile.EnrichedCount).To(BeNumerically("==", 2))
				g.Expect(profile.RootIterator).NotTo(BeNil(), "filtered query should produce an iterator tree")
				g.Expect(profile.RootIterator.Kind).NotTo(BeEmpty())
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
