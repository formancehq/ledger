//go:build e2e

package e2e

import (
	"context"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// prefixFilter builds a QueryFilter for an address prefix match.
func prefixFilter(prefix string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Address{
			Address: &commonpb.AddressMatch{
				Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
			},
		},
	}
}

// listAllAccounts collects all accounts from the streaming RPC into a slice
func listAllAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, afterAddress string, filter *commonpb.QueryFilter) ([]*commonpb.Account, error) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{
		Ledger:       ledgerName,
		PageSize:     pageSize,
		AfterAddress: afterAddress,
		Filter:       filter,
	})
	if err != nil {
		return nil, err
	}

	var accounts []*commonpb.Account
	for {
		account, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

var _ = Describe("Accounts", Ordered, func() {
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

	Context("When listing accounts", Ordered, func() {
		var ledgerName = "accounts-list-ledger"

		BeforeAll(func() {
			// Create ledger
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions that touch multiple accounts
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should list all accounts", func() {
			// The bbolt read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
				g.Expect(err).To(Succeed())
				// world + alice + bob + charlie = 4 accounts
				g.Expect(accounts).To(HaveLen(4))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should return accounts in alphabetical order", func() {
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(4))

			// Alphabetical order: alice, bob, charlie, world
			Expect(accounts[0].Address).To(Equal("alice"))
			Expect(accounts[1].Address).To(Equal("bob"))
			Expect(accounts[2].Address).To(Equal("charlie"))
			Expect(accounts[3].Address).To(Equal("world"))
		})

		It("Should respect page size limit", func() {
			accounts, err := listAllAccounts(ctx, client, ledgerName, 2, "", nil)
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(2))

			// First 2 alphabetically: alice, bob
			Expect(accounts[0].Address).To(Equal("alice"))
			Expect(accounts[1].Address).To(Equal("bob"))
		})

		It("Should paginate with afterAddress", func() {
			// First page: 2 accounts
			firstPage, err := listAllAccounts(ctx, client, ledgerName, 2, "", nil)
			Expect(err).To(Succeed())
			Expect(firstPage).To(HaveLen(2))

			// Second page: after the last account from first page
			secondPage, err := listAllAccounts(ctx, client, ledgerName, 2, firstPage[1].Address, nil)
			Expect(err).To(Succeed())
			Expect(secondPage).To(HaveLen(2))

			// Verify no overlap between pages
			for _, a1 := range firstPage {
				for _, a2 := range secondPage {
					Expect(a1.Address).NotTo(Equal(a2.Address))
				}
			}

			// Third page: should be empty
			thirdPage, err := listAllAccounts(ctx, client, ledgerName, 2, secondPage[1].Address, nil)
			Expect(err).To(Succeed())
			Expect(thirdPage).To(BeEmpty())
		})

		It("Should return empty list for empty ledger", func() {
			emptyLedgerName := "accounts-list-empty"
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(emptyLedgerName, nil)},
			})
			Expect(err).To(Succeed())

			accounts, err := listAllAccounts(ctx, client, emptyLedgerName, 0, "", nil)
			Expect(err).To(Succeed())
			Expect(accounts).To(BeEmpty())
		})

		It("Should return error for non-existent ledger", func() {
			_, err := listAllAccounts(ctx, client, "non-existent-ledger", 0, "", nil)
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})

		It("Should include account metadata", func() {
			// Add metadata to an account
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{
						"role": "admin",
						"tier": "premium",
					}),
				},
			})
			Expect(err).To(Succeed())

			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
			Expect(err).To(Succeed())

			// Find alice in the list
			var aliceAccount *commonpb.Account
			for _, a := range accounts {
				if a.Address == "alice" {
					aliceAccount = a
					break
				}
			}
			Expect(aliceAccount).NotTo(BeNil())
			Expect(aliceAccount.Metadata).NotTo(BeNil())
			Expect(aliceAccount.Metadata.ToMap()["role"]).To(Equal("admin"))
			Expect(aliceAccount.Metadata.ToMap()["tier"]).To(Equal("premium"))
		})
	})

	Context("When listing accounts with prefix filter", Ordered, func() {
		var ledgerName = "accounts-prefix-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create accounts with different prefixes
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "users:bob", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "merchants:shop1", big.NewInt(300), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "merchants:shop2", big.NewInt(400), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should filter accounts by prefix", func() {
			// The bbolt read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("users:"))
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(2))
				g.Expect(accounts[0].Address).To(Equal("users:alice"))
				g.Expect(accounts[1].Address).To(Equal("users:bob"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter merchants by prefix", func() {
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("merchants:"))
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(2))
			Expect(accounts[0].Address).To(Equal("merchants:shop1"))
			Expect(accounts[1].Address).To(Equal("merchants:shop2"))
		})

		It("Should return empty list for non-matching prefix", func() {
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", prefixFilter("nonexistent:"))
			Expect(err).To(Succeed())
			Expect(accounts).To(BeEmpty())
		})

		It("Should combine prefix filter with pagination", func() {
			// Get first page of users
			firstPage, err := listAllAccounts(ctx, client, ledgerName, 1, "", prefixFilter("users:"))
			Expect(err).To(Succeed())
			Expect(firstPage).To(HaveLen(1))
			Expect(firstPage[0].Address).To(Equal("users:alice"))

			// Get second page of users
			secondPage, err := listAllAccounts(ctx, client, ledgerName, 1, firstPage[0].Address, prefixFilter("users:"))
			Expect(err).To(Succeed())
			Expect(secondPage).To(HaveLen(1))
			Expect(secondPage[0].Address).To(Equal("users:bob"))

			// Third page should be empty
			thirdPage, err := listAllAccounts(ctx, client, ledgerName, 1, secondPage[0].Address, prefixFilter("users:"))
			Expect(err).To(Succeed())
			Expect(thirdPage).To(BeEmpty())
		})
	})

	Context("When listing accounts includes both sources and destinations", Ordered, func() {
		var ledgerName = "accounts-source-dest-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create a force transaction from an unfunded account (source-only, no input)
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createForceTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("unfunded-source", "destination-only", big.NewInt(100), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())

			// Create a normal transaction from world
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "normal-account", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should include source-only accounts (accounts with only outputs)", func() {
			// The bbolt read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
				g.Expect(err).To(Succeed())

				// Collect all addresses
				addresses := make(map[string]bool)
				for _, a := range accounts {
					addresses[a.Address] = true
				}

				// unfunded-source has only Output (was source in force transaction)
				g.Expect(addresses).To(HaveKey("unfunded-source"))
				// destination-only has only Input
				g.Expect(addresses).To(HaveKey("destination-only"))
				// normal-account has Input
				g.Expect(addresses).To(HaveKey("normal-account"))
				// world has Output
				g.Expect(addresses).To(HaveKey("world"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should list all accounts in alphabetical order", func() {
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
			Expect(err).To(Succeed())

			// Alphabetical: destination-only, normal-account, unfunded-source, world
			Expect(accounts).To(HaveLen(4))
			Expect(accounts[0].Address).To(Equal("destination-only"))
			Expect(accounts[1].Address).To(Equal("normal-account"))
			Expect(accounts[2].Address).To(Equal("unfunded-source"))
			Expect(accounts[3].Address).To(Equal("world"))
		})
	})

	Context("When listing accounts with multiple assets", Ordered, func() {
		var ledgerName = "accounts-multi-asset-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions with different assets to the same account
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "multi-asset", big.NewInt(100), "USD"),
						newPosting("world", "multi-asset", big.NewInt(50), "EUR"),
						newPosting("world", "multi-asset", big.NewInt(1000), "JPY"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should not duplicate accounts with multiple assets", func() {
			// The bbolt read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
				g.Expect(err).To(Succeed())

				// Should have exactly 2 accounts: multi-asset and world
				g.Expect(accounts).To(HaveLen(2))

				addresses := make(map[string]int)
				for _, a := range accounts {
					addresses[a.Address]++
				}
				g.Expect(addresses["multi-asset"]).To(Equal(1))
				g.Expect(addresses["world"]).To(Equal(1))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When accounts are created across multiple transactions", Ordered, func() {
		var ledgerName = "accounts-incremental-ledger"

		BeforeAll(func() {
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should accumulate accounts across transactions", func() {
			// First batch
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Wait for the async index builder to catch up
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(2)) // world + account-1
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Second batch
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-2", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-3", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(4)) // world + account-1 + account-2 + account-3
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When listing accounts with metadata range filter", Ordered, func() {
		var ledgerName = "accounts-range-filter-ledger"

		BeforeAll(func() {
			// Create ledger with int64 schema for "age" and its index
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
						{
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        "age",
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					}),
					createMetadataIndexAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age"),
				},
			})
			Expect(err).To(Succeed())

			waitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "age")

			// Create accounts with transactions and set typed int metadata
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "dave", big.NewInt(400), "USD"),
					}, nil, nil),
					// alice=20, bob=35, charlie=50, dave=65
					saveAccountMetadataAction(ledgerName, "alice", map[string]string{"age": "20"}),
					saveAccountMetadataAction(ledgerName, "bob", map[string]string{"age": "35"}),
					saveAccountMetadataAction(ledgerName, "charlie", map[string]string{"age": "50"}),
					saveAccountMetadataAction(ledgerName, "dave", map[string]string{"age": "65"}),
				},
			})
			Expect(err).To(Succeed())

			// Wait for index builder to catch up (5 accounts: world + 4 users)
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", nil)
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(5))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter accounts with > (greater than)", func() {
			// age > 35 should match charlie(50), dave(65)
			val := int64(35)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "age"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min:          &val,
								MinExclusive: true,
							},
						},
					},
				},
			}
			Eventually(func(g Gomega) {
				accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
				g.Expect(err).To(Succeed())
				g.Expect(accounts).To(HaveLen(2))
				addresses := []string{accounts[0].Address, accounts[1].Address}
				g.Expect(addresses).To(ContainElements("charlie", "dave"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should filter accounts with >= (greater than or equal)", func() {
			// age >= 35 should match bob(35), charlie(50), dave(65)
			val := int64(35)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "age"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min: &val,
							},
						},
					},
				},
			}
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(3))
			addresses := make([]string, len(accounts))
			for i, a := range accounts {
				addresses[i] = a.Address
			}
			Expect(addresses).To(ContainElements("bob", "charlie", "dave"))
		})

		It("Should filter accounts with < (less than)", func() {
			// age < 50 should match alice(20), bob(35)
			val := int64(50)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "age"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Max:          &val,
								MaxExclusive: true,
							},
						},
					},
				},
			}
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(2))
			addresses := []string{accounts[0].Address, accounts[1].Address}
			Expect(addresses).To(ContainElements("alice", "bob"))
		})

		It("Should filter accounts with <= (less than or equal)", func() {
			// age <= 50 should match alice(20), bob(35), charlie(50)
			val := int64(50)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "age"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Max: &val,
							},
						},
					},
				},
			}
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(3))
			addresses := make([]string, len(accounts))
			for i, a := range accounts {
				addresses[i] = a.Address
			}
			Expect(addresses).To(ContainElements("alice", "bob", "charlie"))
		})

		It("Should filter accounts with combined range (>= AND <)", func() {
			// age >= 30 AND age < 60 should match bob(35), charlie(50)
			minVal := int64(30)
			maxVal := int64(60)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_And{
					And: &commonpb.AndFilter{
						Filters: []*commonpb.QueryFilter{
							{
								Filter: &commonpb.QueryFilter_Field{
									Field: &commonpb.FieldCondition{
										Field: &commonpb.FieldRef{Metadata: "age"},
										Condition: &commonpb.FieldCondition_IntCond{
											IntCond: &commonpb.IntCondition{
												Min: &minVal,
											},
										},
									},
								},
							},
							{
								Filter: &commonpb.QueryFilter_Field{
									Field: &commonpb.FieldCondition{
										Field: &commonpb.FieldRef{Metadata: "age"},
										Condition: &commonpb.FieldCondition_IntCond{
											IntCond: &commonpb.IntCondition{
												Max:          &maxVal,
												MaxExclusive: true,
											},
										},
									},
								},
							},
						},
					},
				},
			}
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
			Expect(err).To(Succeed())
			Expect(accounts).To(HaveLen(2))
			addresses := []string{accounts[0].Address, accounts[1].Address}
			Expect(addresses).To(ContainElements("bob", "charlie"))
		})

		It("Should return empty list when no accounts match the range", func() {
			// age > 100 should match nobody
			val := int64(100)
			filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Field{
					Field: &commonpb.FieldCondition{
						Field: &commonpb.FieldRef{Metadata: "age"},
						Condition: &commonpb.FieldCondition_IntCond{
							IntCond: &commonpb.IntCondition{
								Min:          &val,
								MinExclusive: true,
							},
						},
					},
				},
			}
			accounts, err := listAllAccounts(ctx, client, ledgerName, 0, "", filter)
			Expect(err).To(Succeed())
			Expect(accounts).To(BeEmpty())
		})
	})

	Context("When listing accounts is isolated per ledger", Ordered, func() {
		var (
			ledgerA = "accounts-isolation-a"
			ledgerB = "accounts-isolation-b"
		)

		BeforeAll(func() {
			// Create two ledgers
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createLedgerAction(ledgerA, nil),
					createLedgerAction(ledgerB, nil),
				},
			})
			Expect(err).To(Succeed())

			// Create accounts in ledger A
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerA, []*commonpb.Posting{
						newPosting("world", "alice", big.NewInt(100), "USD"),
					}, nil, nil),
					createTransactionAction(ledgerA, []*commonpb.Posting{
						newPosting("world", "bob", big.NewInt(200), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			// Create accounts in ledger B
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					createTransactionAction(ledgerB, []*commonpb.Posting{
						newPosting("world", "charlie", big.NewInt(300), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should only return accounts from the requested ledger", func() {
			// The bbolt read index is populated asynchronously by the index builder,
			// so we need to wait for it to catch up after writing data.
			Eventually(func(g Gomega) {
				accountsA, err := listAllAccounts(ctx, client, ledgerA, 0, "", nil)
				g.Expect(err).To(Succeed())
				g.Expect(accountsA).To(HaveLen(3)) // world + alice + bob

				addressesA := make(map[string]bool)
				for _, a := range accountsA {
					addressesA[a.Address] = true
				}
				g.Expect(addressesA).To(HaveKey("world"))
				g.Expect(addressesA).To(HaveKey("alice"))
				g.Expect(addressesA).To(HaveKey("bob"))
				g.Expect(addressesA).NotTo(HaveKey("charlie"))

				accountsB, err := listAllAccounts(ctx, client, ledgerB, 0, "", nil)
				g.Expect(err).To(Succeed())
				g.Expect(accountsB).To(HaveLen(2)) // world + charlie

				addressesB := make(map[string]bool)
				for _, a := range accountsB {
					addressesB[a.Address] = true
				}
				g.Expect(addressesB).To(HaveKey("world"))
				g.Expect(addressesB).To(HaveKey("charlie"))
				g.Expect(addressesB).NotTo(HaveKey("alice"))
				g.Expect(addressesB).NotTo(HaveKey("bob"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
