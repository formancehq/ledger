//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/pkg/client"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/components"
	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ledger", func() {
	var (
		ctx     context.Context
		servers []serviceWithClient
	)

	const (
		countInstances = 3
		httpPortBase   = 9100
		grpcPortBase   = 8100
	)

	getLeaderID := func() uint64 {
		var leaderID uint64
		Eventually(func(g Gomega) uint64 {
			state, err := servers[0].client.Cluster.GetClusterState(ctx)
			g.Expect(err).To(Succeed())

			leaderID = uint64(*state.ClusterStateResponse.Data.Leader)
			return leaderID
		}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())
		return leaderID
	}

	BeforeEach(func() {
		ctx = logging.TestingContext()

		servers = make([]serviceWithClient, 0, countInstances)
		for i := range countInstances {
			raftTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(raftTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRootCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(httpPortBase+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(grpcPortBase+i),
					testserver.WithSnapshotThreshold(10),
					testserver.WithDebug(os.Getenv("DEBUG") == "true"),
					testserver.WithRaftTickInterval(10*time.Millisecond),
					testserver.WithRaftHeartbeatTick(10),
					testserver.WithRaftElectionTick(100),
					testserver.WithPeers(func() []raft.Peer {
						ret := make([]raft.Peer, 0, countInstances-1)
						for j := range countInstances {
							if i == j {
								continue
							}
							ret = append(ret, raft.Peer{
								ID:      uint64(j + 1),
								Address: fmt.Sprintf("127.0.0.1:%d", grpcPortBase+j),
							})
						}

						return ret
					}()...),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			servers = append(servers, serviceWithClient{
				service: server,
				client: client.New(
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", httpPortBase+i)),
				),
				raftDataDir: raftTmpDir,
			})
		}

		Eventually(func(g Gomega) bool {
			state, err := servers[0].client.Cluster.GetClusterState(ctx)
			g.Expect(err).To(Succeed())

			return state.ClusterStateResponse.Data.Leader != nil &&
				*state.ClusterStateResponse.Data.Leader != 0
		}).Within(5 * time.Second).To(BeTrue())
	})

	AfterEach(func() {
		for i, server := range servers {
			By(fmt.Sprintf("Stopping node %d", i+1), func() {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				DeferCleanup(cancel)

				Expect(server.service.Stop(ctx)).To(Succeed())
			})
		}
	})

	Context("When saving account metadata via direct endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "test-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					StoreDriver: components.CreateLedgerRequestStoreDriverSqliteMattn,
				},
			})
			Expect(err).To(Succeed())

			_, err = servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "test-account",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save account metadata successfully", func() {
			metadata := map[string]string{
				"account_type": "asset",
				"label":        "Test Account",
			}

			resp, err := servers[leaderID-1].client.Accounts.SaveAccountMetadata(ctx, operations.SaveAccountMetadataRequest{
				LedgerName:  ledgerName,
				Address:     "test-account",
				RequestBody: metadata,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("Should merge metadata with existing account metadata", func() {
			_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "merge-account",
						Amount:      big.NewInt(50),
						Asset:       "USD",
					}},
					AccountMetadata: map[string]map[string]string{
						"merge-account": {
							"key1": "value1",
							"key2": "value2",
						},
					},
				},
			})
			Expect(err).To(Succeed())

			metadata := map[string]string{
				"key3": "value3",
				"key2": "updated_value2",
			}

			resp, err := servers[leaderID-1].client.Accounts.SaveAccountMetadata(ctx, operations.SaveAccountMetadataRequest{
				LedgerName:  ledgerName,
				Address:     "merge-account",
				RequestBody: metadata,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("Should delete account metadata successfully", func() {
			metadata := map[string]string{
				"to_delete": "value",
			}

			resp, err := servers[leaderID-1].client.Accounts.SaveAccountMetadata(ctx, operations.SaveAccountMetadataRequest{
				LedgerName:  ledgerName,
				Address:     "test-account",
				RequestBody: metadata,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			deleteResp, err := servers[leaderID-1].client.Accounts.DeleteAccountMetadata(ctx, operations.DeleteAccountMetadataRequest{
				LedgerName: ledgerName,
				Address:    "test-account",
				Key:        "to_delete",
			})
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
		})
	})

	Context("When saving account metadata via bulk endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "bulk-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					StoreDriver: components.CreateLedgerRequestStoreDriverSqliteMattn,
				},
			})
			Expect(err).To(Succeed())

			_, err = servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "bulk-account",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save account metadata via bulk endpoint", func() {
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account"),
						Metadata: map[string]string{
							"account_type": "asset",
							"label":        "Bulk Account",
						},
					}),
				},
			}

			resp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.BulkResponse).NotTo(BeNil())
			Expect(resp.BulkResponse.Data).To(HaveLen(1))
			Expect(resp.BulkResponse.Data[0].LogID).NotTo(BeNil())
		})

		It("Should handle multiple metadata operations in bulk", func() {
			_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "bulk-account-2",
						Amount:      big.NewInt(50),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())

			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account"),
						Metadata: map[string]string{
							"key1": "value1",
						},
					}),
				},
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account-2"),
						Metadata: map[string]string{
							"key2": "value2",
						},
					}),
				},
			}

			resp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.BulkResponse).NotTo(BeNil())
			Expect(resp.BulkResponse.Data).To(HaveLen(2))
			Expect(resp.BulkResponse.Data[0].LogID).NotTo(BeNil())
			Expect(resp.BulkResponse.Data[1].LogID).NotTo(BeNil())
		})

		It("Should delete account metadata via bulk endpoint", func() {
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account"),
						Metadata: map[string]string{
							"to_delete": "value",
						},
					}),
				},
				{
					Action: components.ActionDeleteMetadata,
					Data: components.CreateBulkElementDataDeleteMetadataRequest(components.DeleteMetadataRequest{
						TargetType: components.DeleteMetadataRequestTargetTypeAccount,
						TargetID:   components.CreateDeleteMetadataRequestTargetIDStr("bulk-account"),
						Key:        "to_delete",
					}),
				},
			}

			resp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.BulkResponse).NotTo(BeNil())
			Expect(resp.BulkResponse.Data).To(HaveLen(2))
			Expect(resp.BulkResponse.Data[0].LogID).NotTo(BeNil())
			Expect(resp.BulkResponse.Data[1].LogID).NotTo(BeNil())
		})
	})

	Context("When creating ledgers and transactions with different drivers", func() {
		type driverTestCase struct {
			driverName      string
			storeDriverEnum components.CreateLedgerRequestStoreDriver
			storeDriver     components.StoreDriver
			description     string
		}

		testCases := []driverTestCase{
			{
				driverName:      "sqlite-mattn",
				storeDriverEnum: components.CreateLedgerRequestStoreDriverSqliteMattn,
				storeDriver:     components.StoreDriverSqliteMattn,
				description:     "SQLite Mattn driver (github.com/mattn/go-sqlite3)",
			},
			{
				driverName:      "sqlite-modern",
				storeDriverEnum: components.CreateLedgerRequestStoreDriverSqliteModern,
				storeDriver:     components.StoreDriverSqliteModern,
				description:     "SQLite Modern driver (modernc.org/sqlite)",
			},
			{
				driverName:      "pebble",
				storeDriverEnum: components.CreateLedgerRequestStoreDriverPebble,
				storeDriver:     components.StoreDriverPebble,
				description:     "Pebble driver (github.com/cockroachdb/pebble)",
			},
		}

		for _, tc := range testCases {
			tc := tc
			Context(fmt.Sprintf("With %s driver", tc.driverName), func() {
				var (
					leaderID   uint64
					ledgerName string
				)

				BeforeEach(func() {
					leaderID = getLeaderID()
					ledgerName = fmt.Sprintf("test-ledger-%s", tc.driverName)
				})

				It("Should create a ledger successfully", func() {
					resp, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							StoreDriver: tc.storeDriverEnum,
						},
					})
					Expect(err).To(Succeed(), "Failed to create ledger with driver %s", tc.driverName)
					Expect(resp).NotTo(BeNil())
					Expect(resp.GetCreateLedgerResponse()).NotTo(BeNil())
					Expect(resp.GetCreateLedgerResponse().Data.Name).To(Equal(ledgerName))
					Expect(resp.GetCreateLedgerResponse().Data.StoreDriver).To(Equal(tc.storeDriver))

					ledger, err := servers[leaderID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
						LedgerName: ledgerName,
					})
					Expect(err).To(Succeed())
					Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
					Expect(ledger.GetGetLedgerResponse().Data.StoreDriver).To(Equal(tc.storeDriver))
				})

				It("Should create a transaction on the ledger", func() {
					_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							StoreDriver: tc.storeDriverEnum,
						},
					})
					Expect(err).To(Succeed())

					resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
						LedgerName: ledgerName,
						CreateTransactionRequest: components.CreateTransactionRequest{
							Postings: []components.PostingRequest{{
								Source:      "world",
								Destination: "account-1",
								Amount:      big.NewInt(100),
								Asset:       "USD",
							}},
						},
					})
					Expect(err).To(Succeed(), "Failed to create transaction on ledger with driver %s", tc.driverName)
					Expect(resp).NotTo(BeNil())
				})

				It("Should create multiple transactions successfully", func() {
					_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							StoreDriver: tc.storeDriverEnum,
						},
					})
					Expect(err).To(Succeed())

					transactions := []struct {
						source      string
						destination string
						amount      *big.Int
						asset       string
					}{
						{"world", "account-1", big.NewInt(100), "USD"},
						{"world", "account-2", big.NewInt(200), "USD"},
						{"account-1", "account-2", big.NewInt(50), "USD"},
					}

					for i, tx := range transactions {
						resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
							LedgerName: ledgerName,
							CreateTransactionRequest: components.CreateTransactionRequest{
								Postings: []components.PostingRequest{{
									Source:      tx.source,
									Destination: tx.destination,
									Amount:      tx.amount,
									Asset:       tx.asset,
								}},
							},
						})
						Expect(err).To(Succeed(), "Failed to create transaction %d with driver %s", i+1, tc.driverName)
						Expect(resp).NotTo(BeNil())
						Expect(resp.GetCreateTransactionResponse()).NotTo(BeNil())
					}
				})

				It("Should create transactions with metadata", func() {
					_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							StoreDriver: tc.storeDriverEnum,
						},
					})
					Expect(err).To(Succeed())

					resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
						LedgerName: ledgerName,
						CreateTransactionRequest: components.CreateTransactionRequest{
							Postings: []components.PostingRequest{{
								Source:      "world",
								Destination: "account-with-metadata",
								Amount:      big.NewInt(100),
								Asset:       "USD",
							}},
							Metadata: map[string]string{
								"description": "Test transaction",
								"category":    "test",
							},
							AccountMetadata: map[string]map[string]string{
								"account-with-metadata": {
									"account_type": "asset",
									"label":        "Account with Metadata",
								},
							},
						},
					})
					Expect(err).To(Succeed(), "Failed to create transaction with metadata using driver %s", tc.driverName)
					Expect(resp).NotTo(BeNil())
					Expect(resp.GetCreateTransactionResponse()).NotTo(BeNil())
				})
			})
		}
	})

	Context("When saving transaction metadata via direct endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "transaction-metadata-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					StoreDriver: components.CreateLedgerRequestStoreDriverSqliteMattn,
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata successfully", func() {
			resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "transaction-metadata-account",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.GetCreateTransactionResponse()).NotTo(BeNil())

			transactionID := resp.GetCreateTransactionResponse().GetData().Transaction.ID
			Expect(transactionID).NotTo(BeZero())

			metadata := map[string]string{
				"reason": "adjustment",
				"source": "support",
			}

			saveResp, err := servers[leaderID-1].client.Transactions.SaveTransactionMetadata(ctx, operations.SaveTransactionMetadataRequest{
				LedgerName:    ledgerName,
				TransactionID: transactionID,
				RequestBody:   metadata,
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
		})

		It("Should delete transaction metadata successfully", func() {
			resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "transaction-metadata-account",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
					Metadata: map[string]string{
						"to_delete": "value",
					},
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.GetCreateTransactionResponse()).NotTo(BeNil())

			transactionID := resp.GetCreateTransactionResponse().GetData().Transaction.ID
			Expect(transactionID).NotTo(BeZero())

			deleteResp, err := servers[leaderID-1].client.Transactions.DeleteTransactionMetadata(ctx, operations.DeleteTransactionMetadataRequest{
				LedgerName:    ledgerName,
				TransactionID: transactionID,
				Key:           "to_delete",
			})
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
		})
	})

	Context("When saving transaction metadata via bulk endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "transaction-metadata-bulk-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					StoreDriver: components.CreateLedgerRequestStoreDriverSqliteMattn,
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata via bulk endpoint", func() {
			resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "transaction-bulk-account",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.GetCreateTransactionResponse()).NotTo(BeNil())

			transactionID := resp.GetCreateTransactionResponse().GetData().Transaction.ID

			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeTransaction,
						TargetID:   components.CreateTargetIDInteger(transactionID),
						Metadata: map[string]string{
							"category": "bulk",
							"reason":   "reconciliation",
						},
					}),
				},
			}

			saveResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.BulkResponse).NotTo(BeNil())
			Expect(saveResp.BulkResponse.Data).To(HaveLen(1))
			Expect(saveResp.BulkResponse.Data[0].LogID).NotTo(BeNil())
		})

		It("Should delete transaction metadata via bulk endpoint", func() {
			resp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "transaction-bulk-account",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.GetCreateTransactionResponse()).NotTo(BeNil())

			transactionID := resp.GetCreateTransactionResponse().GetData().Transaction.ID

			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeTransaction,
						TargetID:   components.CreateTargetIDInteger(transactionID),
						Metadata: map[string]string{
							"to_delete": "value",
						},
					}),
				},
				{
					Action: components.ActionDeleteMetadata,
					Data: components.CreateBulkElementDataDeleteMetadataRequest(components.DeleteMetadataRequest{
						TargetType: components.DeleteMetadataRequestTargetTypeTransaction,
						TargetID:   components.CreateDeleteMetadataRequestTargetIDInteger(transactionID),
						Key:        "to_delete",
					}),
				},
			}

			saveResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.BulkResponse).NotTo(BeNil())
			Expect(saveResp.BulkResponse.Data).To(HaveLen(2))
			Expect(saveResp.BulkResponse.Data[0].LogID).NotTo(BeNil())
			Expect(saveResp.BulkResponse.Data[1].LogID).NotTo(BeNil())
		})
	})

	Context("When reverting transactions", func() {
		var (
			leaderID   uint64
			ledgerName = "revert-transaction-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					StoreDriver: components.CreateLedgerRequestStoreDriverSqliteMattn,
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should revert a transaction successfully", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "account-1",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())
			Expect(createResp.GetCreateTransactionResponse()).NotTo(BeNil())

			transactionID := createResp.GetCreateTransactionResponse().GetData().Transaction.ID
			Expect(transactionID).NotTo(BeZero())

			// Revert the transaction via bulk endpoint
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID: transactionID,
					}),
				},
			}

			revertResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.BulkResponse).NotTo(BeNil())
			Expect(revertResp.BulkResponse.Data).To(HaveLen(1))
			Expect(revertResp.BulkResponse.Data[0].LogID).NotTo(BeNil())
		})

		It("Should revert a transaction with metadata", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "account-1",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			transactionID := createResp.GetCreateTransactionResponse().GetData().Transaction.ID

			// Revert the transaction with metadata
			revertMetadata := map[string]string{
				"reason": "correction",
				"source": "support",
			}

			bulkElements := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID:       transactionID,
						Metadata: revertMetadata,
					}),
				},
			}

			revertResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.BulkResponse).NotTo(BeNil())
			Expect(revertResp.BulkResponse.Data).To(HaveLen(1))
			Expect(revertResp.BulkResponse.Data[0].LogID).NotTo(BeNil())
		})

		It("Should revert a transaction with force flag", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "account-1",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			transactionID := createResp.GetCreateTransactionResponse().GetData().Transaction.ID

			// Revert the transaction with force flag
			force := true
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID:    transactionID,
						Force: &force,
					}),
				},
			}

			revertResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.BulkResponse).NotTo(BeNil())
			Expect(revertResp.BulkResponse.Data).To(HaveLen(1))
			Expect(revertResp.BulkResponse.Data[0].LogID).NotTo(BeNil())
		})

		It("Should revert a transaction with atEffectiveDate flag", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "account-1",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			transactionID := createResp.GetCreateTransactionResponse().GetData().Transaction.ID

			// Revert the transaction with atEffectiveDate flag
			atEffectiveDate := true
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID:              transactionID,
						AtEffectiveDate: &atEffectiveDate,
					}),
				},
			}

			revertResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.BulkResponse).NotTo(BeNil())
			Expect(revertResp.BulkResponse.Data).To(HaveLen(1))
			Expect(revertResp.BulkResponse.Data[0].LogID).NotTo(BeNil())
		})

		It("Should fail to revert a non-existent transaction", func() {
			nonExistentTransactionID := int64(99999)

			bulkElements := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID: nonExistentTransactionID,
					}),
				},
			}

			revertResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).NotTo(Succeed())
			Expect(revertResp).To(BeNil())
		})

		It("Should fail to revert an already reverted transaction", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
				LedgerName: ledgerName,
				CreateTransactionRequest: components.CreateTransactionRequest{
					Postings: []components.PostingRequest{{
						Source:      "world",
						Destination: "account-1",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					}},
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			transactionID := createResp.GetCreateTransactionResponse().GetData().Transaction.ID

			// Revert the transaction first time
			bulkElements1 := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID: transactionID,
					}),
				},
			}

			revertResp1, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements1,
			})
			Expect(err).To(Succeed())
			Expect(revertResp1).NotTo(BeNil())

			// Try to revert the same transaction again
			bulkElements2 := []components.BulkElement{
				{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID: transactionID,
					}),
				},
			}

			revertResp2, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements2,
			})
			Expect(err).NotTo(Succeed())
			Expect(revertResp2).To(BeNil())
		})

		It("Should revert multiple transactions in bulk", func() {
			// Create multiple transactions
			var transactionIDs []int64
			for i := 0; i < 3; i++ {
				createResp, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
					LedgerName: ledgerName,
					CreateTransactionRequest: components.CreateTransactionRequest{
						Postings: []components.PostingRequest{{
							Source:      "world",
							Destination: fmt.Sprintf("account-%d", i+1),
							Amount:      big.NewInt(100 * int64(i+1)),
							Asset:       "USD",
						}},
					},
				})
				Expect(err).To(Succeed())
				Expect(createResp).NotTo(BeNil())
				transactionIDs = append(transactionIDs, createResp.GetCreateTransactionResponse().GetData().Transaction.ID)
			}

			// Revert all transactions in bulk
			bulkElements := make([]components.BulkElement, len(transactionIDs))
			for i, txID := range transactionIDs {
				bulkElements[i] = components.BulkElement{
					Action: components.ActionRevertTransaction,
					Data: components.CreateBulkElementDataRevertTransactionRequest(components.RevertTransactionRequest{
						ID: txID,
					}),
				}
			}

			revertResp, err := servers[leaderID-1].client.Transactions.BulkOperations(ctx, operations.BulkOperationsRequest{
				LedgerName:  ledgerName,
				RequestBody: bulkElements,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.BulkResponse).NotTo(BeNil())
			Expect(revertResp.BulkResponse.Data).To(HaveLen(len(transactionIDs)))
			for _, data := range revertResp.BulkResponse.Data {
				Expect(data.LogID).NotTo(BeNil())
			}
		})
	})
})
