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
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
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
			state, err := servers[0].client.GetClusterState(ctx, &servicepb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())

			leaderID = uint64(state.Leader)
			return leaderID
		}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())
		return leaderID
	}

	BeforeEach(func() {
		ctx = logging.TestingContext()

		servers = make([]serviceWithClient, 0, countInstances)
		for i := range countInstances {
			walTmpDir := GinkgoT().TempDir()
			dataTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(walTmpDir)).To(Succeed())
				Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRunCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(httpPortBase+i),
					testserver.WithWalDir(walTmpDir),
					testserver.WithDataDir(dataTmpDir),
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

			// Create gRPC client
			grpcClient, grpcConn, err := newGRPCClient(grpcPortBase + i)
			Expect(err).To(Succeed())
			DeferCleanup(func() {
				_ = grpcConn.Close()
			})

			servers = append(servers, serviceWithClient{
				service:  server,
				client:   grpcClient,
				grpcConn: grpcConn,
				walDir:   walTmpDir,
				dataDir:  dataTmpDir,
				grpcPort: grpcPortBase + i,
			})
		}

		Eventually(func(g Gomega) bool {
			state, err := servers[0].client.GetClusterState(ctx, &servicepb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())

			return state.Leader != 0
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

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "test-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save account metadata successfully", func() {
			metadata := map[string]string{
				"account_type": "asset",
				"label":        "Test Account",
			}

			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{saveAccountMetadataAction(ledgerName, "test-account", metadata)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should merge metadata with existing account metadata", func() {
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "merge-account", big.NewInt(50), "USD"),
					}, nil, map[string]*commonpb.Metadata{
						"merge-account": {Entries: map[string]string{
							"key1": "value1",
							"key2": "value2",
						}},
					}),
				},
			})
			Expect(err).To(Succeed())

			metadata := map[string]string{
				"key3": "value3",
				"key2": "updated_value2",
			}

			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{saveAccountMetadataAction(ledgerName, "merge-account", metadata)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should delete account metadata successfully", func() {
			metadata := map[string]string{
				"to_delete": "value",
			}

			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{saveAccountMetadataAction(ledgerName, "test-account", metadata)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			deleteResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{deleteAccountMetadataAction(ledgerName, "test-account", "to_delete")},
			})
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
			Expect(deleteResp.Logs).To(HaveLen(1))
		})
	})

	Context("When saving account metadata via bulk endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "bulk-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			_, err = servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should save account metadata via bulk endpoint", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					saveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{
						"account_type": "asset",
						"label":        "Bulk Account",
					}),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should handle multiple metadata operations in bulk", func() {
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "bulk-account-2", big.NewInt(50), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					saveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{"key1": "value1"}),
					saveAccountMetadataAction(ledgerName, "bulk-account-2", map[string]string{"key2": "value2"}),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should delete account metadata via bulk endpoint", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					saveAccountMetadataAction(ledgerName, "bulk-account", map[string]string{"to_delete": "value"}),
					deleteAccountMetadataAction(ledgerName, "bulk-account", "to_delete"),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("When creating ledgers and transactions", func() {
		var (
			leaderID   uint64
			ledgerName string
		)

		BeforeEach(func() {
			leaderID = getLeaderID()
			ledgerName = "test-ledger-create"
		})

		It("Should create a ledger successfully", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed(), "Failed to create ledger")
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			ledger, err := servers[leaderID-1].client.GetLedgerByName(ctx, &servicepb.GetLedgerByNameRequest{
				Name: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("Should create a transaction on the ledger", func() {
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed(), "Failed to create transaction on ledger")
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should create multiple transactions successfully", func() {
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
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
				resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Actions: []*servicepb.Action{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting(tx.source, tx.destination, tx.amount, tx.asset),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed(), "Failed to create transaction %d", i+1)
				Expect(resp).NotTo(BeNil())
				Expect(resp.Logs).To(HaveLen(1))
			}
		})

		It("Should create transactions with metadata", func() {
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-with-metadata", big.NewInt(100), "USD"),
					}, map[string]string{
						"description": "Test transaction",
						"category":    "test",
					}, map[string]*commonpb.Metadata{
						"account-with-metadata": {Entries: map[string]string{
							"account_type": "asset",
							"label":        "Account with Metadata",
						}},
					}),
				},
			})
			Expect(err).To(Succeed(), "Failed to create transaction with metadata")
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})

	Context("When reading transactions", func() {
		var (
			leaderID   uint64
			ledgerName = "get-transaction-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should get a transaction by ID", func() {
			// Create a transaction first
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, map[string]string{"description": "Test transaction"}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())
			Expect(createResp.Logs).To(HaveLen(1))

			// Extract transaction ID from the log
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			createdTx := applyLog.Log.Data.GetCreatedTransaction()
			Expect(createdTx).NotTo(BeNil())
			transactionID := createdTx.Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			// Get the transaction
			getResp, err := servers[leaderID-1].client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(getResp).NotTo(BeNil())
			Expect(getResp.Id).To(Equal(transactionID))
			Expect(getResp.Postings).To(HaveLen(1))
			Expect(getResp.Postings[0].Source).To(Equal("world"))
			Expect(getResp.Postings[0].Destination).To(Equal("account-1"))
			Expect(getResp.Postings[0].Asset).To(Equal("USD"))
		})

		It("Should return error for non-existent transaction", func() {
			nonExistentTransactionID := uint64(99999)

			_, err := servers[leaderID-1].client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
				TransactionId: nonExistentTransactionID,
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should get a reverted transaction and show reverted status", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-revert", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction
			_, err = servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())

			// Get the reverted transaction - it should show as reverted
			getResp, err := servers[leaderID-1].client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
				Ledger:        &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
				TransactionId: transactionID,
			})
			Expect(err).To(Succeed())
			Expect(getResp).NotTo(BeNil())
			Expect(getResp.Reverted).To(BeTrue())
		})

		It("Should read transaction from any node (follower read)", func() {
			// Create a transaction on the leader
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-follower-read", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Find a follower node
			followerIdx := -1
			for i := range servers {
				if uint64(i+1) != leaderID {
					followerIdx = i
					break
				}
			}
			Expect(followerIdx).NotTo(Equal(-1))

			// Eventually the transaction should be readable from the follower
			Eventually(func(g Gomega) {
				getResp, err := servers[followerIdx].client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
					Ledger:        &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
					TransactionId: transactionID,
				})
				g.Expect(err).To(Succeed())
				g.Expect(getResp).NotTo(BeNil())
				g.Expect(getResp.Id).To(Equal(transactionID))
			}).Within(5 * time.Second).WithPolling(100 * time.Millisecond).Should(Succeed())
		})
	})

	Context("When saving transaction metadata via direct endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "transaction-metadata-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata successfully", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-metadata-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			metadata := map[string]string{
				"reason": "adjustment",
				"source": "support",
			}

			saveResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{saveTransactionMetadataAction(ledgerName, transactionID, metadata)},
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(1))
		})

		It("Should delete transaction metadata successfully", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-metadata-account", big.NewInt(100), "USD"),
					}, map[string]string{"to_delete": "value"}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			deleteResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{deleteTransactionMetadataAction(ledgerName, transactionID, "to_delete")},
			})
			Expect(err).To(Succeed())
			Expect(deleteResp).NotTo(BeNil())
			Expect(deleteResp.Logs).To(HaveLen(1))
		})
	})

	Context("When saving transaction metadata via bulk endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "transaction-metadata-bulk-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should save transaction metadata via bulk endpoint", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-bulk-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			saveResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{
						"category": "bulk",
						"reason":   "reconciliation",
					}),
				},
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(1))
		})

		It("Should delete transaction metadata via bulk endpoint", func() {
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "transaction-bulk-account", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))

			log := resp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			saveResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					saveTransactionMetadataAction(ledgerName, transactionID, map[string]string{"to_delete": "value"}),
					deleteTransactionMetadataAction(ledgerName, transactionID, "to_delete"),
				},
			})
			Expect(err).To(Succeed())
			Expect(saveResp).NotTo(BeNil())
			Expect(saveResp.Logs).To(HaveLen(2))
		})
	})

	Context("When reverting transactions", func() {
		var (
			leaderID   uint64
			ledgerName = "revert-transaction-ledger"
		)

		BeforeEach(func() {
			leaderID = getLeaderID()

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should revert a transaction successfully", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())
			Expect(createResp.Logs).To(HaveLen(1))

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
			Expect(transactionID).NotTo(BeZero())

			// Revert the transaction
			revertResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with metadata", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with metadata
			revertMetadata := map[string]string{
				"reason": "correction",
				"source": "support",
			}

			revertResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, false, false, revertMetadata)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with force flag", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with force flag
			revertResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, true, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should revert a transaction with atEffectiveDate flag", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction with atEffectiveDate flag
			revertResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, false, true, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(1))
		})

		It("Should fail to revert a non-existent transaction", func() {
			nonExistentTransactionID := uint64(99999)

			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, nonExistentTransactionID, false, false, nil)},
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should fail to revert an already reverted transaction", func() {
			// Create a transaction
			createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{
					createTransactionAction(ledgerName, []*commonpb.Posting{
						newPosting("world", "account-1", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(createResp).NotTo(BeNil())

			log := createResp.Logs[0]
			applyLog := log.Payload.GetApply()
			transactionID := applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Revert the transaction first time
			revertResp1, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(Succeed())
			Expect(revertResp1).NotTo(BeNil())

			// Try to revert the same transaction again
			_, err = servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{revertTransactionAction(ledgerName, transactionID, false, false, nil)},
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should revert multiple transactions in bulk", func() {
			// Create multiple transactions
			var transactionIDs []uint64
			for i := 0; i < 3; i++ {
				createResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Actions: []*servicepb.Action{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("account-%d", i+1), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
				Expect(createResp).NotTo(BeNil())
				log := createResp.Logs[0]
				applyLog := log.Payload.GetApply()
				transactionIDs = append(transactionIDs, applyLog.Log.Data.GetCreatedTransaction().Transaction.Id)
			}

			// Revert all transactions in bulk
			actions := make([]*servicepb.Action, len(transactionIDs))
			for i, txID := range transactionIDs {
				actions[i] = revertTransactionAction(ledgerName, txID, false, false, nil)
			}

			revertResp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: actions,
			})
			Expect(err).To(Succeed())
			Expect(revertResp).NotTo(BeNil())
			Expect(revertResp.Logs).To(HaveLen(len(transactionIDs)))
		})
	})
})
