//go:build e2e
// +build e2e

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

var _ = Describe("Account Metadata", func() {
	type serviceWithClient struct {
		service                   *testservice.Service
		client                    *client.Formance
		raftDataDir, extraDataDir string
	}

	var (
		ctx     context.Context
		servers []serviceWithClient
	)
	const countInstances = 3

	BeforeEach(func() {
		ctx = logging.TestingContext()

		servers = make([]serviceWithClient, 0, countInstances)
		for i := range countInstances {
			raftTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(raftTmpDir)).To(Succeed())
			})

			extraDataTmpDir := GinkgoT().TempDir()
			DeferCleanup(func() {
				Expect(os.RemoveAll(extraDataTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRootCommand,
				testservice.WithInstruments(
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(9100+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(8100+i),
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
								Address: fmt.Sprintf("127.0.0.1:%d", 8100+j),
							})
						}

						return ret
					}()...),
					testserver.WithExtraDataDir(extraDataTmpDir),
				),
			)
			Expect(server.Start(ctx)).To(Succeed())

			servers = append(servers, serviceWithClient{
				service: server,
				client: client.New(
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", 9100+i)),
				),
				raftDataDir:  raftTmpDir,
				extraDataDir: extraDataTmpDir,
			})
		}

		// Wait for leader election
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
			// Get leader ID
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = uint64(*state.ClusterStateResponse.Data.Leader)
				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Create a ledger
			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
				},
			})
			Expect(err).To(Succeed())

			// Create a transaction to create the account
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
			// Save account metadata via direct endpoint using SDK
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
			// First, create a transaction with account metadata
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

			// Then, save additional metadata via direct endpoint using SDK
			metadata := map[string]string{
				"key3": "value3",
				"key2": "updated_value2", // This should override key2
			}

			resp, err := servers[leaderID-1].client.Accounts.SaveAccountMetadata(ctx, operations.SaveAccountMetadataRequest{
				LedgerName:  ledgerName,
				Address:     "merge-account",
				RequestBody: metadata,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify metadata was merged correctly (we can't directly query metadata yet,
			// but we can verify the log was created)
			// In a real scenario, we would query the account metadata endpoint
		})
	})

	Context("When saving account metadata via bulk endpoint", func() {
		var (
			leaderID   uint64
			ledgerName = "bulk-ledger"
		)

		BeforeEach(func() {
			// Get leader ID
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = uint64(*state.ClusterStateResponse.Data.Leader)
				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Create a ledger
			_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: ledgerName,
				CreateLedgerRequest: components.CreateLedgerRequest{
					Driver: components.CreateLedgerRequestDriverSqlite.ToPointer(),
				},
			})
			Expect(err).To(Succeed())

			// Create a transaction to create the account
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
			// Save account metadata via bulk endpoint
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account"),
						Metadata: map[string]any{
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
			// Create another account
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

			// Save metadata for multiple accounts via bulk endpoint
			bulkElements := []components.BulkElement{
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account"),
						Metadata: map[string]any{
							"key1": "value1",
						},
					}),
				},
				{
					Action: components.ActionAddMetadata,
					Data: components.CreateBulkElementDataAddMetadataRequest(components.AddMetadataRequest{
						TargetType: components.TargetTypeAccount,
						TargetID:   components.CreateTargetIDStr("bulk-account-2"),
						Metadata: map[string]any{
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
	})
})
