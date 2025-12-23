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

var _ = Describe("Ledger Deletion", func() {
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
					testserver.WithHTTPPort(9200+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(8200+i),
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
								Address: fmt.Sprintf("127.0.0.1:%d", 8200+j),
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
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", 9200+i)),
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

	Context("When deleting a ledger", func() {
		var (
			leaderID   uint64
			ledgerName = "test-ledger-to-delete"
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
					Driver: components.CreateLedgerRequestDriverSqliteMattn.ToPointer(),
				},
			})
			Expect(err).To(Succeed())

			// Verify the ledger exists
			ledger, err := servers[leaderID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
				LedgerName: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
		})

		It("Should successfully delete the ledger", func() {
			// Delete the ledger
			resp, err := servers[leaderID-1].client.Ledgers.DeleteLedger(ctx, operations.DeleteLedgerRequest{
				LedgerName: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger no longer exists
			Eventually(func(g Gomega) bool {
				_, err := servers[leaderID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
					LedgerName: ledgerName,
				})
				return err != nil
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Verify the ledger is not in the list of all ledgers
			ledgers, err := servers[leaderID-1].client.Ledgers.ListAllLedgers(ctx)
			Expect(err).To(Succeed())
			for _, ledger := range ledgers.ListAllLedgersResponse.Data {
				Expect(ledger.Name).NotTo(Equal(ledgerName))
			}
		})

		It("Should propagate deletion to all nodes", func() {
			// Delete the ledger from the leader
			_, err := servers[leaderID-1].client.Ledgers.DeleteLedger(ctx, operations.DeleteLedgerRequest{
				LedgerName: ledgerName,
			})
			Expect(err).To(Succeed())

			// Wait for deletion to propagate to all nodes
			Eventually(func(g Gomega) bool {
				for i := range countInstances {
					_, err := servers[i].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
						LedgerName: ledgerName,
					})
					if err == nil {
						return false
					}
				}
				return true
			}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Verify all nodes don't have the ledger in their list
			for i := range countInstances {
				ledgers, err := servers[i].client.Ledgers.ListAllLedgers(ctx)
				Expect(err).To(Succeed())
				for _, ledger := range ledgers.ListAllLedgersResponse.Data {
					Expect(ledger.Name).NotTo(Equal(ledgerName))
				}
			}
		})

		It("Should return 404 when trying to delete a non-existent ledger", func() {
			// Try to delete a non-existent ledger
			_, err := servers[leaderID-1].client.Ledgers.DeleteLedger(ctx, operations.DeleteLedgerRequest{
				LedgerName: "non-existent-ledger",
			})
			Expect(err).NotTo(BeNil())
			// The error should indicate 404 Not Found
		})
	})

	Context("When deleting a ledger with transactions", func() {
		var (
			leaderID   uint64
			ledgerName = "ledger-with-transactions"
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
					Driver: components.CreateLedgerRequestDriverSqliteMattn.ToPointer(),
				},
			})
			Expect(err).To(Succeed())

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
					LedgerName: ledgerName,
					CreateTransactionRequest: components.CreateTransactionRequest{
						Postings: []components.PostingRequest{{
							Source:      "world",
							Destination: fmt.Sprintf("account-%d", i),
							Amount:      big.NewInt(100 * int64(i+1)),
							Asset:       "USD",
						}},
					},
				})
				Expect(err).To(Succeed())
			}
		})

		FIt("Should successfully delete the ledger even with transactions", func() {
			// Delete the ledger (should succeed even with transactions)
			resp, err := servers[leaderID-1].client.Ledgers.DeleteLedger(ctx, operations.DeleteLedgerRequest{
				LedgerName: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger no longer exists
			Eventually(func(g Gomega) bool {
				_, err := servers[leaderID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
					LedgerName: ledgerName,
				})
				return err != nil
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())
		})
	})
})
