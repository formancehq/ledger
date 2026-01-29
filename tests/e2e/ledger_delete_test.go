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

var _ = Describe("Ledger Deletion", func() {
	var (
		ctx     context.Context
		servers []serviceWithClient
	)
	const (
		countInstances = 3
		httpPortBase   = 9200
		grpcPortBase   = 8200
	)

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

		// Wait for leader election
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

	Context("When deleting a ledger", func() {
		var (
			leaderID   uint64
			ledgerName = "test-ledger-to-delete"
			ledgerID   uint32
		)

		BeforeEach(func() {
			// Get leader ID
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.GetClusterState(ctx, &servicepb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())

				leaderID = uint64(state.Leader)
				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Create a ledger
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Extract ledger ID from the response
			log := resp.Logs[0]
			createLog := log.Payload.GetCreateLedger()
			Expect(createLog).NotTo(BeNil())
			ledgerID = createLog.Info.Id

			// Verify the ledger exists
			ledger, err := servers[leaderID-1].client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("Should successfully delete the ledger (hard delete)", func() {
			// Delete the ledger
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{deleteLedgerAction(ledgerID)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger is completely removed (hard delete)
			Eventually(func(g Gomega) bool {
				_, err := servers[leaderID-1].client.GetLedger(ctx, &servicepb.GetLedgerRequest{
					Ledger: &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
				})
				return err != nil
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Verify the ledger is not in the list of all ledgers
			ledgers, err := servers[leaderID-1].client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
			Expect(err).To(Succeed())
			for name := range ledgers.Ledgers {
				Expect(name).NotTo(Equal(ledgerName))
			}

			// Verify the ledger cannot be retrieved (hard delete)
			_, err = servers[leaderID-1].client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
			})
			Expect(err).To(HaveOccurred())

			// Verify the ledger does not appear in the list
			ledgers, err = servers[leaderID-1].client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
			Expect(err).To(Succeed())
			_, found := ledgers.Ledgers[ledgerName]
			Expect(found).To(BeFalse())
		})

		It("Should return error when trying to delete a non-existent ledger", func() {
			// Try to delete a non-existent ledger
			_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{deleteLedgerAction(99999)},
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When deleting a ledger with transactions", func() {
		var (
			leaderID   uint64
			ledgerName = "ledger-with-transactions"
			ledgerID   uint32
		)

		BeforeEach(func() {
			// Get leader ID
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.GetClusterState(ctx, &servicepb.GetClusterStateRequest{})
				g.Expect(err).To(Succeed())

				leaderID = uint64(state.Leader)
				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Create a ledger
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Extract ledger ID
			log := resp.Logs[0]
			createLog := log.Payload.GetCreateLedger()
			Expect(createLog).NotTo(BeNil())
			ledgerID = createLog.Info.Id

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
					Actions: []*servicepb.Action{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("account-%d", i), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should successfully delete the ledger even with transactions", func() {
			// Delete the ledger (should succeed even with transactions)
			resp, err := servers[leaderID-1].client.Apply(ctx, &servicepb.ApplyRequest{
				Actions: []*servicepb.Action{deleteLedgerAction(ledgerID)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger no longer exists
			Eventually(func(g Gomega) bool {
				_, err := servers[leaderID-1].client.GetLedger(ctx, &servicepb.GetLedgerRequest{
					Ledger: &servicepb.LedgerNameOrId{Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName}},
				})
				return err != nil
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())
		})
	})
})
