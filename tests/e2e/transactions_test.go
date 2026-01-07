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

var _ = Describe("Transactions", func() {
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

			server := testservice.New(cmdserver.NewRootCommand,
				testservice.WithInstruments(
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(9300+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(8300+i),
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
								Address: fmt.Sprintf("127.0.0.1:%d", 8300+j),
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
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", 9300+i)),
				),
				raftDataDir: raftTmpDir,
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

	Context("When creating ledgers and transactions with different drivers", func() {
		// Table-driven test: define test cases for each driver
		type driverTestCase struct {
			driverName                     string
			logStoreDriverEnum             components.CreateLedgerRequestLogStoreDriver
			runtimeStoreDriverEnum         components.CreateLedgerRequestRuntimeStoreDriver
			logStoreDriverResponseEnum     components.LogStoreDriver
			runtimeStoreDriverResponseEnum components.RuntimeStoreDriver
			description                    string
		}

		testCases := []driverTestCase{
			{
				driverName:                     "sqlite-mattn",
				logStoreDriverEnum:             components.CreateLedgerRequestLogStoreDriverSqliteMattn,
				runtimeStoreDriverEnum:         components.CreateLedgerRequestRuntimeStoreDriverSqliteMattn,
				logStoreDriverResponseEnum:     components.LogStoreDriverSqliteMattn,
				runtimeStoreDriverResponseEnum: components.RuntimeStoreDriverSqliteMattn,
				description:                    "SQLite Mattn driver (github.com/mattn/go-sqlite3)",
			},
			{
				driverName:                     "sqlite-modern",
				logStoreDriverEnum:             components.CreateLedgerRequestLogStoreDriverSqliteModern,
				runtimeStoreDriverEnum:         components.CreateLedgerRequestRuntimeStoreDriverSqliteModern,
				logStoreDriverResponseEnum:     components.LogStoreDriverSqliteModern,
				runtimeStoreDriverResponseEnum: components.RuntimeStoreDriverSqliteModern,
				description:                    "SQLite Modern driver (modernc.org/sqlite)",
			},
			{
				driverName:                     "pebble",
				logStoreDriverEnum:             components.CreateLedgerRequestLogStoreDriverPebble,
				runtimeStoreDriverEnum:         components.CreateLedgerRequestRuntimeStoreDriverPebble,
				logStoreDriverResponseEnum:     components.LogStoreDriverPebble,
				runtimeStoreDriverResponseEnum: components.RuntimeStoreDriverPebble,
				description:                    "Pebble driver (github.com/cockroachdb/pebble)",
			},
		}

		for _, tc := range testCases {
			tc := tc // capture loop variable
			Context(fmt.Sprintf("With %s driver", tc.driverName), func() {
				var (
					leaderID   uint64
					ledgerName string
				)

				BeforeEach(func() {
					// Get leader ID
					Eventually(func(g Gomega) uint64 {
						state, err := servers[0].client.Cluster.GetClusterState(ctx)
						g.Expect(err).To(Succeed())

						leaderID = uint64(*state.ClusterStateResponse.Data.Leader)
						return leaderID
					}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

					// Create ledger name unique per driver
					ledgerName = fmt.Sprintf("test-ledger-%s", tc.driverName)
				})

				It("Should create a ledger successfully", func() {
					// Create ledger with the specific driver
					resp, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							LogStoreDriver:     tc.logStoreDriverEnum,
							RuntimeStoreDriver: tc.runtimeStoreDriverEnum,
						},
					})
					Expect(err).To(Succeed(), "Failed to create ledger with driver %s", tc.driverName)
					Expect(resp).NotTo(BeNil())
					Expect(resp.GetCreateLedgerResponse()).NotTo(BeNil())
					Expect(resp.GetCreateLedgerResponse().Data.Name).To(Equal(ledgerName))
					Expect(resp.GetCreateLedgerResponse().Data.LogStoreDriver).To(Equal(tc.logStoreDriverResponseEnum))
					Expect(resp.GetCreateLedgerResponse().Data.RuntimeStoreDriver).To(Equal(tc.runtimeStoreDriverResponseEnum))

					// Verify the ledger can be retrieved
					ledger, err := servers[leaderID-1].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
						LedgerName: ledgerName,
					})
					Expect(err).To(Succeed())
					Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
					Expect(ledger.GetGetLedgerResponse().Data.LogStoreDriver).To(Equal(tc.logStoreDriverResponseEnum))
					Expect(ledger.GetGetLedgerResponse().Data.RuntimeStoreDriver).To(Equal(tc.runtimeStoreDriverResponseEnum))
				})

				It("Should create a transaction on the ledger", func() {
					// First, create the ledger
					_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							LogStoreDriver:     tc.logStoreDriverEnum,
							RuntimeStoreDriver: tc.runtimeStoreDriverEnum,
						},
					})
					Expect(err).To(Succeed())

					// Create a simple transaction
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
					// First, create the ledger
					_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							LogStoreDriver:     tc.logStoreDriverEnum,
							RuntimeStoreDriver: tc.runtimeStoreDriverEnum,
						},
					})
					Expect(err).To(Succeed())

					// Create multiple transactions
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
					// First, create the ledger
					_, err := servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
						LedgerName: ledgerName,
						CreateLedgerRequest: components.CreateLedgerRequest{
							LogStoreDriver:     tc.logStoreDriverEnum,
							RuntimeStoreDriver: tc.runtimeStoreDriverEnum,
						},
					})
					Expect(err).To(Succeed())

					// Create a transaction with transaction metadata
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
})
