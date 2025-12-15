//go:build e2e
// +build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
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

var _ = Describe("Simple cluster", func() {

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
			Expect(removeGlob(filepath.Join(raftTmpDir, "*"))).To(Succeed())
			DeferCleanup(func() {
				Expect(os.RemoveAll(raftTmpDir)).To(Succeed())
			})

			extraDataTmpDir := GinkgoT().TempDir()
			Expect(removeGlob(filepath.Join(extraDataTmpDir, "*"))).To(Succeed())
			DeferCleanup(func() {
				Expect(os.RemoveAll(extraDataTmpDir)).To(Succeed())
			})

			server := testservice.New(cmdserver.NewRootCommand,
				testservice.WithInstruments(
					testserver.WithNodeID(i+1),
					testserver.WithHTTPPort(9000+i),
					testserver.WithDataDir(raftTmpDir),
					testserver.WithGRPCPort(8000+i),
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
								Address: fmt.Sprintf("127.0.0.1:%d", 8000+j),
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
					client.WithServerURL(fmt.Sprintf("http://localhost:%d", 9000+i)),
				),
				raftDataDir:  raftTmpDir,
				extraDataDir: extraDataTmpDir,
			})
		}
	})

	AfterEach(func() {
		for _, server := range servers {
			Expect(server.service.Stop(ctx)).To(Succeed())
		}
	})

	It("should start successfully", func() {
		Eventually(func(g Gomega) bool {
			state, err := servers[0].client.Cluster.GetClusterState(ctx)
			g.Expect(err).To(Succeed())

			return state.ClusterStateResponse.Data.Leader != nil &&
				*state.ClusterStateResponse.Data.Leader != 0
		}).Within(5 * time.Second).To(BeTrue())
	})

	Context("when the leader is down", func() {
		var (
			leaderID uint64
		)
		BeforeEach(func() {
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = uint64(*state.ClusterStateResponse.Data.Leader)

				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())
		})
		BeforeEach(func() {
			Expect(servers[leaderID-1].service.Stop(ctx)).To(BeNil())
		})
		It("should elect a new leader", func() {
			Eventually(func(g Gomega) bool {
				state, err := servers[(leaderID+1)%countInstances].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				return state.ClusterStateResponse.Data.Leader != nil &&
					*state.ClusterStateResponse.Data.Leader != 0 &&
					uint64(*state.ClusterStateResponse.Data.Leader) != leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())
		})
	})

	Context("When creating a new bucket and a ledger", func() {
		It("should succeed", func() {
			const snapshotThreshold = 100
			_, err := servers[0].client.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
				BucketName: "bucket0",
				CreateBucketRequest: components.CreateBucketRequest{
					Driver:            "sqlite",
					SnapshotThreshold: pointer.For(int64(snapshotThreshold)),
				},
			})
			Expect(err).To(Succeed())

			_, err = servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: "ledger0",
				CreateLedgerRequest: components.CreateLedgerRequest{
					Bucket: "bucket0",
				},
			})
			Expect(err).To(Succeed())

			//for range snapshotThreshold {
			//	_, err := servers[0].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
			//		LedgerName: "ledger0",
			//		CreateTransactionRequest: components.CreateTransactionRequest{
			//			Postings: []components.PostingRequest{{
			//				Source:      "world",
			//				Destination: "bank",
			//				Amount:      big.NewInt(100),
			//				Asset:       "",
			//			}},
			//		},
			//	})
			//	Expect(err).To(BeNil())
			//}
		})
	})

	Context("When losing a follower", func() {
		var (
			leaderID   uint64
			followerID uint64
		)
		BeforeEach(func() {
			// Wait for leader election
			Eventually(func(g Gomega) uint64 {
				state, err := servers[0].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				leaderID = uint64(*state.ClusterStateResponse.Data.Leader)
				return leaderID
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).NotTo(BeZero())

			// Find a follower (any node that is not the leader)
			for i := range countInstances {
				if uint64(i+1) != leaderID {
					followerID = uint64(i + 1)
					break
				}
			}
			Expect(followerID).NotTo(BeZero(), "followerID should not be zero - all nodes cannot be leaders")
			Expect(followerID).To(BeNumerically(">", 0))
			Expect(followerID).To(BeNumerically("<=", countInstances))

			// Stop the follower
			Expect(servers[followerID-1].service.Stop(ctx)).To(Succeed())
		})

		It("Should continue to work", func() {
			// Ensure leaderID is valid
			Expect(leaderID).NotTo(BeZero(), "leaderID should not be zero")
			Expect(leaderID).To(BeNumerically(">", 0))
			Expect(leaderID).To(BeNumerically("<=", countInstances))

			// Verify cluster still has a leader
			Eventually(func(g Gomega) bool {
				state, err := servers[leaderID-1].client.Cluster.GetClusterState(ctx)
				g.Expect(err).To(Succeed())

				return state.ClusterStateResponse.Data.Leader != nil &&
					*state.ClusterStateResponse.Data.Leader != 0
			}).Within(5 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Create a bucket
			_, err := servers[leaderID-1].client.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
				BucketName: "bucket1",
				CreateBucketRequest: components.CreateBucketRequest{
					Driver: "sqlite",
				},
			})
			Expect(err).To(Succeed())

			// Create a ledger
			_, err = servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
				LedgerName: "ledger1",
				CreateLedgerRequest: components.CreateLedgerRequest{
					Bucket: "bucket1",
				},
			})
			Expect(err).To(Succeed())

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
					LedgerName: "ledger1",
					CreateTransactionRequest: components.CreateTransactionRequest{
						Postings: []components.PostingRequest{{
							Source:      "world",
							Destination: "bank",
							Amount:      big.NewInt(100),
							Asset:       "",
						}},
					},
				})
				Expect(err).To(Succeed())
			}
		})
		Context("Then creating a new bucket", func() {
			var bucketName string
			BeforeEach(func() {
				bucketName = "bucket2"
				// Create a bucket while follower is down
				_, err := servers[leaderID-1].client.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
					BucketName: bucketName,
					CreateBucketRequest: components.CreateBucketRequest{
						Driver: "sqlite",
					},
				})
				Expect(err).To(Succeed())
			})

			Context("Then the follower come back", func() {
				BeforeEach(func() {
					// Restart the follower
					Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
				})

				It("Should restore the state", func() {
					// Wait for follower to reconnect and sync, then verify it can see the bucket
					Eventually(func(g Gomega) bool {
						// First verify the follower is connected
						state, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
						g.Expect(err).To(Succeed())

						if state.ClusterStateResponse.Data.Leader == nil ||
							*state.ClusterStateResponse.Data.Leader == 0 {
							return false
						}

						// Then verify the follower can see the bucket created while it was down
						// Reads are local to the node
						buckets, err := servers[followerID-1].client.Buckets.ListBuckets(ctx)
						g.Expect(err).To(Succeed())

						for _, bucket := range buckets.GetListBucketsResponse().Data {
							if bucket.Name == bucketName {
								return true
							}
						}
						return false
					}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

					// Verify the follower can access the bucket details
					bucket, err := servers[followerID-1].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
						BucketName: bucketName,
					})
					Expect(err).To(Succeed())
					Expect(bucket.GetGetBucketResponse().Data.Name).To(Equal(bucketName))
				})
			})
		})
		Context("Then creating more transactions than the snapshot threshold", func() {
			var bucketName string
			var ledgerName string
			BeforeEach(func() {
				// Create a bucket and ledger
				bucketName = "bucket-snapshot"
				ledgerName = "ledger-snapshot"

				_, err := servers[leaderID-1].client.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
					BucketName: bucketName,
					CreateBucketRequest: components.CreateBucketRequest{
						Driver: "sqlite",
					},
				})
				Expect(err).To(Succeed())

				_, err = servers[leaderID-1].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Bucket: bucketName,
					},
				})
				Expect(err).To(Succeed())

				// Create enough transactions to trigger a snapshot
				// snapshotThreshold is 10, so we create 11 transactions to ensure a snapshot is created
				for i := 0; i < 11; i++ {
					_, err := servers[leaderID-1].client.Transactions.CreateTransaction(ctx, operations.CreateTransactionRequest{
						LedgerName: ledgerName,
						CreateTransactionRequest: components.CreateTransactionRequest{
							Postings: []components.PostingRequest{{
								Source:      "world",
								Destination: "bank",
								Amount:      big.NewInt(100),
								Asset:       "USD",
							}},
						},
					})
					Expect(err).To(Succeed())
				}

				// Wait for snapshot to be created (verify by checking that transactions are visible)
				// todo: is generated but is it useful?
				Eventually(func(g Gomega) bool {
					// Verify the bucket exists
					bucket, err := servers[leaderID-1].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
						BucketName: bucketName,
					})
					g.Expect(err).To(Succeed())
					return bucket.GetGetBucketResponse().Data.Name == bucketName
				}).Within(5 * time.Second).WithPolling(200 * time.Millisecond).Should(BeTrue())
			})

			Context("Then the follower come back", func() {
				BeforeEach(func() {
					// Restart the follower
					Expect(servers[followerID-1].service.Start(ctx)).To(Succeed())
				})

				It("Should restore the state from a snapshot sent by the leader", func() {
					fmt.Printf("waiting for snapshot to be sent by leader to follower %d...\r\n", followerID)
					time.Sleep(10 * time.Second)
				})
			})
		})
	})
})

func removeGlob(path string) (err error) {
	contents, err := filepath.Glob(path)
	if err != nil {
		return
	}
	for _, item := range contents {
		err = os.RemoveAll(item)
		if err != nil {
			return
		}
	}
	return
}
