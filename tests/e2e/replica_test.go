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
		BeforeEach(func() {
			_, err := servers[0].client.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
				BucketName: "bucket0",
				CreateBucketRequest: components.CreateBucketRequest{
					Driver: "sqlite",
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
		})
		It("should succeed", func() {})
		Context("Then deleting the bucket", func() {
			BeforeEach(func() {
				_, err := servers[0].client.Buckets.DeleteBucket(ctx, operations.DeleteBucketRequest{
					BucketName: "bucket0",
				})
				Expect(err).To(Succeed())
			})
			It("Should succeed", func() {})
		})
	})

	Context("When creating a ledger with automatic bucket creation", func() {
		Context("When bucket is empty string", func() {
			var ledgerName string
			BeforeEach(func() {
				ledgerName = "auto-ledger-empty-bucket"
				// Create ledger with empty bucket - should create bucket with ledger name
				_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Bucket: "", // Empty bucket should trigger auto-creation
					},
				})
				Expect(err).To(Succeed())
			})

			It("should create a bucket with the ledger name", func() {
				// Verify the bucket was created with the ledger name
				bucket, err := servers[0].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
					BucketName: ledgerName,
				})
				Expect(err).To(Succeed())
				Expect(bucket.GetGetBucketResponse().Data.Name).To(Equal(ledgerName))
				Expect(string(bucket.GetGetBucketResponse().Data.Driver)).To(Equal("sqlite"))
			})

			It("should have created the ledger in the auto-created bucket", func() {
				ledger, err := servers[0].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
					LedgerName: ledgerName,
				})
				Expect(err).To(Succeed())
				Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
			})
		})

		Context("When bucket is not specified (omitted)", func() {
			var ledgerName string
			BeforeEach(func() {
				ledgerName = "auto-ledger-no-bucket"
				// Create ledger without bucket field - should create bucket with ledger name
				_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						// Bucket field omitted
					},
				})
				Expect(err).To(Succeed())
			})

			It("should create a bucket with the ledger name", func() {
				// Verify the bucket was created with the ledger name
				bucket, err := servers[0].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
					BucketName: ledgerName,
				})
				Expect(err).To(Succeed())
				Expect(bucket.GetGetBucketResponse().Data.Name).To(Equal(ledgerName))
				Expect(bucket.GetGetBucketResponse().Data.Driver).To(Equal("sqlite"))
			})

			It("should have created the ledger in the auto-created bucket", func() {
				ledger, err := servers[0].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
					LedgerName: ledgerName,
				})
				Expect(err).To(Succeed())
				Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
			})
		})

		Context("When bucket does not exist", func() {
			var bucketName string
			var ledgerName string
			BeforeEach(func() {
				bucketName = "non-existent-bucket"
				ledgerName = "ledger-in-new-bucket"
				// Create ledger with a bucket that doesn't exist - should create the bucket automatically
				_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Bucket: bucketName,
					},
				})
				Expect(err).To(Succeed())
			})

			It("should create the bucket automatically", func() {
				// Verify the bucket was created
				bucket, err := servers[0].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
					BucketName: bucketName,
				})
				Expect(err).To(Succeed())
				Expect(bucket.GetGetBucketResponse().Data.Name).To(Equal(bucketName))
				Expect(bucket.GetGetBucketResponse().Data.Driver).To(Equal("sqlite"))
			})

			It("should have created the ledger in the auto-created bucket", func() {
				ledger, err := servers[0].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
					LedgerName: ledgerName,
				})
				Expect(err).To(Succeed())
				Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
			})
		})

		Context("When bucket already exists", func() {
			var bucketName string
			var ledgerName string
			BeforeEach(func() {
				bucketName = "existing-bucket"
				ledgerName = "ledger-in-existing-bucket"
				// Create bucket first
				_, err := servers[0].client.Buckets.CreateBucket(ctx, operations.CreateBucketRequest{
					BucketName: bucketName,
					CreateBucketRequest: components.CreateBucketRequest{
						Driver: "sqlite",
					},
				})
				Expect(err).To(Succeed())

				// Create ledger in existing bucket
				_, err = servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledgerName,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Bucket: bucketName,
					},
				})
				Expect(err).To(Succeed())
			})

			It("should use the existing bucket", func() {
				// Verify the bucket still exists and wasn't recreated
				bucket, err := servers[0].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
					BucketName: bucketName,
				})
				Expect(err).To(Succeed())
				Expect(bucket.GetGetBucketResponse().Data.Name).To(Equal(bucketName))
			})

			It("should have created the ledger in the existing bucket", func() {
				ledger, err := servers[0].client.Ledgers.GetLedger(ctx, operations.GetLedgerRequest{
					LedgerName: ledgerName,
				})
				Expect(err).To(Succeed())
				Expect(ledger.GetGetLedgerResponse().Data.Name).To(Equal(ledgerName))
			})
		})

		Context("When creating multiple ledgers with empty bucket", func() {
			It("should create separate buckets for each ledger", func() {
				ledger1Name := "multi-ledger-1"
				ledger2Name := "multi-ledger-2"

				// Create first ledger with empty bucket
				_, err := servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledger1Name,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Bucket: "",
					},
				})
				Expect(err).To(Succeed())

				// Create second ledger with empty bucket
				_, err = servers[0].client.Ledgers.CreateLedger(ctx, operations.CreateLedgerRequest{
					LedgerName: ledger2Name,
					CreateLedgerRequest: components.CreateLedgerRequest{
						Bucket: "",
					},
				})
				Expect(err).To(Succeed())

				// Verify both buckets were created
				bucket1, err := servers[0].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
					BucketName: ledger1Name,
				})
				Expect(err).To(Succeed())
				Expect(bucket1.GetGetBucketResponse().Data.Name).To(Equal(ledger1Name))

				bucket2, err := servers[0].client.Buckets.GetBucket(ctx, operations.GetBucketRequest{
					BucketName: ledger2Name,
				})
				Expect(err).To(Succeed())
				Expect(bucket2.GetGetBucketResponse().Data.Name).To(Equal(ledger2Name))

				// Verify they are different buckets
				Expect(bucket1.GetGetBucketResponse().Data.Name).NotTo(Equal(bucket2.GetGetBucketResponse().Data.Name))
			})
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
					// Wait for follower to reconnect and sync
					Eventually(func(g Gomega) bool {
						// Verify the follower is connected
						state, err := servers[followerID-1].client.Cluster.GetClusterState(ctx)
						g.Expect(err).To(Succeed())

						return state.ClusterStateResponse.Data.Leader != nil &&
							*state.ClusterStateResponse.Data.Leader != 0
					}).Within(10 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

					// Get the leader's bucket state (local) to compare
					localTrue := true
					leaderState, err := servers[leaderID-1].client.Buckets.GetBucketRaftState(ctx, operations.GetBucketRaftStateRequest{
						BucketName: bucketName,
						Local:      &localTrue,
					})
					Expect(err).To(Succeed())
					leaderBucketState := leaderState.GetBucketClusterStateResponse()
					Expect(leaderBucketState).NotTo(BeNil())
					leaderInnerState := leaderBucketState.Data.GetInnerState()

					// Verify the leader has the expected state
					Expect(leaderInnerState.GetLedgers()).To(HaveKey(ledgerName))
					Expect(leaderInnerState.GetLastSequence()).To(BeNumerically(">=", 11)) // At least 11 transactions

					// Wait for follower to sync and verify its state matches
					Eventually(func(g Gomega) bool {
						// Get follower's local bucket state
						followerState, err := servers[followerID-1].client.Buckets.GetBucketRaftState(ctx, operations.GetBucketRaftStateRequest{
							BucketName: bucketName,
							Local:      &localTrue,
						})
						if err != nil {
							return false
						}

						followerBucketState := followerState.GetBucketClusterStateResponse()
						if followerBucketState == nil {
							return false
						}

						followerInnerState := followerBucketState.Data.GetInnerState()

						// Verify follower has the ledger
						followerLedgers := followerInnerState.GetLedgers()
						if _, exists := followerLedgers[ledgerName]; !exists {
							return false
						}

						// Verify follower's lastSequence matches leader's lastSequence
						// (allowing for some delay in synchronization)
						followerSeq := followerInnerState.GetLastSequence()
						leaderSeq := leaderInnerState.GetLastSequence()

						return followerSeq == leaderSeq
					}).Within(5 * time.Second).WithPolling(100 * time.Millisecond).To(BeTrue())
				})
			})
		})
	})
})
