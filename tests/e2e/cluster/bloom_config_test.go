//go:build e2e

package cluster

import (
	"context"
	"math/big"
	"strconv"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func withBloomVolumesExpectedKeys(n uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, cfg *testservice.RunConfiguration) error {
		cfg.AppendArgs("--bloom-volumes-expected-keys", strconv.FormatUint(n, 10))

		return nil
	}
}

// rollingUpdateBloomConfig performs a rolling restart of all nodes with new bloom config.
func rollingUpdateBloomConfig(
	ctx context.Context,
	servers []*testutil.ServiceWithClient,
	leaderID *uint64,
	expectedKeys uint64,
) {
	lid := *leaderID

	// Restart followers first with new bloom config.
	for i := range len(servers) {
		nodeID := uint64(i + 1)
		if nodeID == lid {
			continue
		}

		newInstruments := append(servers[i].Service.Instruments, withBloomVolumesExpectedKeys(expectedKeys))
		testutil.RestartNodeWithInstruments(ctx, servers[i], newInstruments)

		Eventually(servers[i]).
			WithTimeout(30 * time.Second).
			WithPolling(500 * time.Millisecond).
			Should(BeFollower())
	}

	// Transfer leadership to an upgraded follower.
	var targetID uint64
	for i := range len(servers) {
		if uint64(i+1) != lid {
			targetID = uint64(i + 1)

			break
		}
	}

	resp, err := servers[lid-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
		Transferee: uint32(targetID),
	})
	Expect(err).To(Succeed())
	Expect(resp.NewLeader).To(Equal(uint32(targetID)))
	*leaderID = targetID

	// Wait for all nodes to see the new leader.
	for i := range len(servers) {
		Eventually(func(g Gomega) uint64 {
			state, err := servers[i].ClusterClient.GetClusterState(
				context.Background(),
				&clusterpb.GetClusterStateRequest{NodeId: servers[i].NodeID},
			)
			g.Expect(err).To(Succeed())

			return uint64(state.Leader)
		}).Should(Equal(targetID))
	}

	// Wait for bloom config to propagate via Raft.
	for i := range len(servers) {
		Eventually(func(g Gomega) uint64 {
			state, err := servers[i].ClusterClient.GetClusterState(
				context.Background(),
				&clusterpb.GetClusterStateRequest{NodeId: servers[i].NodeID},
			)
			g.Expect(err).To(Succeed())

			return state.GetClusterConfig().GetBloomVolumes().GetExpectedKeys()
		}).
			WithTimeout(10 * time.Second).
			WithPolling(500 * time.Millisecond).
			Should(Equal(expectedKeys))
	}

	// Restart old leader with new config.
	oldLeaderIdx := int(lid - 1)
	newInstruments := append(servers[oldLeaderIdx].Service.Instruments, withBloomVolumesExpectedKeys(expectedKeys))
	testutil.RestartNodeWithInstruments(ctx, servers[oldLeaderIdx], newInstruments)

	Eventually(servers[oldLeaderIdx]).
		WithTimeout(30 * time.Second).
		WithPolling(500 * time.Millisecond).
		Should(HaveALeader(leaderID))
}

var _ = Describe("Bloom filter config change preserves data", Ordered, func() {
	const countInstances = 3

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
	)

	BeforeAll(func() {
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(
			countInstances,
			testutil.TestRaftBasePort,
			testutil.TestServiceBasePort,
			testutil.TestHTTPBasePort,
			testutil.TestGatewayBasePort,
			// Default instruments already include WithBloomTestConfig() (volumes=10000)
		)
	})

	AfterAll(func() { testutil.StopServers(ctx, servers) })

	It("should create data with initial bloom config", func() {
		client := servers[*leaderID-1].Client

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("bloom-test", nil)))
		Expect(err).To(Succeed())

		// Create enough transactions to populate bloom filters and trigger rotations.
		createTxs(ctx, client, "bloom-test", 20, 100)
		expectVolumeAllNodes(ctx, servers, "bloom-test", "2000")

		// Also set metadata to exercise the metadata bloom filter.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.SaveAccountMetadataAction("bloom-test", "bank", map[string]string{
			"category": "main",
		})))
		Expect(err).To(Succeed())
	})

	It("should preserve volumes after bloom config change (resize up)", func() {
		// Change bloom volumes from 10000 to 50000 — filter is rebuilt with more blocks.
		rollingUpdateBloomConfig(ctx, servers, leaderID, 50000)

		// All existing data must still be accessible — no false negatives.
		expectVolumeAllNodes(ctx, servers, "bloom-test", "2000")

		// Verify metadata survived too.
		for _, srv := range servers {
			Eventually(func(g Gomega) {
				account, err := actions.GetAccount(ctx, srv.Client, "bloom-test", "bank")
				g.Expect(err).To(Succeed())

				v := actions.FindMetadataValue(account.Metadata, "category")
				g.Expect(v).NotTo(BeNil())
				g.Expect(v.GetStringValue()).To(Equal("main"))
			}).
				WithTimeout(30 * time.Second).
				WithPolling(500 * time.Millisecond).
				Should(Succeed())
		}
	})

	It("should handle new transactions after bloom resize", func() {
		client := servers[*leaderID-1].Client

		createTxs(ctx, client, "bloom-test", 30, 100)
		expectVolumeAllNodes(ctx, servers, "bloom-test", "5000")
	})

	It("should preserve volumes after bloom config change (resize down)", func() {
		// Change bloom volumes from 50000 to 5000 — filter rebuilt with fewer blocks.
		rollingUpdateBloomConfig(ctx, servers, leaderID, 5000)

		expectVolumeAllNodes(ctx, servers, "bloom-test", "5000")
	})

	It("should handle transactions after second resize", func() {
		client := servers[*leaderID-1].Client

		createTxs(ctx, client, "bloom-test", 10, 100)
		expectVolumeAllNodes(ctx, servers, "bloom-test", "6000")
	})

	It("should handle new ledger after bloom config changes", func() {
		client := servers[*leaderID-1].Client

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("post-bloom-change", nil)))
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction("post-bloom-change", []*commonpb.Posting{
			actions.NewPosting("world", "user:1", big.NewInt(999), "EUR"),
		}, nil, nil)))
		Expect(err).To(Succeed())

		expectVolumeAllNodes(ctx, servers, "bloom-test", "6000")

		for _, srv := range servers {
			Eventually(func(g Gomega) {
				account, err := actions.GetAccount(ctx, srv.Client, "post-bloom-change", "user:1")
				g.Expect(err).To(Succeed())
				g.Expect(account.Volumes["EUR"].Input).To(Equal("999"))
			}).
				WithTimeout(30 * time.Second).
				WithPolling(500 * time.Millisecond).
				Should(Succeed())
		}
	})
})
