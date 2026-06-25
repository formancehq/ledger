//go:build e2e

package cluster

import (
	"context"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Cluster server version", Ordered, func() {
	const countInstances = 3

	var (
		ctx     context.Context
		servers []*testutil.ServiceWithClient
	)

	BeforeAll(func() {
		ctx, servers, _, _ = testutil.SetupMultiNodeCluster(
			countInstances,
			testutil.TestRaftBasePort, testutil.TestServiceBasePort, testutil.TestHTTPBasePort, testutil.TestGatewayBasePort,
		)
	})

	// SetupMultiNodeCluster does not stop the node servers on its own, so the
	// nodes must be stopped explicitly to release the shared base ports — every
	// other cluster spec does this. Omitting it leaves ports 15000-15300 bound
	// and the next cluster container panics with "bind: address already in use".
	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("Should report a non-empty version for every node in the cluster state", func() {
		// GetClusterState is routed to the bootstrap node, which is the leader
		// after SetupMultiNodeCluster. Peer-version population can lag the join,
		// so poll until every NodeInfo carries a version.
		Eventually(func(g Gomega) {
			state, err := servers[0].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())

			nodes := state.GetNodes()
			// At least 2 nodes ensures the per-peer version mapping is actually
			// exercised, not just the local node's self-report.
			g.Expect(len(nodes)).To(BeNumerically(">=", 2))

			for _, node := range nodes {
				g.Expect(node.GetVersion()).NotTo(BeEmpty(), "expected node %d to report a version", node.GetId())
			}
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
