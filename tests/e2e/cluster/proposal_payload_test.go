//go:build e2e

package cluster

import (
	"slices"
	"sync/atomic"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

var _ = Describe("Proposal payload replication", func() {
	It("replicates batches and policy controls across transfer and restart", func() {
		ctx, servers, gateway, leaderID := testutil.SetupMultiNodeCluster(3, testutil.WithGateway())
		DeferCleanup(func() { testutil.StopServers(ctx, servers) })
		var batches, policies atomic.Uint64
		var invalid atomic.Bool
		gateway.SetInterceptor(testserver.MessageInterceptorFunc(func(message *raftpb.Message) bool {
			// ReadIndex messages also use Entry.Data, but carry opaque request
			// contexts rather than proposals. Inspect actual log replication.
			if message.GetType() != raftpb.MsgApp {
				return true
			}
			for _, entry := range message.GetEntries() {
				if entry.GetType() != raftpb.EntryNormal || len(entry.GetData()) == 0 {
					continue
				}
				p := &raftcmdpb.Proposal{}
				if p.UnmarshalVT(entry.GetData()) != nil {
					invalid.Store(true)

					continue
				}
				if len(p.GetOrders()) != 0 || len(p.GetTechnicalUpdates()) != 0 {
					invalid.Store(true)
				}
				if len(p.GetBatch().GetOrders()) > 0 {
					batches.Add(1)
				}
				if p.GetSetClusterPolicy().GetPolicy().GetRevision() == 100 {
					policies.Add(1)
				}
			}

			return true
		}))
		DeferCleanup(gateway.RemoveInterceptor)

		_, err := servers[*leaderID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("payload", nil)))
		Expect(err).To(Succeed())
		Eventually(batches.Load).Should(BeNumerically(">", 0))
		createTxs(ctx, servers[*leaderID-1].Client, "payload", 2, 100)

		// Reconfigure a follower, then make it leader so its reconciler emits a
		// control using the oneof wire format, alongside business batches.
		oldLeader := *leaderID
		target := oldLeader%3 + 1
		instruments := append(slices.Clone(servers[target-1].Service.Instruments),
			testserver.WithClusterPolicyRevision(100), testserver.WithIdempotencyTTL(2*time.Hour))
		testutil.RestartNodeWithInstruments(ctx, servers[target-1], instruments)
		Eventually(servers[target-1]).WithTimeout(30 * time.Second).Should(BeFollower())
		_, err = servers[oldLeader-1].ClusterClient.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{Transferee: target})
		Expect(err).To(Succeed())
		Eventually(policies.Load).WithTimeout(30 * time.Second).Should(BeNumerically(">", 0))
		*leaderID = target
		createTxs(ctx, servers[target-1].Client, "payload", 2, 100)
		testutil.RestartNodeWithInstruments(ctx, servers[oldLeader-1], servers[oldLeader-1].Service.Instruments)
		Eventually(servers[oldLeader-1]).WithTimeout(30 * time.Second).Should(BeFollower())
		expectVolumeAllNodes(ctx, servers, "payload", "400")
		Expect(invalid.Load()).To(BeFalse())
	})
})
