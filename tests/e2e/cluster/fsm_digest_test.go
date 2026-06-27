//go:build e2e

package cluster

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// fsm_digest_test validates the cross-node FSM digest end-to-end:
//
//   - When the cluster is bootstrapped with fsm_determinism_enabled=true,
//     every node maintains a rolling digest under SubGlobFSMDigest. After a
//     workload, GetFSMDigest at a common applied index must return
//     byte-identical (digest, snapshotIndex, hashVersion) on every peer.
//
//   - When the cluster is bootstrapped with the flag OFF (default),
//     GetFSMDigest must return FAILED_PRECONDITION on every node — the
//     opt-in gate works.
//
// This is the local-reproducible counterpart of the Antithesis assertion in
// tests/antithesis/workload/bin/cmds/main/eventually_fsm_digest_matches.
// The Antithesis assertion exercises the same invariant under fault injection;
// this E2E test exercises it under a happy-path workload, runs in CI, and
// keeps it green without the Antithesis harness.

var _ = Describe("FSM cross-node digest", func() {
	const (
		countInstances = 3
		ledgerName     = "fsm-digest-ledger"
	)

	Context("with fsm_determinism_enabled=true on every node", Ordered, func() {
		var (
			ctx     context.Context
			servers []*testutil.ServiceWithClient
		)

		BeforeAll(func() {
			ctx, servers, _, _ = testutil.SetupMultiNodeCluster(
				countInstances,
				testutil.TestRaftBasePort,
				testutil.TestServiceBasePort,
				testutil.TestHTTPBasePort,
				testutil.TestGatewayBasePort,
				testutil.WithFSMDeterminismEnabled(),
			)
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("returns a byte-identical digest on every peer at a common applied index", func() {
			By("Creating a ledger and applying a small workload")

			_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest(
				"", actions.CreateLedgerAction(ledgerName, nil),
			))
			Expect(err).To(Succeed())

			// Modest workload — 10 transactions touching world + users:N — enough
			// to exercise volumes / metadata / transactions maps with map-key
			// sorts on the deterministic marshal path.
			for i := 0; i < 10; i++ {
				_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest(
					"",
					actions.CreateScriptTransactionAction(
						ledgerName,
						fmt.Sprintf(
							`send [COIN %d] (source = @world destination = @users:%d)`,
							(i+1)*100, i%3,
						),
						nil,
						map[string]string{"label": fmt.Sprintf("tx-%d", i)},
					),
				))
				Expect(err).To(Succeed())
			}

			By("Pinning a common applied index via Barrier and querying each node's digest")

			barrier, err := servers[0].Client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())

			commitIndex := barrier.GetCommitIndex()
			Expect(commitIndex).To(BeNumerically(">", uint64(0)))

			type nodeDigest struct {
				addr   string
				nodeID uint32
				resp   *clusterpb.FSMDigest
			}

			collect := make([]nodeDigest, 0, countInstances)
			for _, srv := range servers {
				resp, err := srv.ClusterClient.GetFSMDigest(ctx, &clusterpb.GetFSMDigestRequest{
					Index:  commitIndex,
					WaitMs: 30_000,
				})
				Expect(err).To(Succeed(), "GetFSMDigest must succeed when the flag is ON on port=%d", srv.GRPCPort)

				Expect(resp.GetDigest()).NotTo(BeEmpty(), "digest must be non-empty after the workload")
				Expect(resp.GetAppliedIndex()).To(BeNumerically(">=", commitIndex))

				collect = append(collect, nodeDigest{
					addr:   fmt.Sprintf("port=%d", srv.GRPCPort),
					nodeID: srv.NodeID,
					resp:   resp,
				})
			}

			Expect(collect).To(HaveLen(countInstances))

			By("Asserting that every peer returned a byte-identical digest")

			ref := collect[0]
			for _, peer := range collect[1:] {
				Expect(bytes.Equal(ref.resp.GetDigest(), peer.resp.GetDigest())).To(
					BeTrue(),
					"digest must match across peers at applied=%d: ref=%s (%x) peer=%s (%x)",
					commitIndex, ref.addr, ref.resp.GetDigest(), peer.addr, peer.resp.GetDigest(),
				)
				Expect(peer.resp.GetAppliedIndex()).To(Equal(ref.resp.GetAppliedIndex()),
					"applied index must match across peers")
				Expect(peer.resp.GetSnapshotIndex()).To(Equal(ref.resp.GetSnapshotIndex()),
					"snapshot index must match across peers")
				Expect(peer.resp.GetHashVersion()).To(Equal(ref.resp.GetHashVersion()),
					"hash version must match across peers")
			}
		})
	})

	Context("with the flag OFF (default)", Ordered, func() {
		var (
			ctx     context.Context
			servers []*testutil.ServiceWithClient
		)

		BeforeAll(func() {
			ctx, servers, _, _ = testutil.SetupMultiNodeCluster(
				countInstances,
				testutil.TestRaftBasePort+100,
				testutil.TestServiceBasePort+100,
				testutil.TestHTTPBasePort+100,
				testutil.TestGatewayBasePort+100,
			)
		})

		AfterAll(func() {
			testutil.StopServers(ctx, servers)
		})

		It("returns FAILED_PRECONDITION on every peer", func() {
			for _, srv := range servers {
				_, err := srv.ClusterClient.GetFSMDigest(ctx, &clusterpb.GetFSMDigestRequest{})
				Expect(err).To(HaveOccurred(),
					"GetFSMDigest must error when the cluster has the flag OFF (node %s)", fmt.Sprintf("port=%d", srv.GRPCPort))
				Expect(status.Code(err)).To(Equal(codes.FailedPrecondition),
					"the error must be FAILED_PRECONDITION, got %s on %s", status.Code(err), fmt.Sprintf("port=%d", srv.GRPCPort))
			}
		})
	})
})

// Silence unused-import warnings if any of the imports above is conditional
// on future code paths. Removing this when the test grows is fine.
var _ = []interface{}{(*commonpb.Account)(nil), (*big.Int)(nil), time.Now}
