//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// MetadataIndexPerReplicaConsistency pins the EN-1323 contract:
//
//   - A retype bumps cluster-wide Index.forward_encoding_version, but
//     each replica drives its own local rewrite at its own pace.
//   - Live filter validation always uses the *current* declared type
//     (the FSM-side schema flips synchronously), so post-retype queries
//     must use the new type. v_current keeps serving structurally
//     correct results during the rewrite via dual-write.
//   - The atomic switch (current ← pending) is a per-replica event;
//     queries on a node that hasn't switched yet return data from the
//     pre-retype keyspace. Eventually every replica catches up.
//
// We pin the eventual-consistency endpoint here: after the retype,
// every replica eventually serves the same entity under the new
// encoding. Strict mid-rewrite ordering needs a stronger primitive
// than min_log_sequence (which only gates on log application, not on
// rewrite completion); the existing helper polls
// `current_version > 0` and is correct only for the initial backfill
// case — left as a follow-up.
var _ = Describe("MetadataIndexPerReplicaConsistency", Ordered, func() {
	const (
		countInstances = 3
		ledgerName     = "per-replica-consistency"
		key            = "score"
	)

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

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("eventually surfaces the new-encoded entity on every replica after a retype", func() {
		// Create a ledger with a STRING-typed metadata field "score"
		// and an account index over it.
		_, err := servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: key, Type: commonpb.MetadataType_METADATA_TYPE_STRING},
			}),
		))
		Expect(err).To(Succeed())

		_, err = servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateAccountMetadataIndexAction(ledgerName, key),
		))
		Expect(err).To(Succeed())

		// Wait for the index to be ready (current_version > 0) on each
		// replica before issuing pre-retype queries.
		for i, s := range servers {
			Expect(actions.WaitForMetadataIndexReady(ctx, s.Client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)).
				To(Succeed(), fmt.Sprintf("node %d must report local index ready before any retype", i))
		}

		// Write an account with a string-typed score that's also a
		// valid uint64 after the retype.
		_, err = servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(100), "USD"),
			}, nil),
			actions.SaveAccountMetadataAction(ledgerName, "alice", map[string]string{key: "030"}),
		))
		Expect(err).To(Succeed())

		// Sanity: the STRING-typed query on every replica finds alice
		// (the lookup matches v_current's STRING encoding).
		for i, s := range servers {
			Eventually(func(g Gomega) {
				accounts, listErr := actions.ListAccountsFiltered(ctx, s.Client, ledgerName, 0, "", actions.StringMetadataFilter(key, "030"))
				g.Expect(listErr).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed(),
				fmt.Sprintf("node %d must surface alice under the STRING encoding", i))
		}

		// Retype to UINT64. This bumps Index.forward_encoding_version
		// cluster-wide and kicks a local rewrite on each replica.
		_, err = servers[0].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, commonpb.MetadataType_METADATA_TYPE_UINT64),
		))
		Expect(err).To(Succeed())

		// On every replica, the UINT64(30) query EVENTUALLY finds
		// alice — this is the per-replica catch-up window. We poll
		// with a long timeout because each replica needs to ingest
		// the retype log AND run its local rewrite + atomic switch
		// before its v_new keyspace serves the right encoding.
		for i, s := range servers {
			Eventually(func(g Gomega) {
				accounts, listErr := actions.ListAccountsFiltered(ctx, s.Client, ledgerName, 0, "", actions.UintMetadataFilter(key, 30))
				g.Expect(listErr).To(Succeed())
				g.Expect(accounts).To(HaveLen(1))
				g.Expect(accounts[0].Address).To(Equal("alice"))
			}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed(),
				fmt.Sprintf("node %d must eventually surface alice under the UINT64 encoding after the local rewrite", i))
		}
	})
})
