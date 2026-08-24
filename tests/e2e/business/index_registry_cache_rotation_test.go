//go:build e2e

package business

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The FSM reads the index registry from its in-memory cache, and a cache entry
// that goes untouched across two rotations is dropped from both generations —
// the entry survives in Pebble, so nothing was deleted and no log records a
// removal. Whatever a proposal needs after that must be preloaded back by
// admission; a path that declares coverage for a key without preloading its
// value reads the evicted entry as absent.
//
// removeFieldType is such a path. It probes the registry to report the index it
// cascade-drops, so an evicted entry makes it drop nothing and report nothing:
// the index outlives the field declaration it was attached to, keeps serving a
// keyspace nothing will purge, and every replica agrees because rotation rides
// the replicated Raft index.
var _ = Describe("Index registry survives cache rotation", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		rotationThreshold = uint64(10)
		// Enough no-op proposals to push the registry entry past gen1. Each
		// Barrier preloads nothing, so none of them refresh it.
		barrierCount = 50
		ledgerName   = "idx-registry-rotation"
		metaKey      = "k3"
	)

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode(
			testserver.WithCacheRotationThreshold(rotationThreshold),
		)
		client = node.Client

		// Declare the field and index it: the registry entry lands in Pebble
		// and in the current cache generation.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
				{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: metaKey, Type: commonpb.MetadataType_METADATA_TYPE_INT64},
			})))
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateAccountMetadataIndexAction(ledgerName, metaKey)))
		Expect(err).To(Succeed())
		Expect(actions.WaitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)).To(Succeed())

		// Age the entry out of both generations without touching it.
		for range barrierCount {
			_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
			Expect(err).To(Succeed())
		}
	})

	It("reports the dropped index after the entry is evicted from cache", func() {
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.RemoveMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)))
		Expect(err).To(Succeed())

		logs := resp.GetLogs()
		Expect(logs).To(HaveLen(1))

		removed := logs[0].GetPayload().GetApply().GetLog().GetData().GetRemovedMetadataFieldType()
		Expect(removed).NotTo(BeNil())

		want := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey))
		Expect(removed.GetDroppedIndex()).NotTo(BeNil(),
			"the index was created and never dropped, so the removal must cascade-drop it — "+
				"an empty dropped_index means the evicted registry entry read as absent, leaving "+
				"the index alive with its declaration gone")
		Expect(indexes.Canonical(removed.GetDroppedIndex())).To(Equal(want))
	})
})
