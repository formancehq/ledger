//go:build e2e

package business

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// An index lives in the bucket-scoped registry until something drops it, and
// removeFieldType reports the index it cascade-dropped on its log. The model
// checker catches that report coming back empty for indexes it watched get
// created and never dropped: the entry is gone from the registry while the
// indexbuilder still holds it, so the index outlives the field declaration it
// was attached to and keeps serving a keyspace nothing will purge.
//
// Each case drives the registry the way the observed traffic did — retypes that
// rewrite the entry, index lifecycle on a neighbouring ledger in the same
// bucket, and both inside one multi-ledger batch — and asserts the removal
// still names the index it dropped.
var _ = Describe("Index registry survives schema churn", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const metaKey = "k3"

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode()
		client = node.Client
	})

	apply := func(reqs ...*servicepb.Request) *servicepb.ApplyResponse {
		GinkgoHelper()

		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		Expect(err).To(Succeed())

		return resp
	}

	// indexedLedger creates a ledger declaring metaKey on accounts and indexes
	// it, returning once the index is READY on every replica.
	indexedLedger := func(ledger string) {
		GinkgoHelper()

		apply(actions.CreateLedgerWithSchemaAction(ledger, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: metaKey, Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		}))

		apply(actions.CreateAccountMetadataIndexAction(ledger, metaKey))
		Expect(actions.WaitForMetadataIndexReady(ctx, client, ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)).To(Succeed())
	}

	// expectRemovalDropsIndex removes the field declaration and requires the log
	// to name the index that went with it.
	expectRemovalDropsIndex := func(ledger, label string) {
		GinkgoHelper()

		resp := apply(actions.RemoveMetadataFieldTypeAction(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey))

		logs := resp.GetLogs()
		Expect(logs).To(HaveLen(1), "%s: the removal commits exactly one log", label)

		removed := logs[0].GetPayload().GetApply().GetLog().GetData().GetRemovedMetadataFieldType()
		Expect(removed).NotTo(BeNil(), "%s: the log must be a field removal", label)

		want := indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey))
		Expect(removed.GetDroppedIndex()).NotTo(BeNil(),
			"%s: the index was created and never dropped, so the removal must cascade-drop it — "+
				"an empty dropped_index leaves the index outliving its declaration", label)
		Expect(indexes.Canonical(removed.GetDroppedIndex())).To(Equal(want), "%s: dropped the wrong index", label)
	}

	It("drops the index after the field type is rewritten repeatedly", func() {
		const ledger = "idx-registry-retype"

		indexedLedger(ledger)

		// Every retype rewrites the registry entry (the bump is what drives
		// each replica's rewrite), so this is the path that keeps touching it.
		types := []commonpb.MetadataType{
			commonpb.MetadataType_METADATA_TYPE_INT32,
			commonpb.MetadataType_METADATA_TYPE_UINT64,
			commonpb.MetadataType_METADATA_TYPE_INT8,
			commonpb.MetadataType_METADATA_TYPE_INT64,
		}
		for range 6 {
			for _, t := range types {
				apply(actions.SetMetadataFieldTypeAction(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey, t))
			}
		}

		expectRemovalDropsIndex(ledger, "after retype churn")
	})

	It("drops the index while a neighbouring ledger churns its own indexes", func() {
		const (
			ledger    = "idx-registry-neighbour"
			neighbour = "idx-registry-neighbour-other"
		)

		indexedLedger(ledger)
		indexedLedger(neighbour)

		// The registry is bucket-scoped: the neighbour's lifecycle must not
		// reach this ledger's entry.
		for range 4 {
			apply(actions.RemoveMetadataFieldTypeAction(neighbour, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey))
			apply(actions.SetMetadataFieldTypeAction(neighbour, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey, commonpb.MetadataType_METADATA_TYPE_INT64))
			apply(actions.CreateAccountMetadataIndexAction(neighbour, metaKey))
		}

		expectRemovalDropsIndex(ledger, "beside a churning neighbour")
	})

	It("drops the index when the batch also touches another ledger's registry", func() {
		const (
			ledger    = "idx-registry-batched"
			neighbour = "idx-registry-batched-other"
		)

		indexedLedger(ledger)
		indexedLedger(neighbour)

		// One batch spanning both ledgers, mixing a retype of the target field
		// with registry writes for the neighbour — the shape the failing
		// sequences committed in.
		apply(
			actions.SetMetadataFieldTypeAction(neighbour, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey, commonpb.MetadataType_METADATA_TYPE_INT32),
			actions.SetMetadataFieldTypeAction(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey, commonpb.MetadataType_METADATA_TYPE_INT32),
			actions.RemoveMetadataFieldTypeAction(neighbour, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey),
		)

		expectRemovalDropsIndex(ledger, "after a shared batch")
	})
})
