//go:build e2e

package business

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Removing a metadata field type cascade-drops the index attached to it (the
// RemovedMetadataFieldType log carries the dropped IndexID). A query whose
// filter needs that index must then be rejected — re-declaring the field
// restores the schema, not the index, and the read path has no scan fallback.
// Serving results against a dropped index reports a partial keyspace as a
// complete answer.
var _ = Describe("Query after metadata field removal", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const metaKey = "k2"

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode()
		client = node.Client
	})

	apply := func(reqs ...*servicepb.Request) {
		GinkgoHelper()

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		Expect(err).To(Succeed())
	}

	// setupDroppedIndex walks the lifecycle: declare the field, index it, wait
	// for READY, then remove the field type (cascade-dropping the index) and
	// re-declare it. The field is declared and the index is gone.
	setupDroppedIndex := func(ledger string) {
		GinkgoHelper()

		apply(actions.CreateLedgerWithSchemaAction(ledger, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: metaKey, Type: commonpb.MetadataType_METADATA_TYPE_INT64},
		}))

		apply(actions.CreateTransactionAction(ledger, []*commonpb.Posting{
			actions.NewPosting("world", "acct:1", big.NewInt(1), "USD"),
		}, nil, map[string]*commonpb.MetadataMap{
			"acct:1": {Values: map[string]*commonpb.MetadataValue{metaKey: commonpb.NewIntValue(42)}},
		}))

		apply(actions.CreateAccountMetadataIndexAction(ledger, metaKey))
		Expect(actions.WaitForMetadataIndexReady(ctx, client, ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey)).To(Succeed())

		// Cascade-drops the index.
		apply(actions.RemoveMetadataFieldTypeAction(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey))

		// Restores the schema declaration — but not the index.
		apply(actions.SetMetadataFieldTypeAction(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, metaKey, commonpb.MetadataType_METADATA_TYPE_INT64))
	}

	expectIndexRejection := func(ledger string, filter *commonpb.QueryFilter, label string) {
		GinkgoHelper()

		_, err := actions.ListAccountsFiltered(ctx, client, ledger, 0, "", filter)
		Expect(err).To(HaveOccurred(), "%s: the index is gone, so the query must be rejected, not served", label)

		st, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(st.Code()).To(Equal(codes.FailedPrecondition), "%s: expected the index-not-found precondition", label)
	}

	It("rejects a bare field filter whose index was cascade-dropped", func() {
		const ledger = "removed-field-bare"

		setupDroppedIndex(ledger)
		expectIndexRejection(ledger, actions.ExistsMetadataFilter(metaKey), "bare exists")
	})

	It("rejects a disjunction whose branches both need the dropped index", func() {
		const ledger = "removed-field-or"

		setupDroppedIndex(ledger)

		// The shape the model driver generated: one branch reads the field
		// directly, the other reads it under a NOT beside an address leaf.
		filter := actions.OrFilter(
			actions.ExistsMetadataFilter(metaKey),
			actions.AndFilter(
				actions.AddressExactFilter("acct:1"),
				actions.NotFilter(actions.ExistsMetadataFilter(metaKey)),
			),
		)

		expectIndexRejection(ledger, filter, "or(field, and(addr, not(field)))")
	})
})
