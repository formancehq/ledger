//go:build e2e

package business

import (
	"fmt"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// MetadataIndexConversionDesync is a sanity check for an index desync that can
// happen when an indexed metadata field's values are overwritten during the
// BUILDING window of a type change.
//
// Under the current design only the indexer's schema-rewrite (BUILDING) runs
// in the background — the FSM-side value converter (CONVERTING) is gone, so
// stored values stay verbatim. The hazard the test still pins is the
// incremental-write path: a delete of an old indexed entry must locate it
// by the encoding currently in the forward index (looked up via the reverse
// map), not by the encoding implied by the prior declared type. Any miss
// would leave a stale entry surfacing the account for a value it no longer
// holds.
//
// Detection: after the type change every score is >= 1_000_000, so an index
// query for the old range [0, n) must return nothing. Any hit is a stale entry.
var _ = Describe("MetadataIndexConversionDesync", Ordered, func() {
	const (
		ledgerName = "idx-conv-desync"
		key        = "score"
		n          = 2000
		chunkSize  = 250
	)

	acct := func(i int) string { return fmt.Sprintf("user:%05d", i) }

	applyChunked := func(g Gomega, makeReq func(i int) *servicepb.Request) {
		for start := 0; start < n; start += chunkSize {
			var env []*servicepb.Request
			for i := start; i < start+chunkSize && i < n; i++ {
				env = append(env, makeReq(i))
			}

			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", env...))
			g.Expect(err).To(Succeed())
		}
	}

	It("does not leave stale index entries after a mid-BUILDING overwrite", func() {
		// Ledger with a STRING field + an index on it.
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: key, Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		})))
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateAccountMetadataIndexAction(ledgerName, key)))
		Expect(err).To(Succeed())
		Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName,
			commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)).To(Succeed())

		// Seed n accounts with score = str(i) (indexed in the string encoding).
		applyChunked(Default, func(i int) *servicepb.Request {
			return actions.SaveAccountMetadataAction(ledgerName, acct(i), map[string]string{key: fmt.Sprintf("%d", i)})
		})

		// Declare INT64: starts the index schema-rewrite (BUILDING) and the value
		// converter (CONVERTING).
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.SetMetadataFieldTypeAction(ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key,
			commonpb.MetadataType_METADATA_TYPE_INT64)))
		Expect(err).To(Succeed())

		// Immediately overwrite every account to a value >= 1_000_000, racing the
		// background rewrite that is now re-encoding the index entries.
		applyChunked(Default, func(i int) *servicepb.Request {
			return actions.SaveAccountMetadataAction(ledgerName, acct(i), map[string]string{key: fmt.Sprintf("%d", 1_000_000+i)})
		})

		// Let the conversion + index rewrite settle (index back to READY).
		Expect(actions.WaitForMetadataIndexReady(sharedCtx, sharedClient, ledgerName,
			commonpb.TargetType_TARGET_TYPE_ACCOUNT, key)).To(Succeed())

		// Sanity: the new values are indexed.
		hiLo, hiHi := int64(1_000_000), int64(1_000_000+n-1)
		Eventually(func(g Gomega) {
			found, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "",
				actions.Int64RangeMetadataFilter(key, &hiLo, &hiHi))
			g.Expect(err).To(Succeed())
			g.Expect(found).NotTo(BeEmpty(), "new values should be indexed")
		}).Within(20 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

		// Desync check: no account currently holds a score in [0, n), so the
		// index must return nothing for that range. Stale entries from the
		// mid-BUILDING overwrite show up here.
		lo, hi := int64(0), int64(n-1)
		Eventually(func(g Gomega) {
			stale, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "",
				actions.Int64RangeMetadataFilter(key, &lo, &hi))
			g.Expect(err).To(Succeed())

			var addresses []string
			for _, a := range stale {
				addresses = append(addresses, a.GetAddress())
			}

			g.Expect(stale).To(BeEmpty(), "stale index entries for overwritten old values: %v", addresses)
		}).Within(30 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
	})
})
