//go:build e2e

package business

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Cross-store value skew (EN-1748, metadata-flip half): a row selected by a
// metadata index filter must satisfy that filter in its own enriched
// metadata — no single-sequence snapshot can return an account for
// tier=gold whose metadata reads tier=silver.
//
// Selection reads the read-index while enrichment reads the main-store
// handle pinned at query start. The alignment wait only bounds the fold from
// BELOW (it must reach the handle's sequence): a tier flip folded past the
// handle would make plain index keys select an account whose enrichment
// still carries the pre-flip value. Metadata index leaves therefore resolve
// their event groups AT the handle's sequence (event_keys.go) — this test
// drives the fold into large catch-up batches (sustained apply pressure,
// many live indexes) so the index snapshot lands well past the handle on
// nearly every query, and asserts the flip can never leak through.
var _ = Describe("Cross-store value skew", Ordered, func() {
	// The apply pressure this spec generates would linger in the shared
	// server's store and tax every later spec that walks it (checker
	// sweeps are O(logs)), so it gets a server of its own.
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const ledgerName = "cross-store-value-skew-ledger"

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode()
		client = node.Client

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerWithSchemaAction(ledgerName, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT, Key: "tier", Type: commonpb.MetadataType_METADATA_TYPE_STRING},
		})))
		Expect(err).To(Succeed())

		// Fold cost per log is multiplicative in the live index count.
		for _, req := range []*servicepb.Request{
			actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
			actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
			actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS),
			actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS),
			actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS),
			actions.CreateAccountMetadataIndexAction(ledgerName, "tier"),
			actions.CreateAccountAssetIndexAction(ledgerName),
		} {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", req))
			Expect(err).To(Succeed())
		}
		Expect(actions.WaitForMetadataIndexReady(ctx, client, ledgerName, commonpb.TargetType_TARGET_TYPE_ACCOUNT, "tier")).To(Succeed())
	})

	It("never returns a tier=gold row whose own metadata disagrees", func() {
		stop := make(chan struct{})

		var pressure sync.WaitGroup

		flip := [2]string{"gold", "silver"}

		for w := 0; w < 4; w++ {
			pressure.Add(1)
			go func(w int) {
				defer GinkgoRecover()
				defer pressure.Done()

				n := 0
				for {
					select {
					case <-stop:
						return
					default:
					}

					reqs := make([]*servicepb.Request, 0, 20)
					for j := 0; j < 20; j++ {
						n++
						// Each touch of an account lands one round (128 txs)
						// after the previous one, so the round-parity value
						// inverts the account's tier on every touch.
						a := fmt.Sprintf("load:%d:%d:a", w, n%128)
						b := fmt.Sprintf("load:%d:%d:b", w, n%128)
						round := n / 128
						reqs = append(reqs, actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", a, big.NewInt(1), "COIN"),
							actions.NewPosting("world", b, big.NewInt(1), "EUR"),
							actions.NewPosting("world", a, big.NewInt(1), "USD/2"),
						}, nil, map[string]*commonpb.MetadataMap{
							a: {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue(flip[round%2])}},
							b: {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue(flip[(round+1)%2])}},
						}))
					}
					if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...)); err != nil {
						return
					}
				}
			}(w)
		}
		stopped := false
		stopPressure := func() {
			if !stopped {
				stopped = true

				close(stop)
				pressure.Wait()
			}
		}
		defer stopPressure()

		gold := actions.StringMetadataFilter("tier", "gold")
		deadline := time.Now().Add(25 * time.Second)
		probes, served := 0, 0

		for time.Now().Before(deadline) {
			probes++

			accs, err := actions.ListAccountsFiltered(ctx, client, ledgerName, 0, "", gold)
			// Alignment waits out a lagging fold rather than rejecting, so an
			// error here is a real failure, not a freshness condition.
			Expect(err).To(Succeed())

			served++

			for _, acc := range accs {
				if got := acc.GetMetadata()["tier"].GetStringValue(); got != "gold" {
					Fail(fmt.Sprintf("probe %d: tier=gold returned %s with tier=%q — index selection and enrichment disagree on one row", probes, acc.GetAddress(), got))
				}
			}
		}

		Expect(served).To(BeNumerically(">", 0), "every probe was rejected — inconclusive")

		// Drain the fold backlog before the spec ends: teardown stops the
		// server under a bounded budget, which a deep backlog can exceed on
		// slow runners.
		stopPressure()
		Eventually(func() uint64 {
			st, err := actions.GetIndexStatus(ctx, client)
			if err != nil {
				return ^uint64(0)
			}

			return st.GetLag()
		}, 3*time.Minute, 500*time.Millisecond).Should(BeZero(), "fold backlog drained")
	})
})
