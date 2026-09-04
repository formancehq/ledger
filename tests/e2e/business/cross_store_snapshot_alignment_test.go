//go:build e2e

package business

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// Cross-store snapshot alignment (EN-1748): a transaction visible in the
// main store whose index rows have not folded yet must never surface through
// an index complement. not(ts[_,_]) over a READY TIMESTAMP index is the
// sharpest probe — any transaction it returns claims "committed but absent
// from the timestamp index", a state no aligned snapshot pair can produce.
//
// The fold is made to lag by throughput asymmetry, with no artificial hooks:
// many live indexes multiply the fold cost of every log while apply cost
// stays flat, so sustained apply pressure drives the read-store fold a
// measurable distance behind the primary head — the natural condition (load,
// faults, backfill storms) under which unaligned snapshots serve torn responses.
var _ = Describe("Cross-store snapshot alignment", Ordered, func() {
	// The apply pressure this spec generates would linger in the shared
	// server's store and tax every later spec that walks it (checker
	// sweeps are O(logs)), so it gets a server of its own.
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const ledgerName = "cross-store-alignment-ledger"

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
		Expect(actions.WaitForBuiltinIndexReady(ctx, client, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP)).To(Succeed())
	})

	It("never surfaces a committed tx as missing from a READY index", func() {
		// This is a correctness probe, not a throughput benchmark. A few complete
		// apply batches establish distinct primary and projection horizons while
		// remaining portable under race and coverage instrumentation.
		const minimumObservedLag = 32

		stop := make(chan struct{})

		var pressure sync.WaitGroup

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
						a := fmt.Sprintf("load:%d:%d:a", w, n%128)
						b := fmt.Sprintf("load:%d:%d:b", w, n%128)
						reqs = append(reqs, actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", a, big.NewInt(1), "COIN"),
							actions.NewPosting("world", b, big.NewInt(1), "EUR"),
							actions.NewPosting("world", a, big.NewInt(1), "USD/2"),
						}, nil, map[string]*commonpb.MetadataMap{
							a: {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("gold")}},
							b: {Values: map[string]*commonpb.MetadataValue{"tier": commonpb.NewStringValue("silver")}},
						}))
					}
					if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...)); err != nil {
						select {
						case <-stop:
							if ctx.Err() == nil {
								Fail(fmt.Sprintf("in-flight apply failed after pressure stopped: %v", err))
							}

							return
						default:
							Fail(fmt.Sprintf("apply pressure failed: %v", err))
						}
					}
				}
			}(w)
		}
		stopped := false
		stopPressure := func() {
			if !stopped {
				stopped = true

				close(stop)
			}
		}
		waitForPressure := func() {
			stopPressure()
			pressure.Wait()
		}
		defer waitForPressure()

		notTs := actions.NotFilter(actions.BuiltinUintRangeFilter(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, 0, ^uint64(0)))
		Eventually(func(g Gomega) uint64 {
			st, err := actions.GetIndexStatus(ctx, client)
			g.Expect(err).To(Succeed())

			return st.GetLag()
		}, 25*time.Second, 10*time.Millisecond).Should(BeNumerically(">=", minimumObservedLag), "pressure never made the fold lag — inconclusive")

		// Open the streaming RPC while pressure is still live so its request is sent
		// under the production read-under-lag condition. Stop producers as soon as
		// the request is on the wire: at most their four in-flight batches remain,
		// rather than another deadline-sized interval of unbounded submissions.
		probeCtx, cancelProbe := context.WithTimeout(ctx, 3*time.Minute)
		probe, err := client.ListTransactions(probeCtx, &servicepb.ListTransactionsRequest{
			Ledger: ledgerName,
			Options: &commonpb.ListOptions{
				Filter: notTs,
			},
		})
		Expect(err).To(Succeed())
		stopPressure()
		waitForPressure()

		var (
			probeTxs []*commonpb.Transaction
			probeErr error
		)
		for {
			tx, recvErr := probe.Recv()
			if errors.Is(recvErr, io.EOF) {
				break
			}
			if recvErr != nil {
				probeErr = recvErr

				break
			}
			probeTxs = append(probeTxs, tx)
		}
		cancelProbe()

		Expect(probeErr).To(Succeed(), "live-lag probe never produced a conclusive response")
		Expect(probeTxs).To(BeEmpty(), "live-lag probe surfaced a committed tx as missing from the READY timestamp index")

		// Bound the generated backlog, then prove the same read succeeds once the
		// fixed projection horizon can catch up.
		queryCtx, cancelQuery := context.WithTimeout(ctx, 3*time.Minute)
		defer cancelQuery()
		txs, err := actions.ListTransactionsFiltered(queryCtx, client, ledgerName, 0, 0, notTs)
		Expect(err).To(Succeed())
		if len(txs) > 0 {
			Fail(fmt.Sprintf("not(ts[_,_]) returned %d row(s), first id=%d — a committed tx surfaced as missing from the READY timestamp index", len(txs), txs[0].GetId()))
		}
	})
})
