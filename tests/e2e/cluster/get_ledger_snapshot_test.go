//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// GetLedger and ListLedgers both return a LedgerInfo assembled from two reads —
// the account types and the ledger metadata — which must reflect the same
// committed state. This spec couples the two: every committed bulk atomically
// toggles one account type AND writes meta["count"] to the resulting type count.
// Any LedgerInfo whose len(AccountTypes) != meta["count"] observed a torn read
// straddling a single atomic bulk.
//
// A torn read is a timing race, so this hammers many parallel readers — split
// across both read paths — against a fast writer and fails on the first
// inconsistency.
var _ = Describe("GetLedger snapshot consistency", Ordered, func() {
	const (
		httpPort   = testutil.TestSingleHTTPPort
		grpcPort   = testutil.TestSingleGRPCPort
		ledgerName = "get-ledger-snapshot"
		toggleType = "toggle"

		getReaders  = 8
		listReaders = 4
		// The race trips in well under a second when present, so a bounded number
		// of toggles gives ample coverage while keeping write volume small. The
		// deadline is only a backstop against a stuck cluster.
		writerToggles = 3000
		backstop      = 60 * time.Second
	)

	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort)

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		// Initial committed state: collapsed — zero account types, count=0.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.SaveLedgerMetadataAction(ledgerName, map[string]string{"count": "0"})))
		Expect(err).To(Succeed())
	})

	It("never returns account types and metadata from different committed states", func() {
		runCtx, cancel := context.WithTimeout(ctx, backstop)
		defer cancel()

		var (
			wg         sync.WaitGroup
			torn       atomic.Pointer[string]
			bulkSeen   atomic.Uint64
			writerDone = make(chan struct{})
		)

		// Writer: alternate the toggle type on/off, each step in one atomic bulk
		// that also writes the matching count. expanded → 1 type, count=1;
		// collapsed → 0 types, count=0.
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer GinkgoRecover()
			defer close(writerDone)

			expanded := false
			for i := 0; i < writerToggles && runCtx.Err() == nil; i++ {
				var toggle *servicepb.Request
				var count string
				if expanded {
					toggle = actions.RemoveAccountTypeAction(ledgerName, toggleType)
					count = "0"
				} else {
					toggle = actions.AddAccountTypeAction(ledgerName, toggleType, "toggle:{id}")
					count = "1"
				}

				_, err := client.Apply(runCtx, servicepb.UnsignedApplyRequest("",
					toggle,
					actions.SaveLedgerMetadataAction(ledgerName, map[string]string{"count": count})))
				if err != nil {
					if runCtx.Err() != nil {
						return
					}
					// A rejected toggle would desync the local expanded/count
					// tracking — fail loudly rather than poison the invariant.
					torn.CompareAndSwap(nil, ptr(fmt.Sprintf("writer Apply failed: %v", err)))
					cancel()
					return
				}

				expanded = !expanded
				bulkSeen.Add(1)
			}
		}()

		// checkInfo enforces the coupled invariant: a LedgerInfo's account-type
		// count must equal its own meta["count"]. Returns false (skip) when the
		// info is absent or carries no count yet.
		checkInfo := func(label string, info *commonpb.LedgerInfo) {
			if info == nil {
				return
			}
			countVal, ok := info.GetMetadata()["count"]
			if !ok {
				return
			}
			count, err := strconv.Atoi(countVal.GetStringValue())
			if err != nil {
				return
			}
			if got := len(info.GetAccountTypes()); got != count {
				torn.CompareAndSwap(nil, ptr(fmt.Sprintf(
					"torn %s: len(AccountTypes)=%d but meta[count]=%d", label, got, count)))
				cancel()
			}
		}

		// runReader spins one read function until the writer is done, a tear is
		// found, or the backstop fires.
		runReader := func(read func() *commonpb.LedgerInfo, label string) {
			defer wg.Done()
			defer GinkgoRecover()

			for {
				select {
				case <-writerDone:
					return
				case <-runCtx.Done():
					return
				default:
				}

				info := read()
				if torn.Load() != nil {
					return
				}
				checkInfo(label, info)
			}
		}

		// GetLedger readers — the path the fix targets directly.
		for range getReaders {
			wg.Add(1)
			go runReader(func() *commonpb.LedgerInfo {
				info, err := client.GetLedger(runCtx, &servicepb.GetLedgerRequest{Ledger: ledgerName})
				if err != nil {
					return nil
				}

				return info
			}, "GetLedger")
		}

		// ListLedgers readers — the sibling path that enriches each ledger's
		// metadata alongside its account types, sharing GetLedger's coupling.
		for range listReaders {
			wg.Add(1)
			go runReader(func() *commonpb.LedgerInfo {
				ledgers, err := actions.ListLedgers(runCtx, client)
				if err != nil {
					return nil
				}

				return ledgers[ledgerName]
			}, "ListLedgers")
		}

		wg.Wait()

		if d := torn.Load(); d != nil {
			Fail(*d)
		}
		Expect(bulkSeen.Load()).To(BeNumerically(">", 0), "writer never committed a bulk")
	})
})

func ptr(s string) *string { return &s }
