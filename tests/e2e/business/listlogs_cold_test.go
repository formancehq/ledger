//go:build e2e

package business

import (
	"context"
	"io"
	"math/big"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// ListLogs pages over sequences from the read-side log index, which still lists
// a log after its chapter is archived and purged from hot storage — so the
// per-log fetch must fall back to cold storage. Without that, listing a page
// that includes an archived log fails the whole call ("log with sequence N not
// found in Pebble"). This pins the cold fallback in the ListLogs cursor.
var _ = Describe("ListLogs reads archived logs from cold storage", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)
	const (
		httpPort = 15706
		grpcPort = 15806
		ledger   = "listlogs-cold"
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort,
			testserver.WithColdStorageDriver("filesystem"),
		)
	})

	It("lists all logs after their chapter is archived", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		for range 3 {
			_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", "acc:1", big.NewInt(10), "USD"),
				}, nil)))
			Expect(err).To(Succeed())
		}

		archiveChapterFull(ctx, client)

		// The read-side index is eventually consistent, so retry until all three
		// logs are listable — each must now resolve from cold storage since the
		// chapter is archived. Asserting inside Eventually makes it actually wait
		// for the indexer rather than pass on the first attempt.
		Eventually(func(g Gomega) {
			stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{Ledger: ledger})
			g.Expect(err).To(Succeed(), "ListLogs must not error on archived logs")

			count := 0
			for {
				_, re := stream.Recv()
				if re == io.EOF {
					break
				}
				g.Expect(re).To(Succeed(), "streaming archived logs must not error")
				count++
			}
			g.Expect(count).To(Equal(3), "expected 3 logs after archiving 3")
		}).Should(Succeed())
	})
})
