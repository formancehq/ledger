//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// A checkpoint is a frozen, read-only snapshot, so any number of clients may
// read the same one at the same time. Every checkpoint-scoped read opens the
// checkpoint's directories for the duration of the request, and Pebble's
// directory lock is per-process: two opens of one directory from the same node
// collide even though both are read-only.
var _ = Describe("Query Checkpoints (concurrent reads of one checkpoint)", Ordered, func() {
	const (
		ledgerName = "qcp-concurrent"
		readers    = 8
	)

	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
		cpID   uint64
	)

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode()
		client = node.Client

		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", "alice", big.NewInt(1000), "USD"),
		}, nil)))
		Expect(err).To(Succeed())

		id, _, err := actions.CreateQueryCheckpoint(ctx, client)
		Expect(err).To(Succeed())
		cpID = id

		// Both halves materialize asynchronously; polling one read to success
		// takes materialization out of what the concurrent reads below measure.
		awaitCheckpointRead(func() (*commonpb.Account, error) {
			return client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:       ledgerName,
				Address:      "alice",
				CheckpointId: cpID,
			})
		})
	})

	It("serves concurrent reads of the same checkpoint", func() {
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			failed  []string
			release = make(chan struct{})
		)

		for range readers {
			wg.Add(1)

			go func() {
				defer GinkgoRecover()
				defer wg.Done()

				<-release

				_, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
					Ledger:       ledgerName,
					Address:      "alice",
					CheckpointId: cpID,
				})
				if err != nil {
					mu.Lock()
					failed = append(failed, fmt.Sprintf("%s: %s", status.Code(err), status.Convert(err).Message()))
					mu.Unlock()
				}
			}()
		}

		close(release)
		wg.Wait()

		Expect(failed).To(BeEmpty(), "reading one frozen checkpoint concurrently must not fail")
	})
})
