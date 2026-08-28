//go:build e2e

package business

import (
	"context"
	"math/big"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// The WAL snapshot directory can vanish under a running node (operator
// mistake, cleanup job). Persisting the next snapshot must recreate it and
// carry on; failing the save instead propagates as a task-pool panic that
// takes the node down.
var _ = Describe("WAL snapshot directory recovery", Ordered, func() {
	var (
		ctx     context.Context
		node    *testutil.ServiceWithClient
		client  servicepb.BucketServiceClient
		snapDir string
	)

	const ledger = "wal-snapdir"

	BeforeAll(func() {
		ctx, node = testutil.SetupSingleNode()
		client = node.Client
		snapDir = filepath.Join(node.WalDir, "snap")
	})

	apply := func(g Gomega) {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "acc:1", big.NewInt(10), "USD"),
			}, nil)))
		g.Expect(err).To(Succeed())
	}

	It("survives the snapshot directory vanishing", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) { apply(g) }).Should(Succeed())

		Expect(os.RemoveAll(snapDir)).To(Succeed(), "simulate the directory vanishing under the node")

		// Background maintenance persists WAL snapshots as entries accumulate.
		// With the directory gone, every save must recreate it and succeed;
		// the node keeps accepting writes throughout.
		Eventually(func(g Gomega) {
			apply(g)

			entries, err := os.ReadDir(snapDir)
			g.Expect(err).To(Succeed(), "the snapshot directory must be recreated by the next save")
			g.Expect(entries).ToNot(BeEmpty(), "a snapshot file must be persisted into the recreated directory")
		}).WithTimeout(30 * time.Second).Should(Succeed())
	})
})
