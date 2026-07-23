//go:build e2e

package business

import (
	"context"
	"math/big"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// A committed idempotency outcome references the committed log by its sequence;
// a replay resolves that reference back to the log. Once the key's chapter is
// archived, the log is purged from hot storage, so without a cold-storage
// fallback the reference resolves to nothing and the replay returns an empty log
// (sequence 0) instead of the committed outcome — an exactly-once violation. The
// key itself is not purged (it lives outside the cold zone), so the replay is
// still served; only the log resolution regresses. This pins that replay
// resolution falls back to cold storage.
var _ = Describe("Idempotency replay resolves archived logs from cold storage", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = 15704
		grpcPort = 15804
		ledger   = "idem-replay-cold"
		idemKey  = "replay-cold-key"
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort,
			testserver.WithColdStorageDriver("filesystem"),
		)
	})

	keyedTx := func() *servicepb.ApplyRequest {
		return actions.WithIdempotencyKey(idemKey,
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
			}, nil, nil),
		)
	}

	It("replays the original committed log after its chapter is archived", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		// First apply commits; capture the committed log's sequence.
		first, err := client.Apply(ctx, keyedTx())
		Expect(err).To(Succeed())
		Expect(first.GetLogs()).To(HaveLen(1))
		originalSeq := first.GetLogs()[0].GetSequence()
		Expect(originalSeq).To(BeNumerically(">", 0))

		// Archive the chapter holding the transaction, purging its log from hot
		// storage into cold.
		archiveChapterFull(ctx, client)

		// Replay the same key + body. The idempotency cache returns a reference to
		// the original log sequence; resolving it must fall back to cold storage
		// and return the original committed log — not an empty (sequence 0) log.
		replay, err := client.Apply(ctx, keyedTx())
		Expect(err).To(Succeed(), "replay of an archived key must succeed")
		Expect(replay.GetLogs()).To(HaveLen(1))
		Expect(replay.GetLogs()[0].GetSequence()).To(Equal(originalSeq),
			"replay must resolve the original committed log from cold storage, not an empty log")

		// The resolved log is the real transaction, not a zero value.
		tx := replay.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
		Expect(tx).ToNot(BeNil(), "replayed log must carry the committed transaction")
		Expect(tx.GetPostings()).To(HaveLen(1))
		Expect(tx.GetPostings()[0].GetDestination()).To(Equal("users:alice"))
	})
})
