//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// A query checkpoint's frozen read index must hold every row indexed up to
// the checkpoint's sequence, on every replica: index-driven reads through the
// checkpoint (logs, filtered transactions) equal the live pre-checkpoint pages
// and agree across nodes. The history written here stays far below the read
// index's memtable flush threshold, so the frozen index contains only what
// the builder materializes explicitly — nothing has reached an SST on its own.
var _ = Describe("Query Checkpoints (frozen read index completeness)", Ordered, func() {
	const (
		countInstances  = 3
		ledgerName      = "qcp-readindex"
		txCount         = 40
		filteredAccount = "acc-3"
	)

	var (
		ctx      context.Context
		servers  []*testutil.ServiceWithClient
		leaderID *uint64
		cpID     uint64

		wantLogSeqs []uint64
		wantTxIDs   []uint64
	)

	destinationIs := func(address string) *commonpb.QueryFilter {
		return actions.AddressExactRoleFilter(address, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION)
	}

	BeforeAll(func() {
		ctx, servers, _, leaderID = testutil.SetupMultiNodeCluster(countInstances)
		leader := servers[*leaderID-1].Client

		_, err := leader.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		// The account→transaction index is opt-in; declaring it before any
		// posting keeps the builder on its inline path (no backfill).
		_, err = leader.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateBuiltinTxIndexAction(ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)))
		Expect(err).To(Succeed())
		Expect(actions.WaitForBuiltinIndexReady(ctx, leader, ledgerName, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)).To(Succeed())

		for i := range txCount {
			_, err := leader.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", fmt.Sprintf("acc-%d", i%8), big.NewInt(int64(i+1)), "USD"),
			}, nil)))
			Expect(err).To(Succeed())
		}

		// Live reads on the leader align the read index to its main store, so
		// these pages are the complete pre-checkpoint state.
		logs, err := listLogs(ctx, leader, ledgerName, nil)
		Expect(err).To(Succeed())
		wantLogSeqs = logSequences(logs)
		Expect(len(wantLogSeqs)).To(BeNumerically(">=", txCount), "one log per transaction")
		GinkgoWriter.Printf("pre-checkpoint ledger logs: %d\n", len(wantLogSeqs))

		txs, err := actions.ListTransactionsFiltered(ctx, leader, ledgerName, 1000, 0, destinationIs(filteredAccount))
		Expect(err).To(Succeed())
		wantTxIDs = transactionIDs(txs)
		Expect(wantTxIDs).To(HaveLen(txCount / 8))
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	It("creates the checkpoint", func() {
		id, _, err := actions.CreateQueryCheckpoint(ctx, servers[*leaderID-1].Client)
		Expect(err).To(Succeed())
		cpID = id
	})

	It("serves every pre-checkpoint log through the checkpoint on every replica", func() {
		for i, node := range servers {
			logs := awaitCheckpointRead(func() ([]*commonpb.Log, error) {
				return listLogs(ctx, node.Client, ledgerName, &commonpb.ReadOptions{CheckpointId: cpID})
			})
			Expect(logSequences(logs)).To(Equal(wantLogSeqs),
				"node %d: the frozen read index must hold every log committed before the checkpoint", i+1)
		}
	})

	It("serves the pre-checkpoint filtered transactions through the checkpoint on every replica", func() {
		for i, node := range servers {
			txs := awaitCheckpointRead(func() ([]*commonpb.Transaction, error) {
				return listAllTransactionsFromCheckpoint(ctx, node.Client, ledgerName, 1000, 0, cpID, destinationIs(filteredAccount))
			})
			Expect(transactionIDs(txs)).To(Equal(wantTxIDs),
				"node %d: the frozen account→transaction index must resolve every pre-checkpoint posting", i+1)
		}
	})

	It("keeps serving the same pages after a post-checkpoint write", func() {
		_, err := servers[*leaderID-1].Client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
			actions.NewPosting("world", filteredAccount, big.NewInt(1), "USD"),
		}, nil)))
		Expect(err).To(Succeed())

		for i, node := range servers {
			logs, err := listLogs(ctx, node.Client, ledgerName, &commonpb.ReadOptions{CheckpointId: cpID})
			Expect(err).To(Succeed())
			Expect(logSequences(logs)).To(Equal(wantLogSeqs), "node %d", i+1)

			txs, err := listAllTransactionsFromCheckpoint(ctx, node.Client, ledgerName, 1000, 0, cpID, destinationIs(filteredAccount))
			Expect(err).To(Succeed())
			Expect(transactionIDs(txs)).To(Equal(wantTxIDs), "node %d", i+1)
		}
	})
})

// awaitCheckpointRead retries read while this replica has not materialized
// the checkpoint yet (typed retryable Unavailable); any other error fails.
// The first served page is returned as-is: a served checkpoint is complete
// by contract, so callers assert on it directly instead of polling until it
// happens to match.
func awaitCheckpointRead[T any](read func() (T, error)) T {
	var out T

	Eventually(func(g Gomega) {
		v, err := read()
		if err != nil {
			if status.Code(err) != codes.Unavailable {
				StopTrying("pre-ready checkpoint read must be retryable Unavailable, never anything else").Wrap(err).Now()
			}

			g.Expect(err).NotTo(HaveOccurred()) // keep polling
			return
		}

		out = v
	}, 30*time.Second, 200*time.Millisecond).Should(Succeed())

	return out
}

// listLogs drains one ledger log page (live when read is nil).
func listLogs(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, read *commonpb.ReadOptions) ([]*commonpb.Log, error) {
	stream, err := client.ListLogs(ctx, &servicepb.ListLogsRequest{
		Ledger:  ledgerName,
		Options: &commonpb.ListOptions{PageSize: 1000, Read: read},
	})
	if err != nil {
		return nil, err
	}

	var logs []*commonpb.Log

	for {
		log, err := stream.Recv()
		if err == io.EOF {
			return logs, nil
		}

		if err != nil {
			return nil, err
		}

		logs = append(logs, log)
	}
}

func logSequences(logs []*commonpb.Log) []uint64 {
	seqs := make([]uint64, 0, len(logs))
	for _, l := range logs {
		seqs = append(seqs, l.GetSequence())
	}

	return seqs
}

func transactionIDs(txs []*commonpb.Transaction) []uint64 {
	ids := make([]uint64, 0, len(txs))
	for _, tx := range txs {
		ids = append(ids, tx.GetId())
	}

	return ids
}
