//go:build e2e

package cluster

import (
	"context"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/protobuf/proto"
)

// EN-2070: exercise the real CLI, Raft admission, mirror worker and each local
// indexer with source history available before the creation request is sent.
var _ = Describe("LedgerctlInitialIndexes", Ordered, func() {
	var ctx context.Context
	var servers []*testutil.ServiceWithClient
	BeforeAll(func() { ctx, servers, _, _ = testutil.SetupMultiNodeCluster(3) })
	AfterAll(func() { testutil.StopServers(ctx, servers) })

	It("commits every initial index before ingestion and replays creation idempotently", func() {
		source := newMockV2Server()
		DeferCleanup(source.Close)
		source.addLog(newV2TransactionLogWithMetadata(1, 0, "world", "alice", "100", "USD", map[string]string{"external:id": "source-0"}))
		const ledger = "atomic-mirror"
		args := []string{"ledgers", "create", "--name", ledger, "--mode", "mirror", "--mirror-base-url", source.URL(),
			"--schema", "transaction:external:id:string", "--index", "reference", "--index", "account-asset",
			"--index", "metadata:transaction:external:id", "--idempotency-key", "atomic-mirror-create"}
		_, err := runCLI(servers[0].GRPCPort, args...)
		Expect(err).To(Succeed())

		for _, server := range servers {
			Eventually(func(g Gomega) {
				txs, err := actions.ListTransactionsFiltered(ctx, server.Client, ledger, 0, 0, actions.StringMetadataFilter("external:id", "source-0"))
				g.Expect(err).To(Succeed())
				g.Expect(txs).To(HaveLen(1))
				status, err := server.Client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
				g.Expect(err).To(Succeed())
				g.Expect(status.GetIndexes()).To(HaveLen(3))
				for _, entry := range status.GetIndexes() {
					g.Expect(entry.GetCurrentVersion()).To(Equal(uint32(1)))
					g.Expect(entry.GetPendingVersion()).To(BeZero())
					g.Expect(entry.GetCursor()).To(BeZero())
				}
			}).Within(20 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())
		}

		entries, err := actions.ListAuditEntries(ctx, servers[0].Client, false)
		Expect(err).To(Succeed())
		var creationEnd uint64
		var ingestionStart uint64
		for _, entry := range entries {
			if entry.GetSuccess() == nil {
				continue
			}
			detail, err := actions.GetAuditEntry(ctx, servers[0].Client, entry.GetSequence())
			Expect(err).To(Succeed())
			for _, item := range detail.GetItems() {
				order := new(raftcmdpb.Order)
				Expect(proto.Unmarshal(item.GetSerializedOrder(), order)).To(Succeed())
				scoped := order.GetLedgerScoped()
				if scoped.GetCreateLedger() != nil {
					Expect(detail.GetItems()).To(HaveLen(4), "creation and all three indexes must share one audited proposal")
					Expect(item.GetOrderIndex()).To(BeZero())
					for _, indexItem := range detail.GetItems()[1:] {
						indexOrder := new(raftcmdpb.Order)
						Expect(proto.Unmarshal(indexItem.GetSerializedOrder(), indexOrder)).To(Succeed())
						Expect(indexOrder.GetLedgerScoped().GetApply().GetCreateIndex()).NotTo(BeNil())
					}
					creationEnd = entry.GetSuccess().GetMaxLogSequence()
				}
				if scoped.GetMirrorIngest() != nil && (ingestionStart == 0 || item.GetLogSequence() < ingestionStart) {
					ingestionStart = item.GetLogSequence()
				}
			}
		}
		Expect(creationEnd).NotTo(BeZero())
		Expect(ingestionStart).To(BeNumerically(">", creationEnd))

		// Repeating the operator's exact command must return the initial success,
		// even though the ledger and all indexes now exist and ingestion has run.
		_, err = runCLI(servers[1].GRPCPort, args...)
		Expect(err).To(Succeed())
	})

	It("rolls back ledger creation when an initial index fails server validation", func() {
		const ledger = "invalid-initial-index"
		_, err := runCLI(servers[0].GRPCPort, "ledgers", "create", "--name", ledger,
			"--index", "reference", "--index", "metadata:transaction:undeclared")
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("schema"))
		_, err = servers[0].Client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
		Expect(err).To(HaveOccurred())
		// Success with the same name proves that the preceding valid index and
		// CreateLedger were discarded with the invalid declaration.
		_, err = runCLI(servers[0].GRPCPort, "ledgers", "create", "--name", ledger, "--index", "reference")
		Expect(err).To(Succeed())
	})
})
