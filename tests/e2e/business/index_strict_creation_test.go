//go:build e2e

package business

import (
	"context"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var _ = Describe("Strict index creation", func() {
	It("accepts only one concurrent fresh create and audits duplicates without rebuilding a ready index", func() {
		ctx, node := testutil.SetupSingleNode()
		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		client := node.Client
		const ledger = "strict-index-create"
		const builtin = commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		type outcome struct {
			key string
			err error
		}
		start := make(chan struct{})
		results := make(chan outcome, 2)
		for _, key := range []string{"strict-create-a", "strict-create-b"} {
			go func() {
				<-start
				_, applyErr := client.Apply(ctx, servicepb.UnsignedApplyRequest(key,
					actions.CreateBuiltinTxIndexAction(ledger, builtin)))
				results <- outcome{key: key, err: applyErr}
			}()
		}
		close(start)
		var rejectedKeys []string
		for range 2 {
			var result outcome
			Eventually(results).Should(Receive(&result))
			if result.err != nil {
				Expect(status.Code(result.err)).To(Equal(codes.AlreadyExists))
				info := actions.ExtractGRPCErrorInfo(result.err)
				Expect(info).NotTo(BeNil())
				Expect(info.Reason).To(Equal("INDEX_ALREADY_EXISTS"))
				rejectedKeys = append(rejectedKeys, result.key)
			}
		}
		Expect(rejectedKeys).To(HaveLen(1), "exactly one fresh request must win")
		Expect(actions.WaitForBuiltinIndexReady(ctx, client, ledger, builtin)).To(Succeed())

		before, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
		Expect(err).To(Succeed())
		Expect(before.GetIndexes()).To(HaveLen(1))
		Expect(before.GetIndexes()[0].GetCurrentVersion()).To(Equal(uint32(1)))
		Expect(before.GetIndexes()[0].GetPendingVersion()).To(BeZero())
		const readyDuplicateKey = "strict-create-ready-duplicate"
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(readyDuplicateKey,
			actions.CreateBuiltinTxIndexAction(ledger, builtin)))
		Expect(status.Code(err)).To(Equal(codes.AlreadyExists))
		Expect(actions.ExtractGRPCErrorInfo(err)).NotTo(BeNil())
		Expect(actions.ExtractGRPCErrorInfo(err).Reason).To(Equal("INDEX_ALREADY_EXISTS"))
		rejectedKeys = append(rejectedKeys, readyDuplicateKey)

		// Wait until all committed logs have reached the builder before
		// asserting that no creation allocated another local version.
		var after *servicepb.GetIndexStatusResponse
		Eventually(func(g Gomega) {
			after, err = client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
			g.Expect(err).To(Succeed())
			g.Expect(after.GetLag()).To(BeZero())
		}).Should(Succeed())
		Expect(after.GetIndexes()).To(HaveLen(1))
		Expect(proto.Equal(after.GetIndexes()[0], before.GetIndexes()[0])).To(BeTrue(),
			"duplicate creation must preserve the full registry row and local build state")

		entries, err := actions.ListAuditEntries(ctx, client, false)
		Expect(err).To(Succeed())
		for _, key := range rejectedKeys {
			entry := auditEntryWithIdempotency(entries, key)
			Expect(entry.GetFailure()).NotTo(BeNil())
			Expect(entry.GetFailure().GetReason()).To(Equal(commonpb.ErrorReason_ERROR_REASON_INDEX_ALREADY_EXISTS))
			full, err := client.GetAuditEntry(ctx, &servicepb.GetAuditEntryRequest{Sequence: entry.GetSequence()})
			Expect(err).To(Succeed())
			Expect(full.GetItems()).To(HaveLen(1))
			Expect(full.GetItems()[0].GetLogSequence()).To(BeZero(), "failure must not emit a created or skipped log")
			Expect(auditOrder(full.GetItems()[0]).GetLedgerScoped().GetApply().GetCreateIndex()).NotTo(BeNil())
		}
		logs, err := actions.ListAllLogs(ctx, client, ledger)
		Expect(err).To(Succeed())
		Expect(logs).To(HaveLen(1), "only the winning creation may emit a ledger apply log")
		Expect(logs[0].GetPayload().GetApply().GetLog().GetData().GetCreateIndex()).NotTo(BeNil())
		check, err := actions.CollectCheckStoreEvents(ctx, client)
		Expect(err).To(Succeed())
		Expect(check.Errors).To(BeEmpty())
	})
})
