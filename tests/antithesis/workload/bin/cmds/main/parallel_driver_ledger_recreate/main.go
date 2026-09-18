// Verify the permanent deletion contract and isolation between ledger names.
// A deleted name cannot be recreated. A different driver-owned ledger must not
// expose its transactions/accounts and may reuse its references: reference
// uniqueness is scoped by ledger name. API absence does not prove disk cleanup.
//
// Only acknowledged predecessors enter the oracle. Every write has a distinct
// operation key, stable across client retries; tombstone probes must not replay
// the original creation's success. An unacknowledged delete is inconclusive.
// Unfiltered, fully paginated reads avoid undeclared reference/address indexes.
// The default linearizable read barrier orders them after acknowledged writes.
package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	antirandom "github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func createTx(ctx context.Context, client servicepb.BucketServiceClient, key, ledger, ref, destination string) (*servicepb.ApplyResponse, error) {
	return client.Apply(ctx, servicepb.UnsignedApplyRequest(key, &servicepb.Request{
		Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: ledger,
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
				CreateTransaction: &servicepb.CreateTransactionPayload{
					Postings:  []*commonpb.Posting{{Source: "world", Destination: destination, Amount: commonpb.NewUint256FromUint64(100), Asset: "USD/2"}},
					Reference: ref, Force: true,
				},
			}},
		}},
	}))
}

// One classification site for unexpected operation failures, with the stage in
// details. Expected tombstone/NotFound outcomes are checked separately below.
func operationFailed(err error, stage string, details internal.Details) bool {
	assert.Always(internal.IsTolerated(err), "ledger deletion scenario has no unexpected operation errors",
		details.With(internal.Details{"stage": stage, "error": err}))
	return err != nil
}

func confirmedTransaction(resp *servicepb.ApplyResponse, details internal.Details) *commonpb.CreatedTransaction {
	created := internal.CheckCreatedTransaction(resp, details)
	assert.Always(created != nil, "ledger deletion acknowledged transaction includes its created log", details)
	return created
}

func confirmTombstone(ctx context.Context, client servicepb.BucketServiceClient, ledger, key string, details internal.Details) bool {
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(key, actions.CreateLedgerAction(ledger, nil)))
	if err != nil && internal.IsTolerated(err) {
		return false
	}
	rejected := status.Code(err) == codes.FailedPrecondition && internal.IsLedgerDeleted(err)
	assert.Always(rejected, "deleted ledger name remains permanently reserved", details.With(internal.Details{"probeKey": key, "error": err}))
	return rejected
}

func main() {
	internal.RunDriver("parallel_driver_ledger_recreate", func(ctx context.Context, client servicepb.BucketServiceClient, _ string) {
		// Small acknowledged predecessor sets exercise multiple references/accounts.
		runScenario(ctx, client, internal.Rand().Uint64(), antirandom.RandomChoice([]int{3, 5}))
	})
}

func runScenario(ctx context.Context, client servicepb.BucketServiceClient, run uint64, txCount int) {
	ledger := internal.PrefixLedgerRecreate.WithSeed(run)
	other := internal.PrefixLedgerRecreate.WithSuffix(fmt.Sprintf("other-%016x", run))
	details := internal.Details{"ledger": ledger, "otherLedger": other}
	key := func(stage string) string { return fmt.Sprintf("lrec-%016x-%s", run, stage) }

	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(key("create"), actions.CreateLedgerAction(ledger, nil)))
	// The bounded original-name namespace can collide with a prior invocation.
	if internal.IsAlreadyExists(err) || internal.IsLedgerDeleted(err) {
		return
	}
	if operationFailed(err, "create predecessor ledger", details) {
		return
	}

	var refs, accounts []string
	var transactionIDs []uint64
	for i := range txCount {
		ref := fmt.Sprintf("lrec-%d-%d", run, i)
		account := fmt.Sprintf("lrec-old:%d:%d", run%1_000_000, i)
		resp, err := createTx(ctx, client, key(fmt.Sprintf("seed-%d", i)), ledger, ref, account)
		if operationFailed(err, "seed predecessor", details) {
			continue
		}
		created := confirmedTransaction(resp, details)
		if created == nil {
			return
		}
		transactionIDs = append(transactionIDs, created.GetTransaction().GetId())
		refs = append(refs, ref)
		accounts = append(accounts, account)
	}
	assert.Sometimes(len(refs) > 0, "ledger deletion predecessor write set recorded", details)
	if len(refs) == 0 {
		return
	}

	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(key("delete"), actions.DeleteLedgerAction(ledger)))
	if operationFailed(err, "delete predecessor ledger", details) {
		return
	}
	if !confirmTombstone(ctx, client, ledger, key("recreate-before"), details) {
		return
	}

	// Deleted reads expose NotFound without ErrorInfo, unlike FSM write refusal.
	_, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: ledger})
	if err != nil && internal.IsTolerated(err) {
		return
	}
	assert.Always(status.Code(err) == codes.NotFound, "deleted ledger is hidden from ledger reads", details.With(internal.Details{"error": err}))
	if status.Code(err) != codes.NotFound {
		return
	}
	for i, transactionID := range transactionIDs {
		_, err = client.GetTransaction(ctx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: transactionID})
		if err != nil && internal.IsTolerated(err) {
			return
		}
		assert.Always(status.Code(err) == codes.NotFound, "deleted ledger hides predecessor transaction point reads",
			details.With(internal.Details{"transactionID": transactionID, "error": err}))
		if status.Code(err) != codes.NotFound {
			return
		}
		_, err = client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledger, Address: accounts[i]})
		if err != nil && internal.IsTolerated(err) {
			return
		}
		assert.Always(status.Code(err) == codes.NotFound, "deleted ledger hides predecessor account point reads",
			details.With(internal.Details{"account": accounts[i], "error": err}))
		if status.Code(err) != codes.NotFound {
			return
		}
	}

	txStream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{Ledger: ledger})
	if err == nil {
		_, err = txStream.Recv()
	}
	if err != nil && internal.IsTolerated(err) {
		return
	}
	assert.Always(status.Code(err) == codes.NotFound, "deleted ledger exposes no predecessor transactions", details.With(internal.Details{"error": err}))
	if status.Code(err) != codes.NotFound {
		return
	}

	accountStream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger})
	if err == nil {
		_, err = accountStream.Recv()
	}
	if err != nil && internal.IsTolerated(err) {
		return
	}
	assert.Always(status.Code(err) == codes.NotFound, "deleted ledger exposes no predecessor accounts", details.With(internal.Details{"error": err}))
	if status.Code(err) != codes.NotFound {
		return
	}

	_, err = createTx(ctx, client, key("deleted-write"), ledger, "", "lrec-after-delete")
	if err != nil && internal.IsTolerated(err) {
		return
	}
	rejected := status.Code(err) == codes.FailedPrecondition && internal.HasErrorReason(err, domain.ErrReasonLedgerDeleted)
	assert.Always(rejected, "deleted ledger rejects new transaction writes", details.With(internal.Details{"error": err}))
	if !rejected {
		return
	}

	_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest(key("create-other"), actions.CreateLedgerAction(other, nil)))
	if operationFailed(err, "create other ledger", details) {
		return
	}
	// Best effort on every exit after confirmed creation. This removes live
	// projections when reachable; the permanent tombstone/audit still remain.
	defer func() {
		_, cleanupErr := client.Apply(ctx, servicepb.UnsignedApplyRequest(key("cleanup-other"), actions.DeleteLedgerAction(other)))
		internal.LogCleanupError("delete isolation ledger", cleanupErr)
	}()
	resp, err := createTx(ctx, client, key("marker"), other, "", "lrec-marker")
	if operationFailed(err, "write other ledger marker", details) {
		return
	}
	if confirmedTransaction(resp, details) == nil {
		return
	}

	// Run before reference reuse. Complete listings also inspect rows beyond the
	// first page and surface errors from stream creation and Recv.
	transactions, err := actions.ListAllTransactions(ctx, client, other)
	if operationFailed(err, "list other ledger transactions", details) {
		return
	}
	isolatedTransactions, isolatedAccounts := true, true
	for _, tx := range transactions {
		if slices.Contains(refs, tx.GetReference()) {
			isolatedTransactions = false
		}
		for _, posting := range tx.GetPostings() {
			if slices.Contains(accounts, posting.GetSource()) || slices.Contains(accounts, posting.GetDestination()) {
				isolatedAccounts = false
			}
		}
	}
	assert.Always(isolatedTransactions, "other ledger never exposes predecessor transactions", details)
	if !isolatedTransactions {
		return
	}

	otherAccounts, err := actions.ListAllAccounts(ctx, client, other)
	if operationFailed(err, "list other ledger accounts", details) {
		return
	}
	for _, account := range otherAccounts {
		if slices.Contains(accounts, account.GetAddress()) {
			isolatedAccounts = false
		}
	}
	assert.Always(isolatedAccounts, "other ledger never exposes predecessor account activity", details)
	if !isolatedAccounts {
		return
	}

	resp, err = createTx(ctx, client, key("reuse"), other, refs[0], "lrec-new:0")
	reuseDetails := details.With(internal.Details{"reference": refs[0], "error": err})
	if err != nil && internal.IsTolerated(err) {
		return
	}
	// This is a reach claim for actual acceptance, not error classification.
	// Inconclusive transport/lifecycle outcomes cannot satisfy or refute it.
	assert.Sometimes(err == nil, "predecessor reference accepted by another ledger", reuseDetails)
	assert.Always(err == nil, "predecessor references are reusable in another ledger", reuseDetails)
	if err != nil || confirmedTransaction(resp, reuseDetails) == nil {
		return
	}

	// The old name remains unavailable after successful activity in the new one.
	confirmTombstone(ctx, client, ledger, key("recreate-after"), details)
}
