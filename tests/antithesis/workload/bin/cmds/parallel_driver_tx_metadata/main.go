package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_tx_metadata", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		key := fmt.Sprintf("tx-meta-%d", r.Uint64())
		value := fmt.Sprintf("val-%d", r.Uint64())

		// 1. Create a transaction to attach metadata to.
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: internal.RandomPostings(),
							Force:    true,
						},
					}},
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create tx for metadata test", internal.Details{"ledger": ledger, "error": err})
		if err != nil {
			return
		}

		createdTx := internal.ExtractCreatedTransaction(resp)
		if createdTx == nil {
			return
		}

		txID := createdTx.Transaction.Id
		details := internal.Details{"ledger": ledger, "txId": txID, "key": key}

		// 2. Save metadata on the transaction.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_TransactionId{TransactionId: txID},
							},
							Metadata: commonpb.MetadataFromGoMap(map[string]string{key: value}),
						},
					}},
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to save transaction metadata", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// 3. Read-after-write: verify the key is present on the transaction.
		getTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
			Ledger:        ledger,
			TransactionId: txID,
		})
		if err != nil {
			internal.LogCleanupError("read transaction after metadata write", err)
			return
		}

		assert.AlwaysOrUnreachable(
			findTxMetadata(getTx.GetTransaction(), key) == value,
			"tx metadata read-after-write should return saved value",
			details.With(internal.Details{"expected": value, "actual": findTxMetadata(getTx.GetTransaction(), key)}),
		)

		// 4. Delete the metadata key.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &commonpb.DeleteMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_TransactionId{TransactionId: txID},
							},
							Key: key,
						},
					}},
				},
			},
		}))

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to delete transaction metadata", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// 5. Read-after-delete: verify the key is gone.
		getTx, err = client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
			Ledger:        ledger,
			TransactionId: txID,
		})
		if err != nil {
			internal.LogCleanupError("read transaction after metadata delete", err)
			return
		}

		assert.AlwaysOrUnreachable(
			findTxMetadata(getTx.GetTransaction(), key) == "",
			"deleted tx metadata key should be absent",
			details,
		)
	})
}

func findTxMetadata(tx *commonpb.Transaction, key string) string {
	if v, ok := tx.GetMetadata()[key]; ok {
		return v.GetStringValue()
	}

	return ""
}
