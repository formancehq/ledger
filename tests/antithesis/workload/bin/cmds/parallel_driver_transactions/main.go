package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_transactions", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		switch random.RandomChoice([]uint8{0, 1}) {
		case 0:
			createRandomPostingsTransaction(ctx, client, ledger)
		case 1:
			createRandomNumscriptTransaction(ctx, client, ledger)
		}
	})
}

func createRandomPostingsTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:      internal.RandomPostings(),
							Metadata:      commonpb.MetadataFromGoMap(internal.RandomMetadata()),
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create a transaction", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	createdTx := internal.ExtractCreatedTransaction(resp)
	if createdTx == nil {
		return
	}

	checkReadAfterWrite(ctx, client, ledger, createdTx)
	internal.CheckPostCommitVolumes(createdTx.PostCommitVolumes, internal.Details{"ledger": ledger})
}

func createRandomNumscriptTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	vars := map[string]string{
		"from":   internal.GetRandomAddress(),
		"to":     internal.GetRandomAddress(),
		"amount": fmt.Sprintf("COIN %v", internal.RandomBigInt().String()),
	}

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Script: &commonpb.Script{
								Plain: `
									vars {
										account $from
										account $to
										monetary $amount
									}
									send $amount (
										source = $from allowing unbounded overdraft
										destination = $to
									)
								`,
								Vars: vars,
							},
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to create a transaction", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	createdTx := internal.ExtractCreatedTransaction(resp)
	if createdTx == nil {
		return
	}

	checkReadAfterWrite(ctx, client, ledger, createdTx)
	internal.CheckPostCommitVolumes(createdTx.PostCommitVolumes, internal.Details{"ledger": ledger})
}

func checkReadAfterWrite(ctx context.Context, client servicepb.BucketServiceClient, ledger string, createdTx *commonpb.CreatedTransaction) {
	_, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: createdTx.Transaction.Id,
	})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		st, _ := status.FromError(err)
		assert.AlwaysOrUnreachable(st.Code() != codes.NotFound, "should always be able to read committed transaction", internal.Details{
			"ledger": ledger,
			"txId":   createdTx.Transaction.Id,
			"error":  err,
		})
	}
}
