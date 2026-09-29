package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_transactions", func(ctx context.Context, client commonpb.BucketServiceClient, ledger string) {
		switch random.RandomChoice([]uint8{0, 1}) {
		case 0:
			createRandomTransaction(ctx, client, ledger)
		case 1:
			createRandomBulkTransactions(ctx, client, ledger)
		}
	})
}

func randomPostingsRequest(ledger string) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
						Postings: internal.RandomPostings(),
						Metadata: commonpb.MetadataFromGoMap(internal.RandomMetadata()),
						Force:    true,
					},
				}},
			},
		},
	}
}

func randomNumscriptRequest(ledger string) *commonpb.Request {
	vars := map[string]string{
		"from":   internal.GetRandomAddress(),
		"to":     internal.GetRandomAddress(),
		"amount": fmt.Sprintf("COIN %v", internal.RandomBigInt().String()),
	}

	return &commonpb.Request{
		Type: &commonpb.Request_Apply{
			Apply: &commonpb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_CreateTransaction{
					CreateTransaction: &commonpb.CreateTransactionPayload{
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
						Force: true,
					},
				}},
			},
		},
	}
}

func randomTransactionRequest(ledger string) *commonpb.Request {
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		return randomPostingsRequest(ledger)
	}

	return randomNumscriptRequest(ledger)
}

func createRandomTransaction(ctx context.Context, client commonpb.BucketServiceClient, ledger string) {
	resp, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", randomTransactionRequest(ledger)))

	assert.Sometimes(internal.IsTolerated(err), "should be able to create a transaction", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	createdTx := internal.CheckCreatedTransaction(resp, internal.Details{"ledger": ledger})
	if createdTx == nil {
		return
	}

	checkReadAfterWrite(ctx, client, ledger, createdTx)
}

func createRandomBulkTransactions(ctx context.Context, client commonpb.BucketServiceClient, ledger string) {
	size := internal.GeometricBulkSize(0.001, 1, 5000)
	requests := make([]*commonpb.Request, size)
	for i := range size {
		requests[i] = randomTransactionRequest(ledger)
	}

	resp, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", requests...))

	assert.Sometimes(internal.IsTolerated(err), "should be able to create bulk transactions", internal.Details{
		"ledger": ledger,
		"size":   size,
		"error":  err,
	})
	if err != nil {
		return
	}

	assert.AlwaysOrUnreachable(uint64(len(resp.GetLogs())) == size, "bulk Apply should return one log per request", internal.Details{
		"ledger":   ledger,
		"expected": size,
		"got":      len(resp.GetLogs()),
	})

	if len(resp.GetLogs()) == 0 {
		return
	}

	// Verify read-after-write for a random entry in the bulk.
	i := int(internal.Rand().Uint64()>>1) % len(resp.GetLogs())
	createdTx := internal.CreatedTransactionFromLog(resp.GetLogs()[i])
	if createdTx == nil {
		return
	}
	checkReadAfterWrite(ctx, client, ledger, createdTx)
	internal.CheckPostCommitVolumes(createdTx.GetTransaction().GetPostCommitVolumes(), internal.Details{"ledger": ledger})
}

func checkReadAfterWrite(ctx context.Context, client commonpb.BucketServiceClient, ledger string, createdTx *commonpb.CreatedTransaction) {
	_, err := client.GetTransaction(ctx, &commonpb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: createdTx.GetTransaction().GetId(),
	})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		st, _ := status.FromError(err)
		assert.AlwaysOrUnreachable(st.Code() != codes.NotFound, "should always be able to read committed transaction", internal.Details{
			"ledger": ledger,
			"txId":   createdTx.GetTransaction().GetId(),
			"error":  err,
		})
	}
}
