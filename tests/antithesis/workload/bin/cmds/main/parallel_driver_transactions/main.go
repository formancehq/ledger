package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	internal.RunGeneratedDriver("parallel_driver_transactions", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
		switch generation.Choice(src, []uint8{0, 1}) {
		case 0:
			createRandomTransaction(ctx, client, ledger, src)
		case 1:
			createRandomBulkTransactions(ctx, client, ledger, src)
		}
	})
}

func randomPostingsRequest(ledger string, src *generation.Source) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: internal.GeneratedPostings(src),
						Metadata: commonpb.MetadataFromGoMap(internal.GeneratedMetadata(src)),
						Force:    true,
					},
				}},
			},
		},
	}
}

func randomNumscriptRequest(ledger string, src *generation.Source) *servicepb.Request {
	vars := map[string]string{
		"from":   internal.GeneratedAddress(src),
		"to":     internal.GeneratedAddress(src),
		"amount": fmt.Sprintf("COIN %v", internal.GeneratedBigInt(src).String()),
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{
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
						Force: true,
					},
				}},
			},
		},
	}
}

func randomTransactionRequest(ledger string, src *generation.Source) *servicepb.Request {
	if generation.Choice(src, []uint8{0, 1}) == 0 {
		return randomPostingsRequest(ledger, src)
	}

	return randomNumscriptRequest(ledger, src)
}

func createRandomTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", randomTransactionRequest(ledger, src)))

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

func createRandomBulkTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
	size := internal.GeneratedBulkSize(src, 0.001, 1, 5000)
	requests := make([]*servicepb.Request, size)
	for i := range size {
		requests[i] = randomTransactionRequest(ledger, src)
	}

	resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", requests...))

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
	i := src.Intn(len(resp.GetLogs()))
	createdTx := internal.CreatedTransactionFromLog(resp.GetLogs()[i])
	if createdTx == nil {
		return
	}
	checkReadAfterWrite(ctx, client, ledger, createdTx)
	internal.CheckPostCommitVolumes(createdTx.GetTransaction().GetPostCommitVolumes(), internal.Details{"ledger": ledger})
}

func checkReadAfterWrite(ctx context.Context, client servicepb.BucketServiceClient, ledger string, createdTx *commonpb.CreatedTransaction) {
	_, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
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
