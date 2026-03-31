package main

import (
	"context"
	"fmt"
	"log"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	log.Println("composer: parallel_driver_transactions")

	ctx := context.Background()
	client, conn, err := internal.NewClient()
	if err != nil {
		log.Printf("error creating client: %s", err)
		return
	}
	defer conn.Close()

	ledger, err := internal.GetRandomLedger(ctx, client)
	if err != nil {
		return
	}

	const count = 100
	pool := pond.New(10, 10e3)

	for range count {
		pool.Submit(func() {
			createTransaction(ctx, client, ledger)
		})
	}

	pool.StopAndWait()
	log.Println("composer: parallel_driver_transactions: done")
}

func createTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	switch random.RandomChoice([]uint8{0, 1}) {
	case 0:
		createRandomPostingsTransaction(ctx, client, ledger)
	case 1:
		createRandomNumscriptTransaction(ctx, client, ledger)
	}
}

// createRandomPostingsTransaction submits a transaction with random postings and checks post-commit volumes.
func createRandomPostingsTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	postings := internal.RandomPostings()
	metadata := internal.RandomMetadata()

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:      postings,
							Metadata:      commonpb.MetadataSetFromMap(metadata),
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		}},
	})

	details := internal.Details{
		"ledger": ledger,
		"error":  err,
	}

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create a transaction", details)
	if err != nil {
		return
	}

	// Extract the created transaction
	createdTx := extractCreatedTransaction(resp)
	if createdTx == nil {
		return
	}

	// Check that we can read it immediately
	_, err = client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: createdTx.Transaction.Id,
	})
	if err != nil {
		st, _ := status.FromError(err)
		assert.AlwaysOrUnreachable(st.Code() != codes.NotFound, "should always be able to read committed transaction", internal.Details{
			"ledger": ledger,
			"txId":   createdTx.Transaction.Id,
		})
	}

	// Check post-commit volumes
	internal.CheckPostCommitVolumes(createdTx.PostCommitVolumes, internal.Details{
		"ledger": ledger,
	})
}

// createRandomNumscriptTransaction submits a numscript transaction.
func createRandomNumscriptTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	script := `
		vars {
			account $from
			account $to
			monetary $amount
		}
		send $amount (
			source = $from allowing unbounded overdraft
			destination = $to
		)
	`
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
								Plain: script,
								Vars:  vars,
							},
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		}},
	})

	details := internal.Details{
		"ledger": ledger,
		"error":  err,
	}

	assert.Sometimes(err == nil, "should be able to create a transaction", details)
	if err != nil {
		return
	}

	createdTx := extractCreatedTransaction(resp)
	if createdTx == nil {
		return
	}

	// Read-after-write check
	_, err = client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: createdTx.Transaction.Id,
	})
	if err != nil {
		st, _ := status.FromError(err)
		assert.AlwaysOrUnreachable(st.Code() != codes.NotFound, "should always be able to read committed transaction", internal.Details{
			"ledger": ledger,
			"txId":   createdTx.Transaction.Id,
		})
	}

	internal.CheckPostCommitVolumes(createdTx.PostCommitVolumes, internal.Details{
		"ledger": ledger,
	})
}

// extractCreatedTransaction extracts the CreatedTransaction from an Apply response.
func extractCreatedTransaction(resp *servicepb.ApplyResponse) *commonpb.CreatedTransaction {
	if resp == nil || len(resp.Logs) == 0 {
		return nil
	}
	applyLog := resp.Logs[0].Payload.GetApply()
	if applyLog == nil {
		return nil
	}
	return applyLog.Log.Data.GetCreatedTransaction()
}

// successOrInsufficientFunds returns true if err is nil or indicates insufficient funds.
func successOrInsufficientFunds(err error) bool {
	if err == nil {
		return true
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	// gRPC uses FailedPrecondition for insufficient funds
	return st.Code() == codes.FailedPrecondition
}
