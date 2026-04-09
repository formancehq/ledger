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
	switch random.RandomChoice([]uint8{0, 0, 1, 1, 2, 3, 4, 5, 6}) {
	case 0:
		createRandomPostingsTransaction(ctx, client, ledger)
	case 1:
		createRandomNumscriptTransaction(ctx, client, ledger)
	case 2:
		saveAndReadAccountMetadata(ctx, client, ledger)
	case 3:
		createAndRevertTransaction(ctx, client, ledger)
	case 4:
		createIdempotentTransaction(ctx, client, ledger)
	case 5:
		saveAndUseNumscript(ctx, client, ledger)
	case 6:
		createAndExecutePreparedQuery(ctx, client, ledger)
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

	// Check that we can read it immediately (linearizable read-after-write).
	// The read index barrier should ensure this always succeeds.
	_, err = client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: createdTx.Transaction.Id,
	})
	if err != nil {
		st, _ := status.FromError(err)
		assert.AlwaysOrUnreachable(st.Code() != codes.NotFound, "should always be able to read committed transaction", internal.Details{
			"ledger": ledger,
			"txId":   createdTx.Transaction.Id,
			"error":  err,
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

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create a transaction", details)
	if err != nil {
		return
	}

	createdTx := extractCreatedTransaction(resp)
	if createdTx == nil {
		return
	}

	// Read-after-write check (linearizable read-after-write).
	// The read index barrier should ensure this always succeeds.
	_, err = client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: createdTx.Transaction.Id,
	})
	if err != nil {
		st, _ := status.FromError(err)
		assert.AlwaysOrUnreachable(st.Code() != codes.NotFound, "should always be able to read committed transaction", internal.Details{
			"ledger": ledger,
			"txId":   createdTx.Transaction.Id,
			"error":  err,
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

// saveAndReadAccountMetadata saves random metadata on a random account, then reads it back
// to verify linearizable read-after-write consistency for metadata.
func saveAndReadAccountMetadata(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	address := internal.GetRandomAddress()
	key := fmt.Sprintf("meta-%d", random.GetRandom()%100)
	value := fmt.Sprintf("val-%d", random.GetRandom()%1000)

	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: address},
								},
							},
							Metadata: commonpb.MetadataSetFromMap(map[string]string{key: value}),
						},
					},
				},
			},
		}},
	})

	details := internal.Details{
		"ledger":  ledger,
		"account": address,
		"key":     key,
		"error":   err,
	}

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to save account metadata", details)
	if err != nil {
		return
	}

	// Read-after-write: the metadata should be visible immediately.
	acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: address,
	})
	if err != nil {
		return
	}

	found := false
	for _, m := range acct.GetMetadata().GetMetadata() {
		if m.GetKey() == key {
			found = true
			actual := m.GetValue().GetStringValue()
			assert.AlwaysOrUnreachable(actual == value, "metadata read-after-write should return saved value", details.With(internal.Details{
				"expected": value,
				"actual":   actual,
			}))

			break
		}
	}

	assert.AlwaysOrUnreachable(found, "saved metadata key should be present in account", details)
}

// createAndRevertTransaction creates a forced transaction and immediately reverts it,
// verifying that both operations succeed and post-commit volumes are consistent.
func createAndRevertTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	postings := internal.RandomPostings()

	resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:      postings,
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create a transaction for revert", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	createdTx := extractCreatedTransaction(resp)
	if createdTx == nil {
		return
	}

	txID := createdTx.Transaction.Id

	// Revert the transaction.
	revertResp, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_RevertTransaction{
						RevertTransaction: &servicepb.RevertTransactionPayload{
							TransactionId: txID,
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
		"txId":   txID,
		"error":  err,
	}

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to revert a transaction", details)
	if err != nil {
		return
	}

	// Verify the revert transaction has consistent post-commit volumes.
	if revertResp != nil && len(revertResp.Logs) > 0 {
		applyLog := revertResp.Logs[0].Payload.GetApply()
		if applyLog != nil {
			if revertedTx := applyLog.Log.Data.GetRevertedTransaction(); revertedTx != nil {
				internal.CheckPostCommitVolumes(revertedTx.PostCommitVolumes, details)
			}
		}
	}

	// Verify the original transaction is now marked as reverted.
	getTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: txID,
	})
	if err != nil {
		return
	}

	assert.AlwaysOrUnreachable(getTx.GetTransaction().GetRevertedBy() > 0,
		"reverted transaction should have revertedBy set", details)
}

// createIdempotentTransaction submits a transaction with an idempotency key twice,
// verifying that both calls succeed and return the same transaction ID.
func createIdempotentTransaction(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	postings := internal.RandomPostings()
	idemKey := fmt.Sprintf("idem-%d", random.GetRandom())

	req := &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			IdempotencyKey: idemKey,
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings:      postings,
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		}},
	}

	details := internal.Details{
		"ledger":         ledger,
		"idempotencyKey": idemKey,
	}

	// First call.
	resp1, err := client.Apply(ctx, req)
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create idempotent transaction (first)", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	tx1 := extractCreatedTransaction(resp1)
	if tx1 == nil {
		return
	}

	// Second call with same idempotency key — should return the same transaction.
	resp2, err := client.Apply(ctx, req)
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to create idempotent transaction (second)", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	tx2 := extractCreatedTransaction(resp2)
	if tx2 == nil {
		return
	}

	assert.Always(tx1.Transaction.Id == tx2.Transaction.Id,
		"idempotent transactions should return the same ID", details.With(internal.Details{
			"firstTxId":  tx1.Transaction.Id,
			"secondTxId": tx2.Transaction.Id,
		}))
}

// saveAndUseNumscript saves a versioned numscript and then creates a transaction using it.
// Tests that the script is readable after save (read-after-write) and usable in transactions.
func saveAndUseNumscript(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	scriptName := fmt.Sprintf("transfer-%d", random.GetRandom()%50)
	version := fmt.Sprintf("%d.0.0", random.GetRandom()%10+1)
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

	details := internal.Details{
		"ledger":     ledger,
		"scriptName": scriptName,
		"version":    version,
	}

	// Save the numscript.
	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_SaveNumscript{
				SaveNumscript: &servicepb.SaveNumscriptRequest{
					Name:    scriptName,
					Content: script,
					Version: version,
					Ledger:  ledger,
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to save numscript", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// Read-after-write: verify the script is readable.
	info, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
		Name:   scriptName,
		Ledger: ledger,
	})
	if err != nil {
		return
	}

	assert.AlwaysOrUnreachable(info.GetName() == scriptName, "saved numscript should be readable", details)

	// Use the script in a transaction via script reference.
	vars := map[string]string{
		"from":   internal.GetRandomAddress(),
		"to":     internal.GetRandomAddress(),
		"amount": fmt.Sprintf("COIN %v", internal.RandomBigInt().String()),
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Script: &commonpb.Script{
								Reference: &commonpb.ScriptReference{
									Name: scriptName,
								},
								Vars: vars,
							},
							Force: true,
						},
					},
				},
			},
		}},
	})

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to use saved numscript in transaction", details.With(internal.Details{"error": err}))
}

// createAndExecutePreparedQuery creates a prepared query filtering accounts by prefix,
// executes it, then deletes it. Tests the full prepared query lifecycle under chaos.
func createAndExecutePreparedQuery(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
	queryName := fmt.Sprintf("q-%d", random.GetRandom()%100)

	details := internal.Details{
		"ledger":    ledger,
		"queryName": queryName,
	}

	// Create a prepared query that filters accounts by address prefix "users:".
	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
					Query: &commonpb.PreparedQuery{
						Name:   queryName,
						Ledger: ledger,
						Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
						Filter: &commonpb.QueryFilter{
							Filter: &commonpb.QueryFilter_Address{
								Address: "users:",
							},
						},
					},
				},
			},
		}},
	})

	if err != nil {
		if internal.IsUnavailable(err) {
			return
		}
		// AlreadyExists is expected if another goroutine created the same query.
		st, _ := status.FromError(err)
		if st.Code() != codes.AlreadyExists {
			assert.Unreachable("prepared query creation returned unexpected error", details.With(internal.Details{"error": err}))
		}
	}

	// Execute the query.
	execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
		Ledger:    ledger,
		QueryName: queryName,
		PageSize:  10,
	})

	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to execute prepared query", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	// Verify the response has a result.
	assert.AlwaysOrUnreachable(execResp != nil, "prepared query should return a response", details)

	// Delete the query.
	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
					Ledger: ledger,
					Name:   queryName,
				},
			},
		}},
	})

	// Deletion may fail if another goroutine already deleted it.
	if err != nil && !internal.IsUnavailable(err) {
		st, _ := status.FromError(err)
		if st.Code() != codes.NotFound {
			assert.Unreachable("prepared query deletion returned unexpected error", details.With(internal.Details{"error": err}))
		}
	}
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
