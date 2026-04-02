package main

import (
	"context"
	"fmt"
	"log"
	"maps"

	"github.com/alitto/pond"
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"go.etcd.io/etcd/client/v3/concurrency"
)

func main() {
	log.Println("composer: parallel_driver_transaction_metadata")

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

	lastTxID, err := internal.GetLastTransactionID(ctx, client, ledger)
	if err != nil || lastTxID < 0 {
		return
	}

	const count = 100
	pool := pond.New(10, 10e3)

	etcdClient := internal.NewEtcdClient()
	defer etcdClient.Close()

	for range count {
		pool.Submit(func() {
			session, err := concurrency.NewSession(etcdClient)
			if err != nil {
				return
			}
			defer session.Close() //nolint:errcheck
			setTransactionMetadata(ctx, client, session, ledger, uint64(lastTxID))
		})
	}

	pool.StopAndWait()
	log.Println("composer: parallel_driver_transaction_metadata: done")
}

func setTransactionMetadata(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	session *concurrency.Session,
	ledger string,
	lastTxID uint64,
) {
	txID := random.GetRandom() % (lastTxID + 1)
	mutex := concurrency.NewMutex(session, fmt.Sprintf("/ledger/%v/transaction/%v", ledger, txID))
	if err := mutex.Lock(ctx); err != nil {
		return
	}
	defer mutex.Unlock(ctx) //nolint:errcheck

	// Get current transaction
	preTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: txID,
	})
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to get entity before metadata change", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	// Set new metadata
	randomMetadata := internal.RandomMetadata()
	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Data: &servicepb.LedgerApplyRequest_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target: &commonpb.Target{
								Target: &commonpb.Target_Transaction{
									Transaction: &commonpb.TargetTransaction{Id: txID},
								},
							},
							Metadata: commonpb.MetadataSetFromMap(randomMetadata),
						},
					},
				},
			},
		}},
	})
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to set metadata", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	// Read and verify
	postTx, err := client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
		Ledger:        ledger,
		TransactionId: txID,
	})
	assert.Sometimes(err == nil || internal.IsUnavailable(err), "should be able to get entity after metadata change", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	preMetadata := commonpb.MetadataSetToMap(preTx.GetTransaction().GetMetadata())
	expectedMetadata := make(map[string]string)
	maps.Copy(expectedMetadata, preMetadata)
	maps.Copy(expectedMetadata, randomMetadata)

	postMetadata := commonpb.MetadataSetToMap(postTx.GetTransaction().GetMetadata())

	sessionAlive := true
	select {
	case <-session.Done():
		sessionAlive = false
	default:
	}

	if sessionAlive {
		assert.Always(maps.Equal(postMetadata, expectedMetadata), "new metadata should be correct", internal.Details{
			"ledger":   ledger,
			"txID":     txID,
			"original": preMetadata,
			"added":    randomMetadata,
			"actual":   postMetadata,
			"expected": expectedMetadata,
		})
	}

	for k, v := range expectedMetadata {
		assert.Always(postMetadata[k] == v, "metadata key should be present after set", internal.Details{
			"ledger": ledger,
			"txID":   txID,
			"key":    k,
			"want":   v,
			"got":    postMetadata[k],
		})
	}
}
