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
	log.Println("composer: parallel_driver_account_metadata")

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

	accounts, err := internal.ListAccounts(ctx, client, ledger)
	if err != nil || len(accounts) == 0 {
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
			setAccountMetadata(ctx, client, session, ledger, accounts)
		})
	}

	pool.StopAndWait()
	log.Println("composer: parallel_driver_account_metadata: done")
}

func setAccountMetadata(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	session *concurrency.Session,
	ledger string,
	accounts []string,
) {
	account := accounts[int(random.GetRandom()%uint64(len(accounts)))]
	mutex := concurrency.NewMutex(session, fmt.Sprintf("/ledger/%v/account/%v", ledger, account))
	if err := mutex.Lock(ctx); err != nil {
		return
	}
	defer mutex.Unlock(ctx) //nolint:errcheck

	// Get current metadata
	preAcc, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: account,
	})
	assert.Sometimes(err == nil, "should be able to get entity before metadata change", internal.Details{
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
								Target: &commonpb.Target_Account{
									Account: &commonpb.TargetAccount{Addr: account},
								},
							},
							Metadata: commonpb.MetadataSetFromMap(randomMetadata),
						},
					},
				},
			},
		}},
	})
	assert.Sometimes(err == nil, "should be able to set metadata", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	// Read and verify metadata
	postAcc, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: account,
	})
	assert.Sometimes(err == nil, "should be able to get entity after metadata change", internal.Details{
		"ledger": ledger,
		"error":  err,
	})
	if err != nil {
		return
	}

	// Verify metadata was merged correctly
	preMetadata := commonpb.MetadataSetToMap(preAcc.GetMetadata())
	expectedMetadata := make(map[string]string)
	maps.Copy(expectedMetadata, preMetadata)
	maps.Copy(expectedMetadata, randomMetadata)

	postMetadata := commonpb.MetadataSetToMap(postAcc.GetMetadata())

	assert.Always(maps.Equal(postMetadata, expectedMetadata), "new metadata should be correct", internal.Details{
		"ledger":   ledger,
		"account":  account,
		"original": preMetadata,
		"added":    randomMetadata,
		"actual":   postMetadata,
		"expected": expectedMetadata,
	})
}
