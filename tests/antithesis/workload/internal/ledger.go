package internal

import (
	"context"
	"io"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// CreateLedger creates a ledger via the Apply RPC and verifies it can be read back.
func CreateLedger(ctx context.Context, client servicepb.BucketServiceClient, name string) error {
	details := Details{"ledger": name}

	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_CreateLedger{
				CreateLedger: &servicepb.CreateLedgerRequest{Name: name},
			},
		}},
	})
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to create ledger", details.With(Details{"error": err}))
	if err != nil {
		return err
	}

	// Verify it's readable
	_, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
	assert.Sometimes(err == nil || IsUnavailable(err), "should always be able to get created ledger", details.With(Details{"error": err}))
	return nil
}

// ListLedgers returns the names of all ledgers.
func ListLedgers(ctx context.Context, client servicepb.BucketServiceClient) ([]string, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}
	var names []string
	for {
		ledger, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		names = append(names, ledger.Name)
	}
	return names, nil
}

// GetRandomLedger returns a random ledger name from the existing ledgers.
func GetRandomLedger(ctx context.Context, client servicepb.BucketServiceClient) (string, error) {
	ledgers, err := ListLedgers(ctx, client)
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to get a random ledger", Details{"error": err})
	if err != nil {
		return "", err
	}
	if len(ledgers) == 0 {
		return "", io.EOF
	}
	return ledgers[random.GetRandom()%uint64(len(ledgers))], nil
}

// ListAccounts returns all account addresses for a given ledger.
func ListAccounts(ctx context.Context, client servicepb.BucketServiceClient, ledger string) ([]string, error) {
	stream, err := client.ListAccounts(ctx, &servicepb.ListAccountsRequest{Ledger: ledger})
	if err != nil {
		return nil, err
	}
	var addresses []string
	for {
		account, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		addresses = append(addresses, account.Address)
	}
	return addresses, nil
}

// ListTransactions returns all transactions for a ledger.
func ListTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledger string, pageSize uint32) ([]*commonpb.Transaction, error) {
	stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{
		Ledger:   ledger,
		PageSize: pageSize,
	})
	if err != nil {
		return nil, err
	}
	var txs []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		txs = append(txs, tx)
	}
	return txs, nil
}

// GetLastTransactionID returns the ID of the most recent transaction in a ledger, or -1 if none.
func GetLastTransactionID(ctx context.Context, client servicepb.BucketServiceClient, ledger string) (int64, error) {
	txs, err := ListTransactions(ctx, client, ledger, 1)
	assert.Sometimes(err == nil || IsUnavailable(err), "should be able to get the latest transaction", Details{"ledger": ledger})
	if err != nil {
		return -1, err
	}
	if len(txs) == 0 {
		return -1, nil
	}
	return int64(txs[0].Id), nil
}

