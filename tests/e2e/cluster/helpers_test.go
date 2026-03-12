//go:build e2e

package cluster

import (
	"context"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// addEventsSinkAction creates a request to add a named sink configuration.
func addEventsSinkAction(config *commonpb.SinkConfig) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddEventsSink{
			AddEventsSink: &servicepb.AddEventsSinkRequest{
				Config: config,
			},
		},
	}
}

// listAllTransactions collects all transactions from the streaming RPC into a slice.
func listAllTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64, filters ...*commonpb.QueryFilter) ([]*commonpb.Transaction, error) {
	req := &servicepb.ListTransactionsRequest{
		Ledger:    ledgerName,
		PageSize:  pageSize,
		AfterTxId: afterTxID,
	}
	if len(filters) > 0 {
		req.Filter = filters[0]
	}
	stream, err := client.ListTransactions(ctx, req)
	if err != nil {
		return nil, err
	}

	var transactions []*commonpb.Transaction
	for {
		tx, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}
