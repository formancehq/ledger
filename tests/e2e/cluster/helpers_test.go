//go:build e2e

package cluster

import (
	"context"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// addEventsSinkAction creates a request to add a named sink configuration.
func addEventsSinkAction(config *commonpb.SinkConfig) *commonpb.Request {
	return &commonpb.Request{
		Type: &commonpb.Request_AddEventsSink{
			AddEventsSink: &commonpb.AddEventsSinkRequest{
				Config: config,
			},
		},
	}
}

// listAllTransactions collects all transactions from the streaming RPC into a slice.
func listAllTransactions(ctx context.Context, client commonpb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64, filters ...*commonpb.QueryFilter) ([]*commonpb.Transaction, error) {
	var filter *commonpb.QueryFilter
	if len(filters) > 0 {
		filter = filters[0]
	}

	return actions.ListTransactionsFiltered(ctx, client, ledgerName, pageSize, afterTxID, filter)
}
