//go:build e2e

package cluster

import (
	"context"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// addEventsSinkAction creates a request to add a named sink configuration.
func addEventsSinkAction(config *ledgerpb.SinkConfig) *ledgerpb.Request {
	return &ledgerpb.Request{
		Type: &ledgerpb.Request_AddEventsSink{
			AddEventsSink: &ledgerpb.AddEventsSinkRequest{
				Config: config,
			},
		},
	}
}

// listAllTransactions collects all transactions from the streaming RPC into a slice.
func listAllTransactions(ctx context.Context, client ledgerpb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64, filters ...*ledgerpb.QueryFilter) ([]*ledgerpb.Transaction, error) {
	var filter *ledgerpb.QueryFilter
	if len(filters) > 0 {
		filter = filters[0]
	}

	return actions.ListTransactionsFiltered(ctx, client, ledgerName, pageSize, afterTxID, filter)
}
