//go:build e2e

package cluster

import (
	"context"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// addEventsSinkAction creates a request to add a named sink configuration.
func addEventsSinkAction(config *ledgerpb.SinkConfig) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddEventsSink{
			AddEventsSink: &servicepb.AddEventsSinkRequest{
				Config: config,
			},
		},
	}
}

// listAllTransactions collects all transactions from the streaming RPC into a slice.
func listAllTransactions(ctx context.Context, client servicepb.BucketServiceClient, ledgerName string, pageSize uint32, afterTxID uint64, filters ...*ledgerpb.QueryFilter) ([]*ledgerpb.Transaction, error) {
	var filter *ledgerpb.QueryFilter
	if len(filters) > 0 {
		filter = filters[0]
	}

	return actions.ListTransactionsFiltered(ctx, client, ledgerName, pageSize, afterTxID, filter)
}
