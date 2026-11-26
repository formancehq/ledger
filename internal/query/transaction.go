package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadTransactionState reads the current state of a transaction from the attributes zone.
func ReadTransactionState(ctx context.Context, reader dal.PebbleGetter, attrs *attributes.Attribute[*commonpb.TransactionState], ledgerName string, txID uint64) (*commonpb.TransactionState, error) {
	_, span := queryTracer.Start(ctx, "query.read_tx_state",
		trace.WithAttributes(
			attribute.String("ledger", ledgerName),
			attribute.Int64("transaction_id", int64(txID)),
		))
	defer span.End()

	txKey := domain.TransactionKey{LedgerName: ledgerName, ID: txID}

	state, err := attrs.Get(reader, txKey.Bytes())
	if err != nil {
		return nil, fmt.Errorf("computing transaction state for tx %d: %w", txID, err)
	}

	return state, nil
}
