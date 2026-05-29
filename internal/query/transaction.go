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
func ReadTransactionState(ctx context.Context, reader dal.PebbleReader, attrs *attributes.Attribute[*commonpb.TransactionState], ledgerID uint32, txID uint64) (*commonpb.TransactionState, error) {
	_, span := queryTracer.Start(ctx, "query.read_tx_state",
		trace.WithAttributes(
			attribute.Int64("ledger_id", int64(ledgerID)),
			attribute.Int64("transaction_id", int64(txID)),
		))
	defer span.End()

	txKey := domain.TransactionKey{LedgerID: ledgerID, ID: txID}

	state, err := attrs.Get(reader, txKey.Bytes())
	if err != nil {
		return nil, fmt.Errorf("computing transaction state for tx %d: %w", txID, err)
	}

	return state, nil
}

// FindTransactionCreationLog returns the system log that created a transaction.
// It reads the TransactionState to find the creation log sequence.
func FindTransactionCreationLog(ctx context.Context, reader dal.PebbleReader, attrs *attributes.Attribute[*commonpb.TransactionState], ledgerID uint32, txID uint64) (*commonpb.Log, error) {
	state, err := ReadTransactionState(ctx, reader, attrs, ledgerID, txID)
	if err != nil {
		return nil, fmt.Errorf("reading transaction state for %d: %w", txID, err)
	}

	if state == nil || state.GetCreatedByLog() == 0 {
		return nil, domain.ErrNotFound
	}

	log, err := ReadLogBySequence(ctx, reader, state.GetCreatedByLog())
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", state.GetCreatedByLog(), err)
	}

	if log == nil {
		return nil, domain.ErrNotFound
	}

	return log, nil
}
