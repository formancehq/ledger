package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadTransactionState reads the current state of a transaction from the attributes zone.
func ReadTransactionState(ctx context.Context, reader dal.PebbleReader, attrs *attributes.Attribute[*commonpb.TransactionState], ledger string, txID uint64) (*commonpb.TransactionState, error) {
	_, span := queryTracer.Start(ctx, "query.read_tx_state",
		trace.WithAttributes(
			attribute.String("ledger", ledger),
			attribute.Int64("transaction_id", int64(txID)),
		))
	defer span.End()

	txKey := domain.TransactionKey{Ledger: ledger, ID: txID}

	state, err := attrs.Get(reader, txKey.Bytes())
	if err != nil {
		return nil, fmt.Errorf("computing transaction state for tx %d: %w", txID, err)
	}

	return state, nil
}

// FindTransactionCreationLog returns the system log that created a transaction.
// It reads the TransactionState to find the creation log sequence.
func FindTransactionCreationLog(ctx context.Context, reader dal.PebbleReader, attrs *attributes.Attribute[*commonpb.TransactionState], ledgerName string, txID uint64) (*commonpb.Log, error) {
	state, err := ReadTransactionState(ctx, reader, attrs, ledgerName, txID)
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
