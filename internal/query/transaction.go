package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
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

// FindTransactionCreationLog returns the system log that created a transaction.
// It reads the TransactionState to find the creation log sequence, then reads
// that log with a cold-storage fallback so the creation log of a transaction
// whose chapter has been archived (and purged from hot storage) is still found.
func FindTransactionCreationLog(ctx context.Context, reader dal.PebbleReader, coldReader *coldstorage.ColdReader, attrs *attributes.Attribute[*commonpb.TransactionState], ledgerName string, txID uint64) (*commonpb.Log, error) {
	state, err := ReadTransactionState(ctx, reader, attrs, ledgerName, txID)
	if err != nil {
		return nil, fmt.Errorf("reading transaction state for %d: %w", txID, err)
	}

	if state == nil || state.GetCreatedByLog() == 0 {
		return nil, domain.ErrNotFound
	}

	log, err := ReadLogBySequenceWithCold(ctx, reader, coldReader, state.GetCreatedByLog())
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", state.GetCreatedByLog(), err)
	}

	if log == nil {
		return nil, domain.ErrNotFound
	}

	return log, nil
}
