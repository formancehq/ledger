package query

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadTransactionUpdates retrieves all updates for a transaction ID from the given reader, ordered by ByLog.
func ReadTransactionUpdates(ctx context.Context, reader dal.PebbleReader, ledger string, transactionID uint64) ([]*commonpb.TransactionUpdate, error) {
	_, span := queryTracer.Start(ctx, "query.read_tx_updates",
		trace.WithAttributes(
			attribute.String("ledger", ledger),
			attribute.Int64("transaction_id", int64(transactionID)),
		))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixTransactionUpdate).
		PutLedgerName(ledger).
		PutUint64(transactionID)
	lowerBound := kb.Snapshot()

	// Upper bound: add 0xFF to get all entries for this transaction
	kb.PutByte(0xFF)
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for transaction updates: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var updates []*commonpb.TransactionUpdate

	for iter.First(); iter.Valid(); iter.Next() {
		valueBytes, err := iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading transaction update value: %w", err)
		}

		update := &commonpb.TransactionUpdate{}
		if err := proto.Unmarshal(valueBytes, update); err != nil {
			return nil, fmt.Errorf("unmarshaling transaction update: %w", err)
		}

		updates = append(updates, update)
	}

	return updates, nil
}

// FindTransactionCreationLog returns the system log that created a transaction.
// It finds the TransactionInit update and retrieves the log.
func FindTransactionCreationLog(ctx context.Context, reader dal.PebbleReader, ledgerName string, txID uint64) (*commonpb.Log, error) {
	updates, err := ReadTransactionUpdates(ctx, reader, ledgerName, txID)
	if err != nil {
		return nil, fmt.Errorf("getting transaction updates for %d: %w", txID, err)
	}

	var sequence uint64

	for _, update := range updates {
		for _, ut := range update.GetUpdates() {
			if ut.GetTransactionInit() != nil {
				sequence = update.GetByLog()

				break
			}
		}

		if sequence != 0 {
			break
		}
	}

	if sequence == 0 {
		return nil, domain.ErrNotFound
	}

	log, err := ReadLogBySequence(ctx, reader, sequence)
	if err != nil {
		return nil, fmt.Errorf("getting system log %d: %w", sequence, err)
	}

	if log == nil {
		return nil, domain.ErrNotFound
	}

	return log, nil
}
