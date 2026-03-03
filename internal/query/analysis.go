package query

import (
	"fmt"
	"io"

	"github.com/formancehq/ledger-v3-poc/internal/domain/analysis"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ScanTransactionsForAnalysis performs a single sequential scan of all Pebble logs,
// extracting CompactTransaction directly from CreatedTransaction/RevertedTransaction
// log entries. This avoids the per-transaction random seeks of the previous approach
// (bbolt scan → Pebble ReadTransactionUpdates → Pebble ReadLogBySequence).
func ScanTransactionsForAnalysis(reader dal.PebbleReader, ledgerName string) ([]analysis.CompactTransaction, error) {
	cursor, err := ReadLogsSince(reader, 0, dal.WithReuse())
	if err != nil {
		return nil, fmt.Errorf("creating log cursor for analysis: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	var transactions []analysis.CompactTransaction
	// txIndex maps transaction ID → index in the transactions slice,
	// so we can mark the original transaction as reverted when we encounter
	// a RevertedTransaction log.
	txIndex := make(map[uint64]int)

	for {
		log, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("reading log for analysis: %w", err)
		}

		if log.Payload == nil {
			continue
		}

		applyLog, ok := log.Payload.Type.(*commonpb.LogPayload_Apply)
		if !ok {
			continue
		}

		if applyLog.Apply.LedgerName != ledgerName {
			continue
		}

		ledgerLog := applyLog.Apply.Log
		if ledgerLog == nil || ledgerLog.Data == nil {
			continue
		}

		switch p := ledgerLog.Data.Payload.(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if p.CreatedTransaction.Transaction == nil {
				continue
			}
			ct := analysis.ExtractCompactTransaction(p.CreatedTransaction.Transaction)
			txIndex[p.CreatedTransaction.Transaction.Id] = len(transactions)
			transactions = append(transactions, ct)

		case *commonpb.LedgerLogPayload_RevertedTransaction:
			// Mark original transaction as reverted
			if idx, found := txIndex[p.RevertedTransaction.RevertedTransactionId]; found {
				transactions[idx].Reverted = true
			}
			// Also extract the revert transaction itself
			if p.RevertedTransaction.RevertTransaction != nil {
				ct := analysis.ExtractCompactTransaction(p.RevertedTransaction.RevertTransaction)
				txIndex[p.RevertedTransaction.RevertTransaction.Id] = len(transactions)
				transactions = append(transactions, ct)
			}
		}
	}

	return transactions, nil
}
