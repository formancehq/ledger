package ledgerstore

import (
	"context"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

// TODO: remove that
func insertTransactions(ctx context.Context, s *Store, txs ...ledger.Transaction) error {
	var previous *ledger.ChainedLog
	logs := collectionutils.Map(txs, func(from ledger.Transaction) *ledger.ChainedLog {
		previous = ledger.NewTransactionLog(&from, map[string]metadata.Metadata{}).ChainLog(previous)
		return previous
	})
	return s.InsertLogs(ctx, logs...)
}
