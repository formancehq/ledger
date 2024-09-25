//go:build it

package ledgerstore

import (
	"context"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/v2/internal"
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
