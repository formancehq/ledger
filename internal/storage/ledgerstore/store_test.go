package ledgerstore

import (
	"context"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestInitializeStore(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	modified, err := store.Migrate(context.Background())
	require.NoError(t, err)
	require.False(t, modified)

	migrationInfos, err := store.GetMigrationsInfo(context.Background())
	require.NoError(t, err)
	require.Len(t, migrationInfos, 1)
}

// TODO: remove that
func insertTransactions(ctx context.Context, s *Store, txs ...ledger.Transaction) error {
	var previous *ledger.ChainedLog
	logs := collectionutils.Map(txs, func(from ledger.Transaction) *ledger.ChainedLog {
		previous = ledger.NewTransactionLog(&from, map[string]metadata.Metadata{}).ChainLog(previous)
		return previous
	})
	return s.InsertLogs(ctx, logs...)
}
