package ledgerstore_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/stretchr/testify/require"
)

func TestInitializeStore(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)

	modified, err := store.Migrate(context.Background())
	require.NoError(t, err)
	require.False(t, modified)
}

func insertTransactions(ctx context.Context, s *ledgerstore.Store, txs ...core.Transaction) error {
	if err := s.InsertTransactions(ctx, txs...); err != nil {
		return err
	}
	moves := collectionutils.Flatten(collectionutils.Map(txs, core.Transaction.GetMoves))
	if err := s.InsertMoves(ctx, moves...); err != nil {
		return err
	}
	return nil
}
