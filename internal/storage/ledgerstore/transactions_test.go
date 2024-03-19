package ledgerstore

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"

	"github.com/pkg/errors"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pointer"

	ledger "github.com/formancehq/ledger/internal"
	internaltesting "github.com/formancehq/ledger/internal/testing"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
)

func expandLogs(logs ...*ledger.Log) []ledger.ExpandedTransaction {
	ret := make([]ledger.ExpandedTransaction, 0)
	accumulatedVolumes := ledger.AccountsAssetsVolumes{}

	appendTx := func(tx *ledger.Transaction) {
		expandedTx := &ledger.ExpandedTransaction{
			Transaction: *tx,
		}
		for _, posting := range tx.Postings {
			expandedTx.PreCommitVolumes.AddInput(posting.Destination, posting.Asset, accumulatedVolumes.GetVolumes(posting.Destination, posting.Asset).Input)
			expandedTx.PreCommitVolumes.AddOutput(posting.Destination, posting.Asset, accumulatedVolumes.GetVolumes(posting.Destination, posting.Asset).Output)
			expandedTx.PreCommitVolumes.AddOutput(posting.Source, posting.Asset, accumulatedVolumes.GetVolumes(posting.Source, posting.Asset).Output)
			expandedTx.PreCommitVolumes.AddInput(posting.Source, posting.Asset, accumulatedVolumes.GetVolumes(posting.Source, posting.Asset).Input)
		}
		for _, posting := range tx.Postings {
			accumulatedVolumes.AddOutput(posting.Source, posting.Asset, posting.Amount)
			accumulatedVolumes.AddInput(posting.Destination, posting.Asset, posting.Amount)
		}
		for _, posting := range tx.Postings {
			expandedTx.PostCommitVolumes.AddInput(posting.Destination, posting.Asset, accumulatedVolumes.GetVolumes(posting.Destination, posting.Asset).Input)
			expandedTx.PostCommitVolumes.AddOutput(posting.Destination, posting.Asset, accumulatedVolumes.GetVolumes(posting.Destination, posting.Asset).Output)
			expandedTx.PostCommitVolumes.AddOutput(posting.Source, posting.Asset, accumulatedVolumes.GetVolumes(posting.Source, posting.Asset).Output)
			expandedTx.PostCommitVolumes.AddInput(posting.Source, posting.Asset, accumulatedVolumes.GetVolumes(posting.Source, posting.Asset).Input)
		}
		ret = append(ret, *expandedTx)
	}

	for _, log := range logs {
		switch payload := log.Data.(type) {
		case ledger.NewTransactionLogPayload:
			appendTx(payload.Transaction)
		case ledger.RevertedTransactionLogPayload:
			appendTx(payload.RevertTransaction)
			ret[payload.RevertedTransactionID.Uint64()].Reverted = true
		case ledger.SetMetadataLogPayload:
			ret[payload.TargetID.(*big.Int).Uint64()].Metadata = ret[payload.TargetID.(*big.Int).Uint64()].Metadata.Merge(payload.Metadata)
		}
	}

	return ret
}

func Reverse[T any](values ...T) []T {
	ret := make([]T, len(values))
	for i := 0; i < len(values)/2; i++ {
		ret[i], ret[len(values)-i-1] = values[len(values)-i-1], values[i]
	}
	if len(values)%2 == 1 {
		ret[(len(values)-1)/2] = values[(len(values)-1)/2]
	}
	return ret
}

func TestGetTransactionWithVolumes(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(0),
			TransactionData: ledger.TransactionData{
				Postings: []ledger.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Reference: "tx1",
				Timestamp: now.Add(-3 * time.Hour),
			},
		},
		PostCommitVolumes: ledger.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
		PreCommitVolumes: ledger.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
	}
	tx2 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(1),
			TransactionData: ledger.TransactionData{
				Postings: []ledger.Posting{
					{
						Source:      "world",
						Destination: "central_bank",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Reference: "tx2",
				Timestamp: now.Add(-2 * time.Hour),
			},
		},
		PostCommitVolumes: ledger.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(200),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(0),
				},
			},
		},
		PreCommitVolumes: ledger.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"central_bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
	}

	require.NoError(t, insertTransactions(ctx, store, tx1.Transaction, tx2.Transaction))

	tx, err := store.GetTransactionWithVolumes(ctx, NewGetTransactionQuery(tx1.ID).
		WithExpandVolumes().
		WithExpandEffectiveVolumes())
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(100),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	}, tx.PostCommitVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(0),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(0),
			},
		},
	}, tx.PreCommitVolumes)

	tx, err = store.GetTransactionWithVolumes(ctx, NewGetTransactionQuery(tx2.ID).
		WithExpandVolumes().
		WithExpandEffectiveVolumes())
	require.Equal(t, tx2.Postings, tx.Postings)
	require.Equal(t, tx2.Reference, tx.Reference)
	require.Equal(t, tx2.Timestamp, tx.Timestamp)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(200),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(200),
				Output: big.NewInt(0),
			},
		},
	}, tx.PostCommitVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"world": {
			"USD": {
				Input:  big.NewInt(0),
				Output: big.NewInt(100),
			},
		},
		"central_bank": {
			"USD": {
				Input:  big.NewInt(100),
				Output: big.NewInt(0),
			},
		},
	}, tx.PreCommitVolumes)
}

func TestGetTransaction(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.Transaction{
		ID: big.NewInt(0),
		TransactionData: ledger.TransactionData{
			Postings: []ledger.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Reference: "tx1",
			Timestamp: now.Add(-3 * time.Hour),
		},
	}
	tx2 := ledger.Transaction{
		ID: big.NewInt(1),
		TransactionData: ledger.TransactionData{
			Postings: []ledger.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Reference: "tx2",
			Timestamp: now.Add(-2 * time.Hour),
		},
	}

	require.NoError(t, insertTransactions(context.Background(), store, tx1, tx2))

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
}

func TestGetTransactionByReference(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.Transaction{
		ID: big.NewInt(0),
		TransactionData: ledger.TransactionData{
			Postings: []ledger.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Reference: "tx1",
			Timestamp: now.Add(-3 * time.Hour),
		},
	}
	tx2 := ledger.Transaction{
		ID: big.NewInt(1),
		TransactionData: ledger.TransactionData{
			Postings: []ledger.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Reference: "tx2",
			Timestamp: now.Add(-2 * time.Hour),
		},
	}

	require.NoError(t, insertTransactions(context.Background(), store, tx1, tx2))

	tx, err := store.GetTransactionByReference(context.Background(), "tx1")
	require.NoError(t, err)
	require.Equal(t, tx1.Postings, tx.Postings)
	require.Equal(t, tx1.Reference, tx.Reference)
	require.Equal(t, tx1.Timestamp, tx.Timestamp)
}

func TestInsertTransactions(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()
	ctx := logging.TestingContext()

	t.Run("success inserting transaction", func(t *testing.T) {
		tx1 := ledger.ExpandedTransaction{
			Transaction: ledger.Transaction{
				ID: big.NewInt(0),
				TransactionData: ledger.TransactionData{
					Postings: ledger.Postings{
						{
							Source:      "world",
							Destination: "alice",
							Amount:      big.NewInt(100),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-3 * time.Hour),
					Metadata:  metadata.Metadata{},
				},
			},
			PreCommitVolumes: map[string]ledger.VolumesByAssets{
				"world": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes(),
				},
				"alice": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes(),
				},
			},
			PostCommitVolumes: map[string]ledger.VolumesByAssets{
				"world": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithOutputInt64(100),
				},
				"alice": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithInputInt64(100),
				},
			},
		}

		err := insertTransactions(context.Background(), store, tx1.Transaction)
		require.NoError(t, err, "inserting transaction should not fail")

		tx, err := store.GetTransactionWithVolumes(ctx, NewGetTransactionQuery(big.NewInt(0)).
			WithExpandVolumes())
		require.NoError(t, err)
		internaltesting.RequireEqual(t, tx1, *tx)
	})

	t.Run("success inserting multiple transactions", func(t *testing.T) {
		t.Parallel()
		tx2 := ledger.ExpandedTransaction{
			Transaction: ledger.Transaction{
				ID: big.NewInt(1),
				TransactionData: ledger.TransactionData{
					Postings: ledger.Postings{
						{
							Source:      "world",
							Destination: "polo",
							Amount:      big.NewInt(200),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-2 * time.Hour),
					Metadata:  metadata.Metadata{},
				},
			},
			PreCommitVolumes: map[string]ledger.VolumesByAssets{
				"world": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithOutputInt64(100),
				},
				"polo": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes(),
				},
			},
			PostCommitVolumes: map[string]ledger.VolumesByAssets{
				"world": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithOutputInt64(300),
				},
				"polo": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithInputInt64(200),
				},
			},
		}

		tx3 := ledger.ExpandedTransaction{
			Transaction: ledger.Transaction{
				ID: big.NewInt(2),
				TransactionData: ledger.TransactionData{
					Postings: ledger.Postings{
						{
							Source:      "world",
							Destination: "gfyrag",
							Amount:      big.NewInt(150),
							Asset:       "USD",
						},
					},
					Timestamp: now.Add(-1 * time.Hour),
					Metadata:  metadata.Metadata{},
				},
			},
			PreCommitVolumes: map[string]ledger.VolumesByAssets{
				"world": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithOutputInt64(300),
				},
				"gfyrag": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes(),
				},
			},
			PostCommitVolumes: map[string]ledger.VolumesByAssets{
				"world": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithOutputInt64(450),
				},
				"gfyrag": map[string]*ledger.Volumes{
					"USD": ledger.NewEmptyVolumes().WithInputInt64(150),
				},
			},
		}

		require.NoError(t, store.InsertLogs(context.Background(),
			ledger.NewTransactionLog(&tx2.Transaction, map[string]metadata.Metadata{}).ChainLog(nil).WithID(2),
			ledger.NewTransactionLog(&tx3.Transaction, map[string]metadata.Metadata{}).ChainLog(nil).WithID(3),
		))

		tx, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(big.NewInt(1)).WithExpandVolumes())
		require.NoError(t, err, "getting transaction should not fail")
		internaltesting.RequireEqual(t, tx2, *tx)

		tx, err = store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(big.NewInt(2)).WithExpandVolumes())
		require.NoError(t, err, "getting transaction should not fail")
		internaltesting.RequireEqual(t, tx3, *tx)
	})
}

func TestCountTransactions(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(0),
			TransactionData: ledger.TransactionData{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Timestamp: now.Add(-3 * time.Hour),
				Metadata:  metadata.Metadata{},
			},
		},
		PreCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
			"alice": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
		},
		PostCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(100),
			},
			"alice": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithInputInt64(100),
			},
		},
	}
	tx2 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(1),
			TransactionData: ledger.TransactionData{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "polo",
						Amount:      big.NewInt(200),
						Asset:       "USD",
					},
				},
				Timestamp: now.Add(-2 * time.Hour),
				Metadata:  metadata.Metadata{},
			},
		},
		PreCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(100),
			},
			"polo": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
		},
		PostCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(300),
			},
			"polo": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithInputInt64(200),
			},
		},
	}

	tx3 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(2),
			TransactionData: ledger.TransactionData{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "gfyrag",
						Amount:      big.NewInt(150),
						Asset:       "USD",
					},
				},
				Timestamp: now.Add(-1 * time.Hour),
				Metadata:  metadata.Metadata{},
			},
		},
		PreCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(300),
			},
			"gfyrag": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
		},
		PostCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(450),
			},
			"gfyrag": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithInputInt64(150),
			},
		},
	}

	err := insertTransactions(context.Background(), store, tx1.Transaction, tx2.Transaction, tx3.Transaction)
	require.NoError(t, err, "inserting transaction should not fail")

	count, err := store.CountTransactions(context.Background(), NewGetTransactionsQuery(NewPaginatedQueryOptions(PITFilterWithVolumes{})))
	require.NoError(t, err, "counting transactions should not fail")
	require.Equal(t, 3, count, "count should be equal")
}

func TestUpdateTransactionsMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(0),
			TransactionData: ledger.TransactionData{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "alice",
						Amount:      big.NewInt(100),
						Asset:       "USD",
					},
				},
				Timestamp: now.Add(-3 * time.Hour),
				Metadata:  metadata.Metadata{},
			},
		},
		PreCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
			"alice": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
		},
		PostCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(100),
			},
			"alice": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithInputInt64(100),
			},
		},
	}
	tx2 := ledger.ExpandedTransaction{
		Transaction: ledger.Transaction{
			ID: big.NewInt(1),
			TransactionData: ledger.TransactionData{
				Postings: ledger.Postings{
					{
						Source:      "world",
						Destination: "polo",
						Amount:      big.NewInt(200),
						Asset:       "USD",
					},
				},
				Timestamp: now.Add(-2 * time.Hour),
				Metadata:  metadata.Metadata{},
			},
		},
		PreCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(100),
			},
			"polo": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes(),
			},
		},
		PostCommitVolumes: map[string]ledger.VolumesByAssets{
			"world": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithOutputInt64(300),
			},
			"polo": map[string]*ledger.Volumes{
				"USD": ledger.NewEmptyVolumes().WithInputInt64(200),
			},
		},
	}

	err := insertTransactions(context.Background(), store, tx1.Transaction, tx2.Transaction)
	require.NoError(t, err, "inserting transaction should not fail")

	err = store.InsertLogs(context.Background(),
		ledger.NewSetMetadataOnTransactionLog(ledger.Now(), tx1.ID, metadata.Metadata{"foo1": "bar2"}).ChainLog(nil).WithID(3),
		ledger.NewSetMetadataOnTransactionLog(ledger.Now(), tx2.ID, metadata.Metadata{"foo2": "bar2"}).ChainLog(nil).WithID(4),
	)
	require.NoError(t, err, "updating multiple transaction metadata should not fail")

	tx, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(big.NewInt(0)).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err, "getting transaction should not fail")
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo1": "bar2"}, "metadata should be equal")

	tx, err = store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(big.NewInt(1)).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err, "getting transaction should not fail")
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo2": "bar2"}, "metadata should be equal")
}

func TestDeleteTransactionsMetadata(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.Transaction{
		ID: big.NewInt(0),
		TransactionData: ledger.TransactionData{
			Postings: ledger.Postings{
				{
					Source:      "world",
					Destination: "alice",
					Amount:      big.NewInt(100),
					Asset:       "USD",
				},
			},
			Timestamp: now.Add(-3 * time.Hour),
			Metadata:  metadata.Metadata{},
		},
	}

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.NewTransactionLog(&tx1, map[string]metadata.Metadata{}).ChainLog(nil).WithID(1),
		ledger.NewSetMetadataOnTransactionLog(ledger.Now(), tx1.ID, metadata.Metadata{"foo1": "bar1", "foo2": "bar2"}).ChainLog(nil).WithID(2),
	))

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	require.NoError(t, err)
	require.Equal(t, tx.Metadata, metadata.Metadata{"foo1": "bar1", "foo2": "bar2"})

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.NewDeleteMetadataLog(ledger.Now(), ledger.DeleteMetadataLogPayload{
			TargetType: ledger.MetaTargetTypeTransaction,
			TargetID:   tx1.ID,
			Key:        "foo1",
		}).ChainLog(nil).WithID(3),
	))

	tx, err = store.GetTransaction(context.Background(), tx1.ID)
	require.NoError(t, err)
	require.Equal(t, metadata.Metadata{"foo2": "bar2"}, tx.Metadata)
}

func TestInsertTransactionInPast(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	).WithDate(now)

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user1", "USD/2", big.NewInt(50)),
	).WithDate(now.Add(time.Hour)).WithIDUint64(1)

	// Insert in past must modify pre/post commit volumes of tx2
	tx3 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user2", "USD/2", big.NewInt(50)),
	).WithDate(now.Add(30 * time.Minute)).WithIDUint64(2)

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}).ChainLog(nil).WithID(1),
		ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}).ChainLog(nil).WithID(2),
		ledger.NewTransactionLog(tx3, map[string]metadata.Metadata{}).ChainLog(nil).WithID(3),
	))

	tx2FromDatabase, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(tx2.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err)

	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 50),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(0, 0),
		},
	}, tx2FromDatabase.PreCommitEffectiveVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 100),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(50, 0),
		},
	}, tx2FromDatabase.PostCommitEffectiveVolumes)
}

func TestInsertTransactionInPastInOneBatch(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	).WithDate(now)

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user1", "USD/2", big.NewInt(50)),
	).WithDate(now.Add(time.Hour)).WithIDUint64(1)

	// Insert in past must modify pre/post commit volumes of tx2
	tx3 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user2", "USD/2", big.NewInt(50)),
	).WithDate(now.Add(30 * time.Minute)).WithIDUint64(2)

	require.NoError(t, insertTransactions(context.Background(), store, *tx1, *tx2, *tx3))

	tx2FromDatabase, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(tx2.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err)

	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 50),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(0, 0),
		},
	}, tx2FromDatabase.PreCommitEffectiveVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 100),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(50, 0),
		},
	}, tx2FromDatabase.PostCommitEffectiveVolumes)
}

func TestInsertTwoTransactionAtSameDateInSameBatch(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	).WithDate(now.Add(-time.Hour))

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user1", "USD/2", big.NewInt(10)),
	).WithDate(now).WithIDUint64(1)

	tx3 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user2", "USD/2", big.NewInt(10)),
	).WithDate(now).WithIDUint64(2)

	require.NoError(t, insertTransactions(context.Background(), store, *tx1, *tx2, *tx3))

	tx2FromDatabase, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(tx2.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err)

	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 10),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(10, 0),
		},
	}, tx2FromDatabase.PostCommitVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 0),
		},
		"user1": {
			"USD/2": ledger.NewVolumesInt64(0, 0),
		},
	}, tx2FromDatabase.PreCommitVolumes)

	tx3FromDatabase, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(tx3.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err)

	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 10),
		},
		"user2": {
			"USD/2": ledger.NewVolumesInt64(0, 0),
		},
	}, tx3FromDatabase.PreCommitVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 20),
		},
		"user2": {
			"USD/2": ledger.NewVolumesInt64(10, 0),
		},
	}, tx3FromDatabase.PostCommitVolumes)
}

func TestInsertTwoTransactionAtSameDateInTwoBatch(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()

	tx1 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	).WithDate(now.Add(-time.Hour))

	tx2 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user1", "USD/2", big.NewInt(10)),
	).WithDate(now).WithIDUint64(1)

	require.NoError(t, insertTransactions(context.Background(), store, *tx1, *tx2))

	tx3 := ledger.NewTransaction().WithPostings(
		ledger.NewPosting("bank", "user2", "USD/2", big.NewInt(10)),
	).WithDate(now).WithIDUint64(2)

	require.NoError(t, store.InsertLogs(context.Background(),
		ledger.NewTransactionLog(tx3, map[string]metadata.Metadata{}).ChainLog(nil).WithID(3),
	))

	tx3FromDatabase, err := store.GetTransactionWithVolumes(context.Background(), NewGetTransactionQuery(tx3.ID).WithExpandVolumes().WithExpandEffectiveVolumes())
	require.NoError(t, err)

	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 10),
		},
		"user2": {
			"USD/2": ledger.NewVolumesInt64(0, 0),
		},
	}, tx3FromDatabase.PreCommitVolumes)
	internaltesting.RequireEqual(t, ledger.AccountsAssetsVolumes{
		"bank": {
			"USD/2": ledger.NewVolumesInt64(100, 20),
		},
		"user2": {
			"USD/2": ledger.NewVolumesInt64(10, 0),
		},
	}, tx3FromDatabase.PostCommitVolumes)
}

func TestGetTransactions(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	now := ledger.Now()
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithIDUint64(0).
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "1"}).
		WithDate(now.Add(-3 * time.Hour))
	tx2 := ledger.NewTransaction().
		WithIDUint64(1).
		WithPostings(
			ledger.NewPosting("world", "bob", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "2"}).
		WithDate(now.Add(-2 * time.Hour))
	tx3 := ledger.NewTransaction().
		WithIDUint64(2).
		WithPostings(
			ledger.NewPosting("world", "users:marley", "USD", big.NewInt(100)),
		).
		WithMetadata(metadata.Metadata{"category": "3"}).
		WithDate(now.Add(-time.Hour))
	tx4 := ledger.NewTransaction().
		WithIDUint64(3).
		WithPostings(
			ledger.NewPosting("users:marley", "world", "USD", big.NewInt(100)),
		).
		WithDate(now)

	logs := []*ledger.Log{
		ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}),
		ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}),
		ledger.NewTransactionLog(tx3, map[string]metadata.Metadata{}),
		ledger.NewRevertedTransactionLog(ledger.Now(), tx3.ID, tx4),
		ledger.NewSetMetadataOnTransactionLog(ledger.Now(), tx3.ID, metadata.Metadata{
			"additional_metadata": "true",
		}),
	}

	require.NoError(t, store.InsertLogs(ctx, ledger.ChainLogs(logs...)...))

	type testCase struct {
		name        string
		query       PaginatedQueryOptions[PITFilterWithVolumes]
		expected    *bunpaginate.Cursor[ledger.ExpandedTransaction]
		expectError error
	}
	testCases := []testCase{
		{
			name:  "nominal",
			query: NewPaginatedQueryOptions(PITFilterWithVolumes{}),
			expected: &bunpaginate.Cursor[ledger.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     Reverse(expandLogs(logs...)...),
			},
		},
		{
			name: "address filter",
			query: NewPaginatedQueryOptions(PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "bob")),
			expected: &bunpaginate.Cursor[ledger.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     expandLogs(logs...)[1:2],
			},
		},
		{
			name: "address filter using segment",
			query: NewPaginatedQueryOptions(PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("account", "users:")),
			expected: &bunpaginate.Cursor[ledger.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     Reverse(expandLogs(logs...)[2:]...),
			},
		},
		{
			name: "filter using metadata",
			query: NewPaginatedQueryOptions(PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("metadata[category]", "2")),
			expected: &bunpaginate.Cursor[ledger.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     expandLogs(logs...)[1:2],
			},
		},
		{
			name: "using point in time",
			query: NewPaginatedQueryOptions(PITFilterWithVolumes{
				PITFilter: PITFilter{
					PIT: pointer.For(now.Add(-time.Hour)),
				},
			}),
			expected: &bunpaginate.Cursor[ledger.ExpandedTransaction]{
				PageSize: 15,
				HasMore:  false,
				Data:     Reverse(expandLogs(logs[:3]...)...),
			},
		},
		{
			name: "filter using invalid key",
			query: NewPaginatedQueryOptions(PITFilterWithVolumes{}).
				WithQueryBuilder(query.Match("invalid", "2")),
			expectError: &errInvalidQuery{},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.query.Options.ExpandVolumes = true
			tc.query.Options.ExpandEffectiveVolumes = false
			cursor, err := store.GetTransactions(ctx, NewGetTransactionsQuery(tc.query))
			if tc.expectError != nil {
				require.True(t, errors.Is(err, tc.expectError))
			} else {
				require.NoError(t, err)
				require.Len(t, cursor.Data, len(tc.expected.Data))
				internaltesting.RequireEqual(t, *tc.expected, *cursor)

				count, err := store.CountTransactions(ctx, NewGetTransactionsQuery(tc.query))
				require.NoError(t, err)
				require.EqualValues(t, len(tc.expected.Data), count)
			}
		})
	}
}

func TestGetLastTransaction(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	tx1 := ledger.NewTransaction().
		WithIDUint64(0).
		WithPostings(
			ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
		)
	tx2 := ledger.NewTransaction().
		WithIDUint64(1).
		WithPostings(
			ledger.NewPosting("world", "bob", "USD", big.NewInt(100)),
		)
	tx3 := ledger.NewTransaction().
		WithIDUint64(2).
		WithPostings(
			ledger.NewPosting("world", "users:marley", "USD", big.NewInt(100)),
		)

	logs := []*ledger.Log{
		ledger.NewTransactionLog(tx1, map[string]metadata.Metadata{}),
		ledger.NewTransactionLog(tx2, map[string]metadata.Metadata{}),
		ledger.NewTransactionLog(tx3, map[string]metadata.Metadata{}),
	}

	require.NoError(t, store.InsertLogs(ctx, ledger.ChainLogs(logs...)...))

	tx, err := store.GetLastTransaction(ctx)
	require.NoError(t, err)
	require.Equal(t, *tx3, tx.Transaction)
}

func TestTransactionFromWorldToWorld(t *testing.T) {
	t.Parallel()
	store := newLedgerStore(t)
	ctx := logging.TestingContext()

	tx := ledger.NewTransaction().
		WithIDUint64(0).
		WithPostings(
			ledger.NewPosting("world", "world", "USD", big.NewInt(100)),
		)
	require.NoError(t, store.InsertLogs(ctx, ledger.ChainLogs(ledger.NewTransactionLog(tx, map[string]metadata.Metadata{}))...))

	account, err := store.GetAccountWithVolumes(ctx, NewGetAccountQuery("world").WithExpandVolumes())
	require.NoError(t, err)
	internaltesting.RequireEqual(t, big.NewInt(0), account.Volumes.Balances()["USD"])
}
