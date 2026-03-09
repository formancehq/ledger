package query_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
)

func TestReadLedgers(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	// Initially no ledgers
	cursor, err := query.ReadLedgers(context.Background(), s)
	require.NoError(t, err)
	ledgers, err := collectLedgers(cursor)
	require.NoError(t, err)
	require.Empty(t, ledgers)

	// Register first ledger
	registerLedger(t, s, "ledger-1")
	cursor, err = query.ReadLedgers(context.Background(), s)
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.Equal(t, "ledger-1", ledgers[0].GetName())

	// Register second ledger
	registerLedger(t, s, "ledger-2")
	cursor, err = query.ReadLedgers(context.Background(), s)
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 2)
}

func TestGetLedgerByName(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	registerLedger(t, s, "my-ledger")

	ledger, err := query.GetLedgerByName(context.Background(), s, "my-ledger")
	require.NoError(t, err)
	require.NotNil(t, ledger)
	require.Equal(t, "my-ledger", ledger.GetName())

	ledger, err = query.GetLedgerByName(context.Background(), s, "non-existing")
	require.Error(t, err)
	require.Nil(t, ledger)
}

func TestReadLedgersSoftDelete(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	attrs := attributes.New()

	const ledgerName = "test-ledger"

	createdAt := commonpb.NewTimestamp(libtime.Now())
	batch := s.NewBatch()
	err := state.SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      ledgerName,
		CreatedAt: createdAt,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Add some data
	batch = s.NewBatch()
	worldKey := domain.VolumeKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "world"}, Asset: "USD"}
	worldCanonicalKey := worldKey.Bytes()
	require.NoError(t, attrs.Volume.Set(batch, 1, worldCanonicalKey, &raftcmdpb.VolumePair{
		Output: commonpb.NewUint256FromUint64(100),
	}))

	metadataKey := domain.MetadataKey{AccountKey: domain.AccountKey{Ledger: ledgerName, Account: "bank"}, Key: "key"}
	metadataCanonicalKey := metadataKey.Bytes()
	require.NoError(t, attrs.Metadata.Set(batch, 1, metadataCanonicalKey, commonpb.NewStringValue("value")))
	require.NoError(t, state.StoreTransactionUpdate(batch, domain.TransactionKey{Ledger: ledgerName, ID: 1}, &commonpb.TransactionUpdate{
		ByLog: 1,
		Updates: []*commonpb.TransactionUpdateType{
			{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			},
		},
	}))
	require.NoError(t, batch.Commit())

	// Verify ledger exists and is not deleted
	cursor, err := query.ReadLedgers(context.Background(), s)
	require.NoError(t, err)
	ledgers, err := collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.Nil(t, ledgers[0].GetDeletedAt())

	// Soft delete ledger
	deletedAt := commonpb.NewTimestamp(libtime.Now())
	batch = s.NewBatch()
	require.NoError(t, state.SaveLedger(batch, &commonpb.LedgerInfo{
		Name:      ledgerName,
		CreatedAt: createdAt,
		DeletedAt: deletedAt,
	}))
	require.NoError(t, batch.Commit())

	// Verify ledger still exists but is marked as deleted
	cursor, err = query.ReadLedgers(context.Background(), s)
	require.NoError(t, err)
	ledgers, err = collectLedgers(cursor)
	require.NoError(t, err)
	require.Len(t, ledgers, 1)
	require.NotNil(t, ledgers[0].GetDeletedAt())
	require.Equal(t, deletedAt.GetData(), ledgers[0].GetDeletedAt().GetData())

	// Verify data still exists (soft delete doesn't remove data)
	volumeResult, _, err := attrs.Volume.ComputeValue(s, 100, worldCanonicalKey)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(100), volumeResult.GetOutput().ToBigInt())
}
