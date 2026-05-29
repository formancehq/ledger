package query_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
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
	worldKey := domain.VolumeKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "world"}, Asset: "USD"}
	worldCanonicalKey := worldKey.Bytes()
	_, err = attrs.Volume.Set(batch, worldCanonicalKey, &raftcmdpb.VolumePair{
		Output: commonpb.NewUint256FromUint64(100),
	})
	require.NoError(t, err)

	metadataKey := domain.MetadataKey{AccountKey: domain.AccountKey{LedgerID: 1, Account: "bank"}, Key: "key"}
	metadataCanonicalKey := metadataKey.Bytes()
	_, err = attrs.Metadata.Set(batch, metadataCanonicalKey, commonpb.NewStringValue("value"))
	require.NoError(t, err)
	txKey := domain.TransactionKey{LedgerID: 1, ID: 1}
	_, err = attrs.Transaction.Set(batch, txKey.Bytes(), &commonpb.TransactionState{
		CreatedByLog: 1,
	})
	require.NoError(t, err)
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
	volumeResult, err := attrs.Volume.Get(s, worldCanonicalKey)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(100), volumeResult.GetOutput().ToBigInt())
}
