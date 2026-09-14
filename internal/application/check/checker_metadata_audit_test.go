package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestCheck_SavedMetadataUsesAuditedOrderAsAuthority(t *testing.T) {
	t.Parallel()

	t.Run("healthy", func(t *testing.T) {
		t.Parallel()

		engine, _ := metadataAuditFixture(t)
		errors, terminalErr := runMetadataAuditCheck(engine)
		require.NoError(t, terminalErr)
		require.Empty(t, errors)
	})

	t.Run("coordinated log and projection corruption", func(t *testing.T) {
		t.Parallel()

		engine, log := metadataAuditFixture(t)
		tamperSavedMetadata(t, engine, log, true)
		require.Equal(t, "approved", auditedMetadataValue(t, engine, 2, "status").GetStringValue())

		errors, terminalErr := runMetadataAuditCheck(engine)
		require.NoError(t, terminalErr)
		require.Len(t, errors, 1)
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH, errors[0].GetErrorType())
		require.Equal(t, log.GetSequence(), errors[0].GetLogSequence())
	})

	t.Run("projection-only corruption", func(t *testing.T) {
		t.Parallel()

		engine, log := metadataAuditFixture(t)
		tamperSavedMetadata(t, engine, log, false)

		errors, terminalErr := runMetadataAuditCheck(engine)
		require.NoError(t, terminalErr)
		require.Len(t, errors, 1)
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH, errors[0].GetErrorType())
	})
}

func auditedMetadataValue(t *testing.T, engine *testEngine, auditSequence uint64, key string) *commonpb.MetadataValue {
	t.Helper()

	snap, err := engine.store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()

	itemKey := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).
		PutUint64(auditSequence).
		PutUint32(0).
		Build()
	value, closer, err := snap.Get(itemKey)
	require.NoError(t, err)
	defer func() { require.NoError(t, closer.Close()) }()

	item := &auditpb.AuditItem{}
	require.NoError(t, item.UnmarshalVT(value))
	order := &raftcmdpb.Order{}
	require.NoError(t, order.UnmarshalVT(item.GetSerializedOrder()))

	return order.GetLedgerScoped().GetApply().GetAddMetadata().GetMetadata()[key]
}

func metadataAuditFixture(t *testing.T) (*testEngine, *commonpb.Log) {
	t.Helper()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("main"))
	logs := engine.processAndCommit(saveAccountMetadataOrder("main", "users:001", map[string]string{"status": "approved"}))
	require.Len(t, logs, 1)

	return engine, logs[0]
}

func tamperSavedMetadata(t *testing.T, engine *testEngine, log *commonpb.Log, rewriteLog bool) {
	t.Helper()

	tampered := commonpb.NewStringValue("rejected")
	batch := engine.store.OpenWriteSession()
	defer func() { _ = batch.Cancel() }()

	if rewriteLog {
		forgedLog := log.CloneVT()
		forgedLog.GetPayload().GetApply().GetLog().GetData().GetSavedMetadata().Metadata["status"] = tampered
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(log.GetSequence()).Build()
		require.NoError(t, batch.SetProto(key, forgedLog))
	}

	metadataKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: "main", Account: "users:001"},
		Key:        "status",
	}
	_, err := engine.attrs.Metadata.Set(batch, metadataKey.Bytes(), tampered)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func runMetadataAuditCheck(engine *testEngine) ([]*servicepb.CheckStoreError, error) {
	checker := NewChecker(engine.store, engine.attrs, engine.clusterID, nil, logging.Testing())
	var errors []*servicepb.CheckStoreError
	err := checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if checkErr := event.GetError(); checkErr != nil {
			errors = append(errors, checkErr)
		}
	})

	return errors, err
}
