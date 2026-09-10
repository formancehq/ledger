package ctrl

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestAuditPublicProjectionPreservesLiveAndCheckpointEvidence(t *testing.T) {
	t.Parallel()
	controller, _ := newAuditAlignmentController(t, 12)
	ctx := context.Background()
	header := &auditpb.AuditEntry{
		Sequence: 1, OrderCount: 1,
		Outcome:   &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS, Message: "historical credential", Context: map[string]string{"password": "historical credential"}}},
		Signature: &signaturepb.SignedApplyBatch{KeyId: "client", Payload: []byte("signed credential"), Signature: []byte("signature bytes")},
	}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: &commonpb.SinkConfigInput{Name: "events", Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://user:password@host/events", Secret: "sink credential"}}}}}}}}
	encoded, err := order.MarshalVT()
	require.NoError(t, err)
	item := &auditpb.AuditItem{SerializedOrder: encoded}
	batch := controller.store.OpenWriteSession()
	require.NoError(t, batch.SetProto(dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1).Build(), header))
	require.NoError(t, batch.SetProto(dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(1).PutUint32(0).Build(), item))
	require.NoError(t, batch.Commit())

	view, err := controller.GetAuditEntry(ctx, 1)
	require.NoError(t, err)
	require.Equal(t, "client", view.GetSignature().GetKeyId())
	require.Equal(t, "[redacted]", view.GetFailure().GetMessage())
	require.Empty(t, view.GetFailure().GetContext())
	require.Len(t, view.GetItems(), 1)
	sink := view.GetItems()[0].GetOrder().GetSystemScoped().GetAddEventsSink().GetConfig()
	require.Equal(t, "events", sink.GetName())
	require.Equal(t, "host", sink.GetHttp().GetEndpoint().GetAddress().GetHost())
	require.Equal(t, "[redacted]", sink.GetHttp().GetSecret())
	require.Equal(t, "[redacted]", sink.GetHttp().GetEndpoint().GetPassword())

	cpPath := filepath.Join(t.TempDir(), "checkpoint")
	require.NoError(t, controller.store.Checkpoint(cpPath))
	frozen, err := dal.OpenReadOnly(cpPath, logging.NopZap())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, frozen.Close()) })
	for _, store := range []*dal.Store{controller.store, frozen} {
		rows, err := controller.ListAuditEntriesFrom(ctx, store, nil, 10, 0, nil, false)
		require.NoError(t, err)
		entries, err := cursor.Collect(rows)
		require.NoError(t, err)
		require.Len(t, entries, 1)
		require.Empty(t, entries[0].GetItems())
		require.Equal(t, "client", entries[0].GetSignature().GetKeyId())
		require.Equal(t, "[redacted]", entries[0].GetFailure().GetMessage())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)
		original, err := query.ReadAuditEntry(ctx, handle, 1)
		require.NoError(t, err)
		require.True(t, proto.Equal(header, original))
		items, err := query.ReadAuditItems(ctx, handle, 1)
		require.NoError(t, err)
		require.Len(t, items, 1)
		require.True(t, proto.Equal(item, items[0]))
		require.NoError(t, handle.Close())
	}
}
