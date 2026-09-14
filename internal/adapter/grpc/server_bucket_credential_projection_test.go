package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func credentialReadAuth(scope internalauth.Scope) internalauth.AuthConfig {
	return internalauth.AuthConfig{Enabled: true, ScopeMapping: internalauth.ScopeMapping{internalauth.ScopeMappingAnonymousKey: {scope}}}
}

func requireCredentialFreeProto(t *testing.T, message proto.Message) {
	t.Helper()
	encoded, err := proto.Marshal(message)
	require.NoError(t, err)
	require.NotContains(t, string(encoded), "credentialSentinel")
	require.NotContains(t, string(encoded), "urlSentinel")
}

func TestCredentialProjectionLiveGRPC(t *testing.T) {
	t.Parallel()
	impl, backend := newListHandlerHarness(t)
	impl.authCfg = credentialReadAuth(internalauth.ScopeOpsRead)
	sink := &commonpb.SinkConfig{Name: "sink", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Secret: "credentialSentinel", Endpoint: "https://u:urlSentinel@localhost"}}}
	sinkStatus := &commonpb.SinkStatus{SinkName: "sink", Cursor: 12, Error: &commonpb.SinkError{Message: "dial https://u:credentialSentinel@localhost failed"}}
	originalSink, originalStatus := sink.CloneVT(), sinkStatus.CloneVT()
	backend.EXPECT().GetEventsSinks(gomock.Any()).Return([]*commonpb.SinkConfig{sink}, []*commonpb.SinkStatus{sinkStatus}, nil)
	response, err := impl.GetEventsSinks(context.Background(), &servicepb.GetEventsSinksRequest{})
	require.NoError(t, err)
	requireCredentialFreeProto(t, response)
	require.Equal(t, uint64(12), response.GetSinkStatuses()[0].GetCursor())
	require.True(t, proto.Equal(originalSink, sink))
	require.True(t, proto.Equal(originalStatus, sinkStatus))
	impl.authCfg = credentialReadAuth(internalauth.ScopeLedgersRead)
	info := &commonpb.LedgerInfo{Name: "a", MirrorSource: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}
	original := info.CloneVT()
	backend.EXPECT().GetLedgerByName(gomock.Any(), "a").Return(info, nil)
	got, err := impl.GetLedger(context.Background(), &servicepb.GetLedgerRequest{Ledger: "a"})
	require.NoError(t, err)
	requireCredentialFreeProto(t, got)
	require.Equal(t, "a", got.GetName())
	// Two pages prove the projection leaves exclusive name cursors intact.
	second := info.CloneVT()
	second.Name = "b"
	backend.EXPECT().ListLedgers(gomock.Any()).Return(page(info, second), nil)
	backend.EXPECT().ListLedgers(gomock.Any()).Return(page(info, second), nil)
	firstPage := newFakeServerStream[commonpb.LedgerInfo](t)
	require.NoError(t, impl.ListLedgers(&servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{PageSize: 1}}, firstPage))
	require.Len(t, firstPage.sent, 1)
	require.Equal(t, "a", firstPage.trailerCursor())
	requireCredentialFreeProto(t, firstPage.sent[0])
	lastPage := newFakeServerStream[commonpb.LedgerInfo](t)
	require.NoError(t, impl.ListLedgers(&servicepb.ListLedgersRequest{Options: &commonpb.ListOptions{PageSize: 1, Cursor: firstPage.trailerCursor()}}, lastPage))
	require.Len(t, lastPage.sent, 1)
	require.Equal(t, "b", lastPage.sent[0].GetName())
	require.Empty(t, lastPage.trailerCursor())
	requireCredentialFreeProto(t, lastPage.sent[0])
	require.True(t, proto.Equal(original, info))
}

func TestCredentialProjectionHistoryGRPC(t *testing.T) {
	t.Parallel()
	for _, config := range []struct{ name, order, batch, log string }{
		{"sink", `{"systemScoped":{"addEventsSink":{"config":{"name":"sink","http":{"secret":"credentialSentinel","endpoint":"https://u:urlSentinel@localhost"}}}}}`, `{"requests":[{"addEventsSink":{"config":{"name":"sink","http":{"secret":"credentialSentinel","endpoint":"https://u:urlSentinel@localhost"}}}}]}`, `{"payload":{"addedEventsSink":{"config":{"name":"sink","http":{"secret":"credentialSentinel","endpoint":"https://u:urlSentinel@localhost"}}}}}`},
		{"mirror", `{"ledgerScoped":{"ledger":"mirror","createLedger":{"mirrorSource":{"postgres":{"dsn":"postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}}`, `{"requests":[{"createLedger":{"name":"mirror","mirrorSource":{"postgres":{"dsn":"postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}]}`, `{"payload":{"createLedger":{"name":"mirror","mirrorSource":{"postgres":{"dsn":"postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}}`},
	} {
		t.Run(config.name, func(t *testing.T) {
			t.Parallel()
			impl, backend := newListHandlerHarness(t)
			impl.authCfg = credentialReadAuth(internalauth.ScopeAuditRead)
			order := &raftcmdpb.Order{}
			require.NoError(t, protojson.Unmarshal([]byte(config.order), order))
			orderBytes, err := proto.Marshal(order)
			require.NoError(t, err)
			batch := &servicepb.ApplyBatch{}
			require.NoError(t, protojson.Unmarshal([]byte(config.batch), batch))
			batchBytes, err := proto.Marshal(batch)
			require.NoError(t, err)
			entry := &auditpb.AuditEntry{Sequence: 7, Items: []*auditpb.AuditItem{{SerializedOrder: orderBytes}}, Signature: &signaturepb.SignedApplyBatch{Payload: batchBytes, Signature: []byte("signature")}}
			original := entry.CloneVT()
			backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(7)).Return(entry, nil)
			got, err := impl.GetAuditEntry(context.Background(), &servicepb.GetAuditEntryRequest{Sequence: 7})
			require.NoError(t, err)
			requireCredentialFreeProto(t, got)
			require.NotEmpty(t, got.GetItems()[0].GetSerializedOrder())
			require.NotEmpty(t, got.GetSignature().GetPayload())
			listed := entry.CloneVT()
			listed.Items = nil
			// A forwarded cursor's trailer must survive projection, even though its
			// token is different from the last entry's sequence.
			backend.EXPECT().ListAuditEntries(gomock.Any(), uint32(3), uint64(0), gomock.Any(), false).Return(&upstreamCursor[auditpb.AuditEntry]{items: []*auditpb.AuditEntry{listed}, nextCursor: "17"}, nil)
			stream := newFakeServerStream[auditpb.AuditEntry](t)
			require.NoError(t, impl.ListAuditEntries(&servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{PageSize: 2}}, stream))
			require.Len(t, stream.sent, 1)
			require.Equal(t, "17", stream.trailerCursor())
			requireCredentialFreeProto(t, stream.sent[0])
			require.True(t, proto.Equal(original, entry))
			log := &commonpb.Log{}
			require.NoError(t, protojson.Unmarshal([]byte(config.log), log))
			log.Sequence = 9
			originalLog := log.CloneVT()
			impl.authCfg = credentialReadAuth(internalauth.ScopeOpsRead)
			backend.EXPECT().GetLog(gomock.Any(), uint64(9)).Return(log, nil)
			gotLog, err := impl.GetLog(context.Background(), &servicepb.GetLogRequest{Sequence: 9})
			require.NoError(t, err)
			requireCredentialFreeProto(t, gotLog)
			// Current ListLogs only yields Apply records; this additional synthetic
			// creation-log case guards the shared response boundary if that scope grows.
			// GetLog above independently covers the currently reachable system history.
			impl.authCfg = credentialReadAuth(internalauth.ScopeLedgersRead)
			backend.EXPECT().ListLogs(gomock.Any(), "mirror", uint64(0), uint32(3), gomock.Any()).Return(&upstreamCursor[commonpb.Log]{items: []*commonpb.Log{log}, nextCursor: "19"}, nil)
			logStream := newFakeServerStream[commonpb.Log](t)
			require.NoError(t, impl.ListLogs(&servicepb.ListLogsRequest{Ledger: "mirror", Options: &commonpb.ListOptions{PageSize: 2}}, logStream))
			require.Len(t, logStream.sent, 1)
			require.Equal(t, "19", logStream.trailerCursor())
			requireCredentialFreeProto(t, logStream.sent[0])
			require.True(t, proto.Equal(originalLog, log))
		})
	}
}

func TestCredentialProjectionMalformedAuditGRPC(t *testing.T) {
	t.Parallel()
	impl, backend := newListHandlerHarness(t)
	impl.authCfg = credentialReadAuth(internalauth.ScopeAuditRead)
	entry := &auditpb.AuditEntry{Sequence: 7, Items: []*auditpb.AuditItem{{SerializedOrder: []byte("credentialSentinel")}}}
	backend.EXPECT().GetAuditEntry(gomock.Any(), uint64(7)).Return(entry, nil)
	got, err := impl.GetAuditEntry(context.Background(), &servicepb.GetAuditEntryRequest{Sequence: 7})
	require.Error(t, err)
	require.Nil(t, got)
	require.NotContains(t, err.Error(), "credentialSentinel")
	backend.EXPECT().ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(page(entry), nil)
	stream := newFakeServerStream[auditpb.AuditEntry](t)
	require.Error(t, impl.ListAuditEntries(&servicepb.ListAuditEntriesRequest{}, stream))
	require.Empty(t, stream.sent)
}

// A real checkpoint controller reads the preserved creation-time credentials
// even after live state changes. Projection belongs after controller selection,
// while both live and checkpoint storage retain their authoritative payloads.
func TestCredentialProjectionCheckpointGRPC(t *testing.T) {
	t.Parallel()
	logger := logging.NopZap()
	meter := noop.NewMeterProvider().Meter("credential-projection")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	index, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	info := &commonpb.LedgerInfo{Name: "mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://u:credentialSentinel@localhost/db?password=urlSentinel"}}}}
	log := &commonpb.Log{Sequence: 9, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "mirror", MirrorSource: info.GetMirrorSource().CloneVT()}}}}
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "mirror", info))
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{log}))
	require.NoError(t, batch.Commit())
	const checkpointID uint64 = 42
	_, err = store.CreateQueryCheckpoint(checkpointID)
	require.NoError(t, err)
	require.NoError(t, index.CreateCheckpoint(store.QueryCheckpointReadIndexDir(checkpointID)))
	require.NoError(t, readstore.MarkCheckpointReady(store.QueryCheckpointReadIndexDir(checkpointID)))
	// Remove the source from live data to uniquely identify the checkpoint path.
	liveInfo := info.CloneVT()
	liveInfo.MirrorSource = nil
	liveLog := log.CloneVT()
	liveLog.Payload.GetCreateLedger().MirrorSource = nil
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "mirror", liveInfo))
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{liveLog}))
	require.NoError(t, batch.Commit())
	local := ctrl.NewDefaultController(nil, store, logger, attributes.New(), index, nil, meter)
	impl := &BucketServiceServerImpl{logger: logger, ctrl: local, localCtrl: local, store: store, authCfg: credentialReadAuth(internalauth.ScopeLedgersRead)}
	got, err := impl.GetLedger(context.Background(), &servicepb.GetLedgerRequest{Ledger: "mirror", Read: &commonpb.ReadOptions{CheckpointId: checkpointID}})
	require.NoError(t, err)
	require.NotNil(t, got.GetMirrorSource())
	requireCredentialFreeProto(t, got)
	live, err := impl.GetLedger(context.Background(), &servicepb.GetLedgerRequest{Ledger: "mirror"})
	require.NoError(t, err)
	require.Nil(t, live.GetMirrorSource())
	impl.authCfg = credentialReadAuth(internalauth.ScopeOpsRead)
	gotLog, err := impl.GetLog(context.Background(), &servicepb.GetLogRequest{Sequence: 9, CheckpointId: checkpointID})
	require.NoError(t, err)
	require.NotNil(t, gotLog.GetPayload().GetCreateLedger().GetMirrorSource())
	requireCredentialFreeProto(t, gotLog)
	// Reopen the checkpoint directly through the controller: the public reads
	// must neither rewrite the snapshot nor strip worker/recovery credentials.
	checkpoint, cleanup, err := impl.readController(context.Background(), checkpointID)
	require.NoError(t, err)
	defer cleanup()
	original, err := checkpoint.GetLedgerByName(context.Background(), "mirror")
	require.NoError(t, err)
	require.True(t, proto.Equal(info.GetMirrorSource(), original.GetMirrorSource()))
	originalLog, err := checkpoint.GetLog(context.Background(), 9)
	require.NoError(t, err)
	require.True(t, proto.Equal(log, originalLog))
}
