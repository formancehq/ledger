package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
	sink := &commonpb.SinkConfig{Name: "sink", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Secret: "credentialSentinel", Endpoint: &commonpb.ConnectionURL{Scheme: "https", Username: "u", Password: new("urlSentinel"), Address: &commonpb.ConnectionAddress{Host: "localhost"}}}}}
	// Sink adapters sanitize diagnostics before persistence; public reads retain them.
	const diagnostic = "posting event seq=1: sending request to https://localhost/events?key=[redacted]: EOF"
	sinkStatus := &commonpb.SinkStatus{SinkName: "sink", Cursor: 12, Error: &commonpb.SinkError{Message: diagnostic}}
	originalSink, originalStatus := sink.CloneVT(), sinkStatus.CloneVT()
	backend.EXPECT().GetEventsSinks(gomock.Any()).Return([]*commonpb.SinkConfig{sink}, []*commonpb.SinkStatus{sinkStatus}, nil)
	response, err := impl.GetEventsSinks(context.Background(), &servicepb.GetEventsSinksRequest{})
	require.NoError(t, err)
	requireCredentialFreeProto(t, response)
	require.Equal(t, uint64(12), response.GetSinkStatuses()[0].GetCursor())
	require.Equal(t, diagnostic, response.GetSinkStatuses()[0].GetError().GetMessage())
	require.True(t, proto.Equal(originalSink, sink))
	require.True(t, proto.Equal(originalStatus, sinkStatus))
	impl.authCfg = credentialReadAuth(internalauth.ScopeLedgersRead)
	info := &commonpb.LedgerInfo{Name: "a", MirrorSource: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Connection: &commonpb.DatabaseConnection{Scheme: "postgres", Username: new("u"), Password: new("credentialSentinel"), Database: new("db"), Addresses: []*commonpb.ConnectionAddress{{Host: "localhost"}}, Options: []*commonpb.ConnectionOption{{Name: "sslpassword", Value: &commonpb.ConnectionOption_SecretText{SecretText: "urlSentinel"}}}}}}}}
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
	info := &commonpb.LedgerInfo{Name: "mirror", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR, MirrorSource: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Connection: &commonpb.DatabaseConnection{Scheme: "postgres", Username: new("u"), Password: new("credentialSentinel"), Database: new("db"), Addresses: []*commonpb.ConnectionAddress{{Host: "localhost"}}, Options: []*commonpb.ConnectionOption{{Name: "sslpassword", Value: &commonpb.ConnectionOption_SecretText{SecretText: "urlSentinel"}}}}}}}}
	log := &commonpb.Log{Sequence: 9, ResponseSignature: &signaturepb.SignedLog{Payload: []byte("credentialSentinel")}, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "mirror", MirrorSource: info.GetMirrorSource().CloneVT()}}}}
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
