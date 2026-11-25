package ctrl

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

func TestDefaultControllerGetLogReturnsProjectionWithoutMutatingStore(t *testing.T) {
	t.Parallel()

	store := newCtrlTestStore(t)
	config, secrets := secretBearingSinkConfigs()
	stored := &commonpb.Log{
		Sequence: 7,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config[1]},
		}},
	}
	batch := store.OpenWriteSession()
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{stored}))
	require.NoError(t, batch.Commit())

	projected, err := newSecretProjectionTestController(store).GetLog(context.Background(), stored.GetSequence())
	require.NoError(t, err)
	require.NotContains(t, projected.String(), secrets[1])
	require.Nil(t, projected.GetResponseSignature())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	authoritative, err := query.ReadLogBySequence(context.Background(), handle, stored.GetSequence())
	require.NoError(t, err)
	require.Contains(t, authoritative.String(), secrets[1])
	require.Nil(t, authoritative.GetResponseSignature())
}

func TestDefaultControllerListLogsOmitsUnknownPayloadFieldsWithoutMutatingStore(t *testing.T) {
	t.Parallel()

	const ledger = "unknown-fields"
	const canary = "future-log-secret-canary"
	logger := logging.FromContext(logging.TestingContext())
	store := newCtrlTestStore(t)
	payload := &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_FillGap{FillGap: &commonpb.FilledGapLog{}}}
	payload.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 1000, protowire.BytesType), []byte(canary)))
	stored := &commonpb.Log{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: ledger, Log: &commonpb.LedgerLog{Id: 1, Data: payload},
	}}}}

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, ledger, &commonpb.LedgerInfo{Name: ledger}))
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{stored}))
	require.NoError(t, state.SetAppliedIndex(batch, 1))
	require.NoError(t, batch.Commit())

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })
	indexBatch := rs.NewBatch()
	wb := readstore.NewWriteBatch()
	wb.Init(indexBatch)
	require.NoError(t, wb.WriteLedgerLogIndex(dal.NewKeyBuilder(), ledger, 1, 1))
	require.NoError(t, rs.WriteProgress(indexBatch, 1))
	require.NoError(t, rs.WriteRaftProgress(indexBatch, 1))
	require.NoError(t, indexBatch.Commit())
	rs.NotifyProgress()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })
	authoritativeBefore, err := query.ReadLogBySequence(t.Context(), handle, 1)
	require.NoError(t, err)
	beforeBytes, err := authoritativeBefore.MarshalVT()
	require.NoError(t, err)
	require.Contains(t, string(beforeBytes), canary)

	ctrl := NewDefaultController(nil, store, logger, attributes.New(), rs, nil, noop.NewMeterProvider().Meter("test"))
	listed, err := ctrl.ListLogs(t.Context(), ledger, 0, 10, nil, false)
	require.NoError(t, err)
	logs, err := cursor.Collect(listed)
	require.NoError(t, err)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].GetPayload().GetApply().GetLog().GetId())
	projectedBytes, err := logs[0].MarshalVT()
	require.NoError(t, err)
	require.NotContains(t, string(projectedBytes), canary)
	require.Empty(t, logs[0].GetPayload().GetApply().GetLog().GetData().ProtoReflect().GetUnknown())

	authoritativeAfter, err := query.ReadLogBySequence(t.Context(), handle, 1)
	require.NoError(t, err)
	afterBytes, err := authoritativeAfter.MarshalVT()
	require.NoError(t, err)
	require.Equal(t, beforeBytes, afterBytes)
}

func TestDefaultControllerGetAuditEntryReturnsProjectionWithoutMutatingStore(t *testing.T) {
	t.Parallel()

	store := newCtrlTestStore(t)
	config, secrets := secretBearingSinkConfigs()
	order := addSinkOrder(config[2])
	serializedOrder := processing.MarshalOrderBusinessIntent(order, nil)
	publicBatch, err := (&servicepb.ApplyBatch{Requests: []*servicepb.Request{{
		Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config[2].CloneVT()}},
	}}}).MarshalVT()
	require.NoError(t, err)

	entry := &auditpb.AuditEntry{
		Sequence: 9,
		Signature: &signaturepb.SignedApplyBatch{
			KeyId: "key", Signature: []byte("signature"), Payload: publicBatch,
		},
	}
	item := &auditpb.AuditItem{OrderIndex: 0, SerializedOrder: serializedOrder}

	batch := store.OpenWriteSession()
	batch.KeyBuilder.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(entry.GetSequence())
	require.NoError(t, batch.SetProtoDeterministic(batch.KeyBuilder.Consume(), entry))
	batch.KeyBuilder.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(entry.GetSequence()).PutUint32(0)
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), item))
	require.NoError(t, batch.Commit())

	projected, err := newSecretProjectionTestController(store).GetAuditEntry(context.Background(), entry.GetSequence())
	require.NoError(t, err)
	require.NotContains(t, string(projected.GetItems()[0].GetSerializedOrder()), secrets[2])
	require.NotContains(t, string(projected.GetSignature().GetPayload()), secrets[2])
	require.Empty(t, projected.GetSignature().GetSignature())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	authoritativeItems, err := query.ReadAuditItems(context.Background(), handle, entry.GetSequence())
	require.NoError(t, err)
	require.Contains(t, string(authoritativeItems[0].GetSerializedOrder()), secrets[2])
	authoritativeEntry, err := query.ReadAuditEntry(context.Background(), handle, entry.GetSequence())
	require.NoError(t, err)
	require.Contains(t, string(authoritativeEntry.GetSignature().GetPayload()), secrets[2])
}

func TestListAuditEntriesProjectsSignedSecretsOnDirectAndIndexedCheckpointPaths(t *testing.T) {
	t.Parallel()

	controller, rs := newAuditAlignmentController(t, 12)
	config := &commonpb.SinkConfig{Name: "sink", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{
		Endpoint: "https://user:historical-password@hooks.example/events",
	}}}
	payload, err := (&servicepb.ApplyBatch{Requests: []*servicepb.Request{{
		Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config}},
	}}}).MarshalVT()
	require.NoError(t, err)
	entry := &auditpb.AuditEntry{
		Sequence:  1,
		Ledgers:   []string{"main"},
		Outcome:   &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
		Signature: &signaturepb.SignedApplyBatch{KeyId: "key", Signature: []byte("signature"), Payload: payload},
	}
	batch := controller.store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1).Build()
	require.NoError(t, batch.SetProto(key, entry))
	require.NoError(t, batch.Commit())

	indexBatch := rs.NewBatch()
	require.NoError(t, indexBatch.SetBytes(readstore.AuditIndexStringKey(dal.NewKeyBuilder(), readstore.AuditFieldLedger, "main", 1), nil))
	require.NoError(t, rs.WriteAuditProgress(indexBatch, 1))
	require.NoError(t, rs.WriteAuditRaftProgress(indexBatch, 12))
	require.NoError(t, indexBatch.Commit())

	checkpointDir := filepath.Join(t.TempDir(), "readindex")
	require.NoError(t, rs.CreateCheckpoint(checkpointDir))
	frozen, err := readstore.OpenReadOnly(checkpointDir, logging.NopZap())
	require.NoError(t, err)
	t.Cleanup(func() { _ = frozen.Close() })

	for _, tc := range []struct {
		name string
		list func() (cursor.Cursor[*auditpb.AuditEntry], error)
	}{
		{name: "direct", list: func() (cursor.Cursor[*auditpb.AuditEntry], error) {
			return controller.ListAuditEntries(context.Background(), 10, 0, nil, false)
		}},
		{name: "indexed checkpoint", list: func() (cursor.Cursor[*auditpb.AuditEntry], error) {
			return controller.ListAuditEntriesFrom(context.Background(), controller.store, frozen, 10, 0, auditLedgerFilter("main"), false)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			listed, err := tc.list()
			require.NoError(t, err)
			entries, err := cursor.Collect(listed)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			require.NotContains(t, string(entries[0].GetSignature().GetPayload()), "historical-password")
			require.Empty(t, entries[0].GetSignature().GetSignature())
		})
	}

	handle, err := controller.store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })
	stored, err := query.ReadAuditEntry(context.Background(), handle, 1)
	require.NoError(t, err)
	require.Equal(t, payload, stored.GetSignature().GetPayload())
	require.Equal(t, []byte("signature"), stored.GetSignature().GetSignature())
}

func newSecretProjectionTestController(store *dal.Store) *DefaultController {
	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	return NewDefaultController(nil, store, logger, attributes.New(), nil, nil, meter)
}

func TestProjectAuditEntryForReadRedactsOrdersAndSignedPayload(t *testing.T) {
	t.Parallel()

	sinkConfigs, sinkSecrets := secretBearingSinkConfigs()
	mirrorSources, mirrorSecrets := secretBearingMirrorSources()

	var (
		items    []*auditpb.AuditItem
		requests []*servicepb.Request
	)

	for _, config := range sinkConfigs {
		order := addSinkOrder(config.CloneVT())
		items = append(items, &auditpb.AuditItem{
			OrderIndex:      uint32(len(items)),
			SerializedOrder: processing.MarshalOrderBusinessIntent(order, nil),
		})
		requests = append(requests, &servicepb.Request{Type: &servicepb.Request_AddEventsSink{
			AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config.CloneVT()},
		}})
	}

	for _, source := range mirrorSources {
		order := createLedgerOrder(source.CloneVT())
		items = append(items, &auditpb.AuditItem{
			OrderIndex:      uint32(len(items)),
			SerializedOrder: processing.MarshalOrderBusinessIntent(order, nil),
		})
		requests = append(requests, &servicepb.Request{Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{MirrorSource: source.CloneVT()},
		}})
	}

	batchPayload, err := (&servicepb.ApplyBatch{Requests: requests}).MarshalVT()
	require.NoError(t, err)

	entry := &auditpb.AuditEntry{
		Sequence: 42,
		Items:    items,
		Signature: &signaturepb.SignedApplyBatch{
			KeyId:     "signer-key",
			Signature: []byte("original-signature"),
			Payload:   batchPayload,
		},
	}
	original := entry.CloneVT()

	projected, err := projectHistoricalAudit(entry)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, entry), "read projection must not mutate authoritative audit data")
	require.Empty(t, projected.GetSignature().GetSignature(), "signature does not cover projected bytes")
	require.Equal(t, "signer-key", projected.GetSignature().GetKeyId())

	projectedBatch := &servicepb.ApplyBatch{}
	require.NoError(t, projectedBatch.UnmarshalVT(projected.GetSignature().GetPayload()))
	require.Len(t, projectedBatch.GetRequests(), len(requests))

	for _, item := range projected.GetItems() {
		order := &raftcmdpb.Order{}
		require.NoError(t, order.UnmarshalVT(item.GetSerializedOrder()))
	}

	rendered, err := json.Marshal(projected)
	require.NoError(t, err)

	for _, secret := range append(sinkSecrets, mirrorSecrets...) {
		require.NotContains(t, string(rendered), secret)
		require.NotContains(t, string(projected.GetSignature().GetPayload()), secret)
		for _, item := range projected.GetItems() {
			require.NotContains(t, string(item.GetSerializedOrder()), secret)
		}
	}
	require.NotContains(t, string(rendered), "token-endpoint-password")
}

func TestProjectAuditEntryForReadPreservesVerifiableEvidenceWithoutCredentials(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{Name: "sink"}},
	}}, Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{0x01}}}
	// Model a legacy audit item that still contains OrderTechnical. A no-op
	// secret projection must preserve these exact historical bytes rather than
	// normalizing them through the current business-intent marshaller.
	serializedOrder := order.MarshalDeterministicVT(nil)
	batchPayload, err := (&servicepb.ApplyBatch{Requests: []*servicepb.Request{{
		Type: &servicepb.Request_RemoveEventsSink{RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{Name: "sink"}},
	}}}).MarshalVT()
	require.NoError(t, err)

	entry := &auditpb.AuditEntry{
		Sequence: 43,
		Items:    []*auditpb.AuditItem{{SerializedOrder: serializedOrder}},
		Signature: &signaturepb.SignedApplyBatch{
			KeyId:     "signer-key",
			Signature: []byte("original-signature"),
			Payload:   batchPayload,
		},
	}

	projected, err := projectHistoricalAudit(entry)
	require.NoError(t, err)
	require.True(t, proto.Equal(entry, projected))
	require.Equal(t, serializedOrder, projected.GetItems()[0].GetSerializedOrder())
	require.Equal(t, batchPayload, projected.GetSignature().GetPayload())
	require.Equal(t, []byte("original-signature"), projected.GetSignature().GetSignature())
}

func TestProjectAuditEntryForReadRejectsUnparseableSecretContainers(t *testing.T) {
	t.Parallel()

	_, err := projectHistoricalAudit(&auditpb.AuditEntry{
		Items: []*auditpb.AuditItem{{SerializedOrder: []byte{0xff}}},
	})
	require.ErrorContains(t, err, "decoding audit order")

	_, err = projectHistoricalAudit(&auditpb.AuditEntry{
		Signature: &signaturepb.SignedApplyBatch{Payload: []byte{0xff}},
	})
	require.ErrorContains(t, err, "decoding signed audit batch")
}

func TestProjectLogForReadRedactsEveryCredentialVariant(t *testing.T) {
	t.Parallel()

	sinkConfigs, sinkSecrets := secretBearingSinkConfigs()
	for i, config := range sinkConfigs {
		log := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config},
		}}}
		original := log.CloneVT()

		projected, err := projectHistoricalLog(log)
		require.NoError(t, err)
		require.True(t, proto.Equal(original, log))
		require.NotContains(t, projected.String(), sinkSecrets[i])
	}

	mirrorSources, mirrorSecrets := secretBearingMirrorSources()
	for i, source := range mirrorSources {
		log := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{MirrorSource: source},
		}}}
		original := log.CloneVT()

		projected, err := projectHistoricalLog(log)
		require.NoError(t, err)
		require.True(t, proto.Equal(original, log))
		require.NotContains(t, projected.String(), mirrorSecrets[i])
	}
	// The OAuth token endpoint is an independently credential-bearing URL.
	projected, err := projectHistoricalLog(&commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
		CreateLedger: &commonpb.CreatedLedgerLog{MirrorSource: mirrorSources[len(mirrorSources)-1]},
	}}})
	require.NoError(t, err)
	require.NotContains(t, projected.String(), "token-endpoint-password")
}

func TestProjectingCursorPreservesItemsAndClose(t *testing.T) {
	t.Parallel()

	upstream := &projectionTestCursor{items: []*commonpb.Log{{Sequence: 1}}}
	projected := &historicalCursor[*commonpb.Log]{inner: upstream, project: projectHistoricalLog}

	item, err := projected.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), item.GetSequence())

	_, err = projected.Next()
	require.ErrorIs(t, err, io.EOF)
	require.NoError(t, projected.Close())
	require.True(t, upstream.closed)
}

type projectionTestCursor struct {
	items  []*commonpb.Log
	closed bool
}

func (c *projectionTestCursor) Next() (*commonpb.Log, error) {
	if len(c.items) == 0 {
		return nil, io.EOF
	}

	item := c.items[0]
	c.items = c.items[1:]

	return item, nil
}

func (c *projectionTestCursor) Close() error {
	c.closed = true

	return nil
}

var _ cursor.Cursor[*commonpb.Log] = (*projectionTestCursor)(nil)

func TestProjectingCursorReturnsProjectionErrors(t *testing.T) {
	t.Parallel()

	expected := errors.New("projection failed")
	projected := &historicalCursor[*commonpb.Log]{
		inner:   cursor.NewSliceCursor([]*commonpb.Log{{Sequence: 1}}),
		project: func(*commonpb.Log) (*commonpb.Log, error) { return nil, expected },
	}

	_, err := projected.Next()
	require.ErrorIs(t, err, expected)
}

func secretBearingSinkConfigs() ([]*commonpb.SinkConfig, []string) {
	return []*commonpb.SinkConfig{
		{Name: "nats", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{Url: "nats://token-secret@nats.example:4222", Topic: "ledger"}}},
		{Name: "clickhouse", Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{Dsn: "clickhouse://user:clickhouse-secret@db.example/ledger", Table: "events"}}},
		{Name: "kafka", Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{Brokers: []string{"broker:9092"}, SaslUsername: "ledger", SaslPassword: "kafka-secret"}}},
		{Name: "http", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://hooks.example/ledger", Secret: "webhook-secret"}}},
		{Name: "http-userinfo", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://user:http-password@hooks.example/ledger"}}},
		{Name: "databricks-token", Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{ServerHostname: "db.example", Auth: &commonpb.DatabricksSinkConfig_Token{Token: "databricks-token-secret"}}}},
		{Name: "databricks-oauth", Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{ServerHostname: "db.example", Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "client", ClientSecret: "databricks-oauth-secret"}}}}},
	}, []string{
		"nats://token-secret@nats.example:4222",
		"clickhouse://user:clickhouse-secret@db.example/ledger",
		"kafka-secret",
		"webhook-secret",
		"http-password",
		"databricks-token-secret",
		"databricks-oauth-secret",
	}
}

func secretBearingMirrorSources() ([]*commonpb.MirrorSourceConfig, []string) {
	return []*commonpb.MirrorSourceConfig{
		{LedgerName: "source-http", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
			BaseUrl: "https://ledger.example",
			Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{
				ClientId: "client", ClientSecret: "mirror-oauth-secret", TokenEndpoint: "https://auth.example/token",
			},
		}}},
		{LedgerName: "source-postgres", Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{
			Dsn: "postgres://ledger:mirror-postgres-secret@db.example/ledger",
		}}},
		{LedgerName: "source-http-userinfo", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{
			BaseUrl:                 "https://user:mirror-http-password@ledger.example",
			Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{TokenEndpoint: "https://user:token-endpoint-password@auth.example/token"},
		}}},
	}, []string{"mirror-oauth-secret", "postgres://ledger:mirror-postgres-secret@db.example/ledger", "mirror-http-password"}
}

func addSinkOrder(config *commonpb.SinkConfig) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{AddEventsSink: &raftcmdpb.AddEventsSinkOrder{Config: config}},
	}}}
}

func createLedgerOrder(source *commonpb.MirrorSourceConfig) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{MirrorSource: source}},
	}}}
}
