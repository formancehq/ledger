package ctrl

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

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
)

func TestDefaultControllerGetLogReturnsProjectionWithoutMutatingStore(t *testing.T) {
	t.Parallel()

	store := newReceiptTestStore(t)
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
	require.Contains(t, projected.String(), redactedSecret)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	authoritative, err := query.ReadLogBySequence(context.Background(), handle, stored.GetSequence())
	require.NoError(t, err)
	require.Contains(t, authoritative.String(), secrets[1])
}

func TestDefaultControllerGetAuditEntryReturnsProjectionWithoutMutatingStore(t *testing.T) {
	t.Parallel()

	store := newReceiptTestStore(t)
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
	batch.KeyBuilder.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(entry.GetSequence())
	require.NoError(t, batch.SetProtoDeterministic(batch.KeyBuilder.Consume(), entry))
	batch.KeyBuilder.PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(entry.GetSequence()).PutUint32(0)
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

func newSecretProjectionTestController(store *dal.Store) *DefaultController {
	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	return NewDefaultController(nil, store, logger, attributes.New(), nil, nil, nil, nil, meter)
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

	projected, err := projectAuditEntryForRead(entry)
	require.NoError(t, err)
	require.Equal(t, original, entry, "read projection must not mutate authoritative audit data")
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
}

func TestProjectAuditEntryForReadRejectsUnparseableSecretContainers(t *testing.T) {
	t.Parallel()

	_, err := projectAuditEntryForRead(&auditpb.AuditEntry{
		Items: []*auditpb.AuditItem{{SerializedOrder: []byte{0xff}}},
	})
	require.ErrorContains(t, err, "unmarshaling serialized order")

	_, err = projectAuditEntryForRead(&auditpb.AuditEntry{
		Signature: &signaturepb.SignedApplyBatch{Payload: []byte{0xff}},
	})
	require.ErrorContains(t, err, "unmarshaling ApplyBatch")
}

func TestProjectLogForReadRedactsEveryCredentialVariant(t *testing.T) {
	t.Parallel()

	sinkConfigs, sinkSecrets := secretBearingSinkConfigs()
	for i, config := range sinkConfigs {
		log := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config},
		}}}
		original := log.CloneVT()

		projected, err := projectLogForRead(log)
		require.NoError(t, err)
		require.Equal(t, original, log)
		require.NotContains(t, projected.String(), sinkSecrets[i])
		require.Contains(t, projected.String(), redactedSecret)
	}

	mirrorSources, mirrorSecrets := secretBearingMirrorSources()
	for i, source := range mirrorSources {
		log := &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{MirrorSource: source},
		}}}
		original := log.CloneVT()

		projected, err := projectLogForRead(log)
		require.NoError(t, err)
		require.Equal(t, original, log)
		require.NotContains(t, projected.String(), mirrorSecrets[i])
		require.Contains(t, projected.String(), redactedSecret)
	}
}

func TestProjectingCursorPreservesItemsErrorsAndPagination(t *testing.T) {
	t.Parallel()

	upstream := &projectionTestCursor{items: []*commonpb.Log{{Sequence: 1}}, nextCursor: "next-page"}
	projected := newProjectingCursor[*commonpb.Log](upstream, projectLogForRead)

	item, err := projected.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1), item.GetSequence())

	_, err = projected.Next()
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, "next-page", projected.(interface{ NextCursor() string }).NextCursor())
	require.NoError(t, projected.Close())
	require.True(t, upstream.closed)
}

type projectionTestCursor struct {
	items      []*commonpb.Log
	nextCursor string
	closed     bool
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

func (c *projectionTestCursor) NextCursor() string {
	return c.nextCursor
}

var _ cursor.Cursor[*commonpb.Log] = (*projectionTestCursor)(nil)

func TestProjectingCursorReturnsProjectionErrors(t *testing.T) {
	t.Parallel()

	expected := errors.New("projection failed")
	projected := newProjectingCursor(
		cursor.NewSliceCursor([]*commonpb.Log{{Sequence: 1}}),
		func(*commonpb.Log) (*commonpb.Log, error) { return nil, expected },
	)

	_, err := projected.Next()
	require.ErrorIs(t, err, expected)
}

func secretBearingSinkConfigs() ([]*commonpb.SinkConfig, []string) {
	return []*commonpb.SinkConfig{
			{Name: "nats", Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{Url: "nats://token-secret@nats.example:4222", Topic: "ledger"}}},
			{Name: "clickhouse", Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{Dsn: "clickhouse://user:clickhouse-secret@db.example/ledger", Table: "events"}}},
			{Name: "kafka", Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{Brokers: []string{"broker:9092"}, SaslUsername: "ledger", SaslPassword: "kafka-secret"}}},
			{Name: "http", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://hooks.example/ledger", Secret: "webhook-secret"}}},
			{Name: "databricks-token", Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{ServerHostname: "db.example", Auth: &commonpb.DatabricksSinkConfig_Token{Token: "databricks-token-secret"}}}},
			{Name: "databricks-oauth", Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{ServerHostname: "db.example", Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "client", ClientSecret: "databricks-oauth-secret"}}}}},
		}, []string{
			"nats://token-secret@nats.example:4222",
			"clickhouse://user:clickhouse-secret@db.example/ledger",
			"kafka-secret",
			"webhook-secret",
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
	}, []string{"mirror-oauth-secret", "postgres://ledger:mirror-postgres-secret@db.example/ledger"}
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
