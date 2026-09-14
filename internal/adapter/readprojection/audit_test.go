package readprojection

import (
	"crypto/ed25519"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

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

	projected, err := Audit(entry)
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

func TestProjectAuditEntryForReadPreservesVerifiableEvidenceWithoutCredentials(t *testing.T) {
	t.Parallel()

	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
		Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{Name: "sink"}},
	}}, Technical: &raftcmdpb.OrderTechnical{CoverageBits: []byte{0x01}}}
	// A no-op display projection must preserve the original binary encoding,
	// including technical fields when supplied by a controller fixture.
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

	projected, err := Audit(entry)
	require.NoError(t, err)
	require.Equal(t, entry, projected)
	require.Equal(t, serializedOrder, projected.GetItems()[0].GetSerializedOrder())
	require.Equal(t, batchPayload, projected.GetSignature().GetPayload())
	require.Equal(t, []byte("original-signature"), projected.GetSignature().GetSignature())
}

func TestProjectAuditEntryForReadRejectsUnparseableSecretContainers(t *testing.T) {
	t.Parallel()

	_, err := Audit(&auditpb.AuditEntry{
		Items: []*auditpb.AuditItem{{SerializedOrder: []byte{0xff}}},
	})
	require.ErrorContains(t, err, "decoding audit order")

	_, err = Audit(&auditpb.AuditEntry{
		Signature: &signaturepb.SignedApplyBatch{Payload: []byte{0xff}},
	})
	require.ErrorContains(t, err, "decoding signed audit batch")
}

func TestAuditCryptographicEvidenceAndRepeatedProjection(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	for _, secret := range []string{"", "credential-sentinel"} {
		t.Run(secret, func(t *testing.T) {
			t.Parallel()
			config := &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://example.test/hook", Secret: secret}}}
			payload, err := (&servicepb.ApplyBatch{Requests: []*servicepb.Request{{Type: &servicepb.Request_AddEventsSink{AddEventsSink: &servicepb.AddEventsSinkRequest{Config: config}}}}}).MarshalVT()
			require.NoError(t, err)
			entry := &auditpb.AuditEntry{Signature: &signaturepb.SignedApplyBatch{KeyId: "fixture", Payload: payload, Signature: ed25519.Sign(key, payload)}}
			original := proto.Clone(entry)
			result, err := Audit(entry)
			require.NoError(t, err)
			require.True(t, proto.Equal(original, entry))
			require.True(t, ed25519.Verify(key.Public().(ed25519.PublicKey), entry.GetSignature().GetPayload(), entry.GetSignature().GetSignature()))
			if secret == "" {
				require.True(t, proto.Equal(entry, result))
				require.True(t, ed25519.Verify(key.Public().(ed25519.PublicKey), result.GetSignature().GetPayload(), result.GetSignature().GetSignature()))
			} else {
				require.Empty(t, result.GetSignature().GetSignature())
				require.NotContains(t, string(result.GetSignature().GetPayload()), secret)
				require.False(t, ed25519.Verify(key.Public().(ed25519.PublicKey), result.GetSignature().GetPayload(), entry.GetSignature().GetSignature()))
			}
			again, err := Audit(result)
			require.NoError(t, err)
			require.True(t, proto.Equal(result, again))
		})
	}
}

func TestLogProjectionIncludesSignedEnvelope(t *testing.T) {
	t.Parallel()
	configs, secrets := secretBearingSinkConfigs()
	sources, mirrorSecrets := secretBearingMirrorSources()
	var logs []*commonpb.Log
	for _, config := range configs {
		logs = append(logs, &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config}}}})
	}
	for _, source := range sources {
		logs = append(logs, &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{MirrorSource: source}}}})
	}
	secrets = append(secrets, mirrorSecrets...)
	for i, log := range logs {
		payload, err := log.MarshalVT()
		require.NoError(t, err)
		log.ResponseSignature = &signaturepb.SignedLog{KeyId: "fixture", Payload: payload, Signature: []byte("proof-of-original")}
		original := proto.Clone(log)
		result, err := Log(log)
		require.NoError(t, err)
		require.True(t, proto.Equal(original, log))
		require.Empty(t, result.GetResponseSignature().GetSignature())
		require.Equal(t, "fixture", result.GetResponseSignature().GetKeyId())
		require.NotContains(t, result.String(), secrets[i])
		require.NotContains(t, string(result.GetResponseSignature().GetPayload()), secrets[i])
		decoded := &commonpb.Log{}
		require.NoError(t, decoded.UnmarshalVT(result.GetResponseSignature().GetPayload()))
		require.True(t, proto.Equal(result.GetPayload(), decoded.GetPayload()))
		again, err := Log(result)
		require.NoError(t, err)
		require.True(t, proto.Equal(result, again))
	}
}

func TestLogProjectionRejectsInvalidSignedPayload(t *testing.T) {
	t.Parallel()
	_, err := Log(&commonpb.Log{ResponseSignature: &signaturepb.SignedLog{Payload: []byte{0xff}}})
	require.ErrorContains(t, err, "decoding signed log")
	nested, err := (&commonpb.Log{ResponseSignature: &signaturepb.SignedLog{Payload: []byte("untrusted")}}).MarshalVT()
	require.NoError(t, err)
	_, err = Log(&commonpb.Log{ResponseSignature: &signaturepb.SignedLog{Payload: nested}})
	require.ErrorContains(t, err, "nested response signature")
}

func TestProjectionNilAndUnchangedRecords(t *testing.T) {
	t.Parallel()
	log, err := Log(nil)
	require.NoError(t, err)
	require.Nil(t, log)
	entry, err := Audit(nil)
	require.NoError(t, err)
	require.Nil(t, entry)
	// Audit failures are authoritative business diagnostics, not sink/mirror
	// transport status; their reason/context must not be changed by this policy.
	original := &auditpb.AuditEntry{Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Message: "business diagnostic", Context: map[string]string{"ledger": "main"}}}}
	entry, err = Audit(original)
	require.NoError(t, err)
	require.True(t, proto.Equal(original, entry))
	payload, err := (&commonpb.Log{Sequence: 7}).MarshalVT()
	require.NoError(t, err)
	plain := &commonpb.Log{Sequence: 7, ResponseSignature: &signaturepb.SignedLog{Payload: payload, Signature: []byte("original")}}
	log, err = Log(plain)
	require.NoError(t, err)
	require.True(t, proto.Equal(plain, log))
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
		"token-secret",
		"clickhouse-secret",
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
	}, []string{"mirror-oauth-secret", "mirror-postgres-secret"}
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
