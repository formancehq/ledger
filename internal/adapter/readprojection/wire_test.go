package readprojection

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestCredentialWireRejectsShadowedRequestVariants(t *testing.T) {
	t.Parallel()
	sinks, _ := secretBearingSinkConfigs()
	mirrors, _ := secretBearingMirrorSources()
	var requests []*servicepb.Request
	for _, sink := range sinks {
		requests = append(requests, &servicepb.Request{Type: &servicepb.Request_AddEventsSink{
			AddEventsSink: &servicepb.AddEventsSinkRequest{Config: sink},
		}})
	}
	for _, mirror := range mirrors {
		requests = append(requests, &servicepb.Request{Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{MirrorSource: mirror},
		}})
	}
	last := wireMarshal(t, &servicepb.Request{Type: &servicepb.Request_RemoveEventsSink{
		RemoveEventsSink: &servicepb.RemoveEventsSinkRequest{Name: "sink"},
	}})
	for _, request := range requests {
		first := wireMarshal(t, request)
		payload := wireMessage(1, append(first, last...))
		original := bytes.Clone(payload)
		decoded := &servicepb.ApplyBatch{}
		require.NoError(t, decoded.UnmarshalVT(payload), "fixture must be accepted protobuf")
		require.NotNil(t, decoded.GetRequests()[0].GetRemoveEventsSink(), "prove secret branch is shadowed")
		require.ErrorContains(t, validateCredentialWire(payload, &servicepb.ApplyBatch{}), "shadowed credential")
		result, err := Audit(&auditpb.AuditEntry{Signature: &signaturepb.SignedApplyBatch{Payload: payload}})
		require.ErrorContains(t, err, "shadowed credential")
		require.Nil(t, result, "public projection must not return shadowed bytes")
		require.Equal(t, original, payload, "wire validation must preserve authoritative bytes")
	}
}

func TestCredentialWireRejectsShadowedScalarAndConfigOneof(t *testing.T) {
	t.Parallel()

	// A repeated secret with an empty final value is invisible after decoding.
	doubledSecret := append(wireString(2, "hidden-webhook-key"), wireString(2, "")...)
	sink := wireMessage(5, doubledSecret) // SinkConfig.http
	decodedSink := &commonpb.SinkConfig{}
	require.NoError(t, decodedSink.UnmarshalVT(sink))
	require.Empty(t, decodedSink.GetHttp().GetSecret())
	require.ErrorContains(t, validateCredentialWire(sink, decodedSink), "shadowed credential")

	// The same shape through MirrorSourceConfig.http.oauth2_client_credentials.
	oauth := append(wireString(2, "hidden-oauth-key"), wireString(2, "")...)
	mirror := wireMessage(2, wireMessage(2, oauth))
	decodedMirror := &commonpb.MirrorSourceConfig{}
	require.NoError(t, decodedMirror.UnmarshalVT(mirror))
	require.Empty(t, decodedMirror.GetHttp().GetOauth2ClientCredentials().GetClientSecret())
	require.ErrorContains(t, validateCredentialWire(mirror, decodedMirror), "shadowed credential")

	// A different config oneof arm can hide an entire credential subtree.
	sink = append(wireMessage(5, wireString(2, "hidden-webhook-key")), wireMessage(4, nil)...)
	require.NoError(t, decodedSink.UnmarshalVT(sink))
	require.NotNil(t, decodedSink.GetKafka())
	require.ErrorContains(t, validateCredentialWire(sink, decodedSink), "shadowed credential")

	// And a parent singular message can carry the hidden occurrence.
	firstHTTP := wireMessage(2, wireString(2, "hidden-oauth-key"))
	lastHTTP := wireMessage(2, wireString(2, ""))
	mirror = append(wireMessage(2, firstHTTP), wireMessage(2, lastHTTP)...)
	require.ErrorContains(t, validateCredentialWire(mirror, &commonpb.MirrorSourceConfig{}), "shadowed credential")
}

func TestCredentialWireCoversOrdersAndSignedLogPayloads(t *testing.T) {
	t.Parallel()
	config := &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Http{
		Http: &commonpb.HttpSinkConfig{Secret: "hidden-key"},
	}}
	order := wireMarshal(t, addSinkOrder(config))
	// Order.system_scoped followed by Order.ledger_scoped loses the system arm.
	order = append(order, wireMessage(1, wireString(1, "ledger"))...)
	require.ErrorContains(t, validateCredentialWire(order, &raftcmdpb.Order{}), "shadowed credential")
	entry, err := Audit(&auditpb.AuditEntry{Items: []*auditpb.AuditItem{{SerializedOrder: order}}})
	require.ErrorContains(t, err, "shadowed credential")
	require.Nil(t, entry)

	logPayload := append(wireMessage(7, wireMessage(1, wireMarshal(t, config))), wireMessage(2, nil)...)
	log := wireMessage(2, logPayload)
	decoded := &commonpb.Log{}
	require.NoError(t, decoded.UnmarshalVT(log))
	require.NotNil(t, decoded.GetPayload().GetDeleteLedger())
	require.ErrorContains(t, validateCredentialWire(log, decoded), "shadowed credential")
	projected, err := Log(&commonpb.Log{ResponseSignature: &signaturepb.SignedLog{Payload: log}})
	require.ErrorContains(t, err, "shadowed credential")
	require.Nil(t, projected)
}

func TestCredentialWirePreservesUnrelatedDuplicateFields(t *testing.T) {
	t.Parallel()
	for _, secret := range []string{"", "visible-key"} {
		t.Run(secret, func(t *testing.T) {
			t.Parallel()
			// Duplicate endpoint-free name fields do not obscure the secret.
			sink := append(wireString(1, "first"), wireString(1, "last")...)
			sink = append(sink, wireMessage(5, wireString(2, secret))...)
			request := wireMessage(7, wireMessage(1, sink))
			payload := wireMessage(1, request)
			// Duplicate unrelated batch identity also remains supported.
			payload = append(payload, wireString(2, "first-id")...)
			payload = append(payload, wireString(2, "last-id")...)
			original := bytes.Clone(payload)
			require.NoError(t, validateCredentialWire(payload, &servicepb.ApplyBatch{}))
			require.Equal(t, original, payload)
		})
	}
	// Even duplicate request oneofs are safe when neither arm has credentials.
	first := wireMessage(7, wireMessage(1, wireMessage(5, nil)))
	last := wireMessage(8, wireString(1, "sink"))
	require.NoError(t, validateCredentialWire(wireMessage(1, append(first, last...)), &servicepb.ApplyBatch{}))
	unchanged := &auditpb.AuditEntry{Signature: &signaturepb.SignedApplyBatch{Payload: wireMessage(1, append(first, last...)), Signature: []byte("unchanged-proof")}}
	projected, err := Audit(unchanged)
	require.NoError(t, err)
	require.True(t, proto.Equal(unchanged, projected), "non-secret duplicate encodings retain exact evidence")

	// Repeated requests are legitimate, including credential-bearing requests.
	request := wireMessage(1, wireMessage(7, wireMessage(1, wireMessage(5, wireString(2, "key")))))
	require.NoError(t, validateCredentialWire(append(bytes.Clone(request), request...), &servicepb.ApplyBatch{}))
}

func TestCredentialWireRejectsMalformedContainersWithoutEchoingBytes(t *testing.T) {
	t.Parallel()
	for _, payload := range [][]byte{{0xff}, {0x0a, 0xff}, wireMessage(1, []byte{0xff})} {
		require.Error(t, validateCredentialWire(payload, &servicepb.ApplyBatch{}))
	}
}

func wireMarshal(t *testing.T, message proto.Message) []byte {
	t.Helper()
	raw, err := proto.Marshal(message)
	require.NoError(t, err)

	return raw
}

func wireMessage(number protowire.Number, body []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), body)
}

func wireString(number protowire.Number, value string) []byte {
	return wireMessage(number, []byte(value))
}
