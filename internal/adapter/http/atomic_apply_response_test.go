package http

import (
	"strings"
	"testing"

	jsonv2 "encoding/json/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestAtomicApplyResponseRedactsSinkCredentialsAndSignaturePayload(t *testing.T) {
	t.Parallel()
	const secret = "reusable-credential-value"
	config := &commonpb.SinkConfig{Name: "webhook", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://example.com/events", Secret: secret}}}
	log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config}}}, ResponseSignature: &signaturepb.SignedLog{Payload: []byte(secret)}}
	encoded, err := jsonv2.Marshal(atomicApplyResponse(&servicepb.ApplyResponse{Logs: []*commonpb.Log{log}}))
	require.NoError(t, err)
	require.NotContains(t, string(encoded), secret)
	require.NotContains(t, string(encoded), "responseSignature")
	require.True(t, strings.Contains(string(encoded), "[redacted]"))
	require.Contains(t, string(encoded), "webhook")
	require.Equal(t, secret, log.GetPayload().GetAddedEventsSink().GetConfig().GetHttp().GetSecret(), "response projection must not mutate the committed log")
	require.Equal(t, []byte(secret), log.GetResponseSignature().GetPayload())
}
