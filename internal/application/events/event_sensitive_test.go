package events

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestOutboundEventRedactsCredentialBearingLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		log    *commonpb.Log
		secret string
		safe   string
	}{
		{
			name:   "postgres mirror",
			log:    &commonpb.Log{Sequence: 41, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "orders", MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://user:pg-canary@db.example/ledger"}}}}}}},
			secret: "pg-canary",
			safe:   "orders",
		},
		{
			name:   "http mirror",
			log:    &commonpb.Log{Sequence: 42, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "orders", MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://user:url-canary@source.example/v2", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientId: "client", ClientSecret: "oauth-canary", TokenEndpoint: "https://user:token-canary@auth.example/token"}}}}}}}},
			secret: "oauth-canary",
			safe:   "orders",
		},
		{
			name:   "sink configuration log",
			log:    &commonpb.Log{Sequence: 43, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: &commonpb.SinkConfig{Name: "outbound", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://user:sink-canary@hooks.example/event", Secret: "hmac-canary"}}}}}}},
			secret: "hmac-canary",
			safe:   "outbound",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, format := range []Format{FormatJSON, FormatProto} {
				data, err := SerializeEvent(LogToEvent(tc.log), format)
				require.NoError(t, err)
				require.NotContains(t, string(data), tc.secret)
				require.Contains(t, string(data), tc.safe)
				for _, canary := range []string{"pg-canary", "url-canary", "oauth-canary", "token-canary", "sink-canary", "hmac-canary"} {
					require.False(t, bytes.Contains(data, []byte(canary)), "%s leaked in %s", canary, format)
				}
				var decoded eventspb.Event
				if format == FormatProto {
					require.NoError(t, decoded.UnmarshalVT(data))
					require.Equal(t, tc.log.GetSequence(), decoded.GetLogSequence())
					require.Equal(t, LogToEvent(tc.log).GetType(), decoded.GetType())
				}
			}
			require.Contains(t, tc.log.String(), tc.secret)
		})
	}
}
