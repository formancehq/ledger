package events

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestOutboundEventRedactsCredentialBearingLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		log    *commonpb.Log
		secret string
	}{
		{
			name:   "postgres mirror",
			log:    &commonpb.Log{Sequence: 41, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "orders", MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://user:pg-canary@db.example/ledger"}}}}}}},
			secret: "pg-canary",
		},
		{
			name:   "http mirror",
			log:    &commonpb.Log{Sequence: 42, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "orders", MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source", Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://user:url-canary@source.example/v2", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientId: "client", ClientSecret: "oauth-canary", TokenEndpoint: "https://user:token-canary@auth.example/token"}}}}}}}},
			secret: "oauth-canary",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			original := proto.Clone(tc.log)
			event := LogToEvent(tc.log)
			require.Equal(t, commonpb.EventType_CREATED_LEDGER, event.GetType())
			require.Equal(t, tc.log.GetPayload().GetCreateLedger().GetName(), event.GetLog().GetPayload().GetCreateLedger().GetName())
			mirror := event.GetLog().GetPayload().GetCreateLedger().GetMirrorSource()
			require.NotNil(t, mirror)
			require.Equal(t, "source", mirror.GetLedgerName())
			require.False(t, proto.Equal(tc.log.GetPayload().GetCreateLedger().GetMirrorSource(), mirror))
			if tc.name == "postgres mirror" {
				require.NotNil(t, mirror.GetPostgres())
				require.Contains(t, mirror.GetPostgres().GetDsn(), "db.example/ledger")
			} else {
				require.NotNil(t, mirror.GetHttp())
				require.Contains(t, mirror.GetHttp().GetBaseUrl(), "source.example/v2")
				require.Equal(t, "client", mirror.GetHttp().GetOauth2ClientCredentials().GetClientId())
			}
			for _, format := range []Format{FormatJSON, FormatProto} {
				data, err := SerializeEvent(event, format)
				require.NoError(t, err)
				require.NotContains(t, string(data), tc.secret)
				for _, canary := range []string{"pg-canary", "url-canary", "oauth-canary", "token-canary"} {
					require.False(t, bytes.Contains(data, []byte(canary)), "%s leaked in %s", canary, format)
				}
				var decoded eventspb.Event
				if format == FormatProto {
					require.NoError(t, decoded.UnmarshalVT(data))
					require.Equal(t, tc.log.GetSequence(), decoded.GetLogSequence())
					require.Equal(t, LogToEvent(tc.log).GetType(), decoded.GetType())
				}
			}
			require.True(t, proto.Equal(original, tc.log))
		})
	}
}
