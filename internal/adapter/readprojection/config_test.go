package readprojection

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestSink(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		config  *commonpb.SinkConfig
		secrets []string
	}{
		{"nil", nil, nil},
		{"no variant", &commonpb.SinkConfig{Name: "unconfigured"}, nil},
		{"nats", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Nats{Nats: &commonpb.NatsSinkConfig{Url: "nats://natsTokenSentinel@one:4222, user:natsPasswordSentinel@two:4222", Topic: "events"}}}, []string{"natsTokenSentinel", "natsPasswordSentinel"}},
		{"clickhouse", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfig{Dsn: "clickhouse://reporter:clickhousePasswordSentinel@db:9000/events?password=clickhouseQuerySentinel&http_proxy=https%3A%2F%2FproxyUser%3AproxyPasswordSentinel%40proxy%3Fapi_key%3DproxyQuerySentinel&secure=true", Table: "events"}}}, []string{"clickhousePasswordSentinel", "clickhouseQuerySentinel", "proxyPasswordSentinel", "proxyQuerySentinel"}},
		{"kafka", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{Brokers: []string{"broker:9092"}, Topic: "events", Tls: true, SaslMechanism: "PLAIN", SaslUsername: "writer", SaslPassword: "kafkaPasswordSentinel"}}}, []string{"kafkaPasswordSentinel"}},
		{"http", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://user:httpPasswordSentinel@webhook/path?unusual=httpQuerySentinel", Secret: "hmacSecretSentinel"}}}, []string{"httpPasswordSentinel", "httpQuerySentinel", "hmacSecretSentinel"}},
		{"databricks pat", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{ServerHostname: "warehouse", HttpPath: "/sql/warehouse", Catalog: "catalog", Schema: "schema", Table: "events", Port: 443, Auth: &commonpb.DatabricksSinkConfig_Token{Token: "patSecretSentinel"}}}}, []string{"patSecretSentinel"}},
		{"databricks oauth", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "client", ClientSecret: "databricksOAuthSentinel"}}}}}, []string{"databricksOAuthSentinel"}},
		{"nil nats", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Nats{}}, nil},
		{"nil clickhouse", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Clickhouse{}}, nil},
		{"nil kafka", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Kafka{}}, nil},
		{"nil http", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Http{}}, nil},
		{"nil databricks", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Databricks{}}, nil},
		{"nil oauth", &commonpb.SinkConfig{Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{}}}}, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.config != nil {
				test.config.Name = "named-sink"
				test.config.Format = "protobuf"
				test.config.BatchSize = 12
				test.config.BatchDelayMs = 34
				test.config.EventTypes = []commonpb.EventType{commonpb.EventType_COMMITTED_TRANSACTION}
			}
			before := test.config.CloneVT()
			out := Sink(test.config)
			require.True(t, proto.Equal(before, test.config), "input must remain authoritative")
			require.True(t, proto.Equal(out, Sink(out)), "projection must be a fixed point")
			assertNoSecrets(t, out, test.secrets)
			if out != nil {
				require.Equal(t, test.config.GetName(), out.GetName())
				require.Equal(t, test.config.GetFormat(), out.GetFormat())
				require.Equal(t, test.config.GetBatchSize(), out.GetBatchSize())
				require.Equal(t, test.config.GetBatchDelayMs(), out.GetBatchDelayMs())
				require.Equal(t, test.config.GetEventTypes(), out.GetEventTypes())
				out.EventTypes[0] = commonpb.EventType_EVENT_TYPE_UNSPECIFIED
				require.True(t, proto.Equal(before, test.config), "slices must be independently cloned")
			}
			projected := before.CloneVT()
			require.Equal(t, len(test.secrets) != 0, redactSink(projected))
			require.False(t, redactSink(projected), "already redacted data must leave signed bytes intact")
		})
	}
}

func TestMirror(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		config  *commonpb.MirrorSourceConfig
		secrets []string
	}{
		{"nil", nil, nil},
		{"empty", &commonpb.MirrorSourceConfig{}, nil},
		{"nil http", &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Http{}}, nil},
		{"nil postgres", &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{}}, nil},
		{"http", &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://user:mirrorBasicSentinel@source/v2?key=baseQuerySentinel", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientId: "client", ClientSecret: "mirrorOAuthSentinel", TokenEndpoint: "https://tokenUser:tokenPasswordSentinel@auth/token?arbitrary=tokenQuerySentinel", Scopes: []string{"ledger:read"}}}}}, []string{"mirrorBasicSentinel", "baseQuerySentinel", "mirrorOAuthSentinel", "tokenPasswordSentinel", "tokenQuerySentinel"}},
		{"postgres URI", &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://reader:pgPasswordSentinel@database:5432/ledger?sslmode=require&password=pgQuerySentinel&sslpassword=tlsPasswordSentinel", AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1", AssumeRoleArn: "role"}}}}, []string{"pgPasswordSentinel", "pgQuerySentinel", "tlsPasswordSentinel"}},
		{"postgres keyword", &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "host=database port=5432 dbname=ledger user=reader password='pgKeywordSentinel' sslmode=require"}}}, []string{"pgKeywordSentinel"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if test.config != nil {
				test.config.LedgerName = "source-ledger"
				test.config.BatchSize = 77
			}
			before := test.config.CloneVT()
			out := Mirror(test.config)
			require.True(t, proto.Equal(before, test.config))
			require.True(t, proto.Equal(out, Mirror(out)))
			assertNoSecrets(t, out, test.secrets)
			if out != nil {
				require.Equal(t, "source-ledger", out.GetLedgerName())
				require.EqualValues(t, 77, out.GetBatchSize())
				if pg := out.GetPostgres(); pg != nil {
					require.True(t, proto.Equal(before.GetPostgres().GetAwsIamAuth(), pg.GetAwsIamAuth()))
				}
				if http := out.GetHttp(); http != nil && http.GetOauth2ClientCredentials() != nil {
					require.Equal(t, "client", http.GetOauth2ClientCredentials().GetClientId())
					require.Equal(t, []string{"ledger:read"}, http.GetOauth2ClientCredentials().GetScopes())
					http.Oauth2ClientCredentials.Scopes[0] = "changed"
					require.True(t, proto.Equal(before, test.config))
				}
			}
			projected := before.CloneVT()
			require.Equal(t, len(test.secrets) != 0, redactMirror(projected))
			require.False(t, redactMirror(projected))
		})
	}
}

func TestLedgerAndSinkStatus(t *testing.T) {
	t.Parallel()
	require.Nil(t, Ledger(nil))
	require.Nil(t, SinkStatus(nil))
	for _, message := range []string{"", "transport error includes diagnosticSecretSentinel", redactedDiagnostic} {
		t.Run(message, func(t *testing.T) {
			t.Parallel()
			info := &commonpb.LedgerInfo{
				Name: "mirror", Id: 42,
				Metadata:           map[string]*commonpb.MetadataValue{"label": {Type: &commonpb.MetadataValue_StringValue{StringValue: "keep"}}},
				MirrorSyncProgress: &commonpb.MirrorSyncProgress{State: commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING, Cursor: 4, SourceLogCount: 7, RemainingLogs: 3, Error: &commonpb.MirrorSyncError{Message: message, OccurredAt: &commonpb.Timestamp{Data: 17}}},
				MirrorSource:       &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://user:ledgerSecretSentinel@host/db"}}},
			}
			before := info.CloneVT()
			out := Ledger(info)
			require.True(t, proto.Equal(info, before))
			require.Equal(t, redactedDiagnostic, out.GetMirrorSyncProgress().GetError().GetMessage())
			require.EqualValues(t, 17, out.GetMirrorSyncProgress().GetError().GetOccurredAt().GetData())
			require.Equal(t, before.GetMirrorSyncProgress().GetState(), out.GetMirrorSyncProgress().GetState())
			require.Equal(t, before.GetMirrorSyncProgress().GetCursor(), out.GetMirrorSyncProgress().GetCursor())
			require.Equal(t, before.GetMirrorSyncProgress().GetSourceLogCount(), out.GetMirrorSyncProgress().GetSourceLogCount())
			require.Equal(t, before.GetMirrorSyncProgress().GetRemainingLogs(), out.GetMirrorSyncProgress().GetRemainingLogs())
			require.Len(t, out.GetMetadata(), len(before.GetMetadata()))
			for key, value := range before.GetMetadata() {
				require.True(t, proto.Equal(value, out.GetMetadata()[key]), key)
			}
			require.Equal(t, before.GetName(), out.GetName())
			require.Equal(t, before.GetId(), out.GetId())
			assertNoSecrets(t, out, []string{"ledgerSecretSentinel", "diagnosticSecretSentinel"})
			require.True(t, proto.Equal(out, Ledger(out)))
			out.Metadata["label"].Type = &commonpb.MetadataValue_StringValue{StringValue: "changed"}
			out.MirrorSyncProgress.Error.OccurredAt.Data = 99
			require.True(t, proto.Equal(before, info))
			status := &commonpb.SinkStatus{SinkName: "sink", Cursor: 33, Error: &commonpb.SinkError{Message: message, OccurredAt: &commonpb.Timestamp{Data: 17}}}
			statusBefore := status.CloneVT()
			projected := SinkStatus(status)
			require.Equal(t, redactedDiagnostic, projected.GetError().GetMessage())
			require.EqualValues(t, 17, projected.GetError().GetOccurredAt().GetData())
			require.EqualValues(t, 33, projected.GetCursor())
			require.Equal(t, "sink", projected.GetSinkName())
			require.True(t, proto.Equal(projected, SinkStatus(projected)))
			projected.Error.OccurredAt.Data = 99
			require.True(t, proto.Equal(status, statusBefore))
		})
	}
	require.Nil(t, Ledger(&commonpb.LedgerInfo{}).GetMirrorSyncProgress())
	require.Nil(t, Ledger(&commonpb.LedgerInfo{MirrorSyncProgress: &commonpb.MirrorSyncProgress{}}).GetMirrorSyncProgress().GetError())
	require.Nil(t, SinkStatus(&commonpb.SinkStatus{}).GetError())
}

func TestRedactURL(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		kind urlKind
		raw  string
		want string
	}{
		{"empty", urlHTTP, "", ""},
		{"marker", urlHTTP, "[redacted]", "[redacted]"},
		{"unchanged exact URL", urlHTTP, "https://host/a%2Fb?empty&another=", "https://host/a%2Fb?empty&another="},
		{"HTTP password", urlHTTP, "https://user:secret@host/path", "https://%5Bredacted%5D@host/path"},
		{"HTTP token", urlHTTP, "https://token@host/path", "https://%5Bredacted%5D@host/path"},
		{"HTTP empty userinfo", urlHTTP, "https://:@host/path", "https://:@host/path"},
		{"HTTP arbitrary queries", urlHTTP, "https://host/path?k=first&k=second&empty=&other=third", "https://host/path?empty=&k=%5Bredacted%5D&k=%5Bredacted%5D&other=%5Bredacted%5D"},
		{"postgres userinfo", urlPostgres, "postgres://reader:secret@host:5432/ledger?sslmode=require", "postgres://reader:%5Bredacted%5D@host:5432/ledger?sslmode=require"},
		{"postgres query", urlPostgres, "postgresql://reader@host/ledger?password=one&password=two&sslpassword=three&sslmode=require", "postgresql://reader@host/ledger?password=%5Bredacted%5D&password=%5Bredacted%5D&sslmode=require&sslpassword=%5Bredacted%5D"},
		{"postgres Unix socket", urlPostgres, "postgres:///ledger?host=%2Fvar%2Frun%2Fpostgresql&password=secret&sslmode=disable", "postgres:///ledger?host=%2Fvar%2Frun%2Fpostgresql&password=%5Bredacted%5D&sslmode=disable"},
		{"postgres multiple hosts", urlPostgres, "postgres://reader:secret@host1:5432,host2:5433/ledger?options=-c+search_path%3Dledger&sslmode=require", "postgres://reader:%5Bredacted%5D@host1:5432,host2:5433/ledger?options=-c+search_path%3Dledger&sslmode=require"},
		{"postgres empty password", urlPostgres, "postgres://reader:@host/db?password=&sslpassword=", "postgres://reader:@host/db?password=&sslpassword="},
		{"unchanged DB operational query", urlPostgres, "postgres://reader@host/db?sslmode=require&sslkey=%2Fkey&connect_timeout=10", "postgres://reader@host/db?sslmode=require&sslkey=%2Fkey&connect_timeout=10"},
		{"clickhouse userinfo", urlClickHouse, "clickhouse://writer:secret@host/db?secure=true", "clickhouse://writer:%5Bredacted%5D@host/db?secure=true"},
		{"clickhouse query", urlClickHouse, "clickhouse://host/db?password=one&secure=true", "clickhouse://host/db?password=%5Bredacted%5D&secure=true"},
		{"invalid escape", urlHTTP, "https://user:%zz@host", "[redacted]"},
		{"invalid query", urlHTTP, "https://host?key=%xx", "[redacted]"},
		{"invalid query semicolon", urlHTTP, "https://host?key=one;other=two", "[redacted]"},
		{"opaque URL", urlHTTP, "https:secret", "[redacted]"},
		{"missing scheme", urlHTTP, "user:secret@host", "[redacted]"},
		{"unsupported scheme", urlHTTP, "ftp://user:secret@host", "[redacted]"},
		{"fragment", urlHTTP, "https://host/path#secret", "https://host/path#%5Bredacted%5D"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := redactURL(test.raw, test.kind)
			require.Equal(t, test.want, got)
			require.Equal(t, got, redactURL(got, test.kind))
		})
	}
}

func TestClickHouseProxy(t *testing.T) {
	t.Parallel()
	for _, proxy := range []string{"https://user:proxySentinel@proxy:8080/path?custom=querySentinel", "https://proxy/path?token=querySentinel", "socks5://user:proxySentinel@proxy:1080", "socks5h://user:proxySentinel@proxy:1080", "invalid proxySentinel"} {
		t.Run(proxy, func(t *testing.T) {
			t.Parallel()
			raw := "clickhouse://reader@db/events?http_proxy=" + url.QueryEscape(proxy) + "&secure=true"
			got := redactURL(raw, urlClickHouse)
			require.NotContains(t, got, "proxySentinel")
			require.NotContains(t, got, "querySentinel")
			parsed, err := url.Parse(got)
			require.NoError(t, err)
			require.Equal(t, "db", parsed.Host)
			require.Equal(t, "/events", parsed.Path)
			require.Equal(t, "true", parsed.Query().Get("secure"))
			require.Equal(t, got, redactURL(got, urlClickHouse))
		})
	}
}

func TestRedactNATS(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ raw, want string }{
		{"", ""},
		{"[redacted]", "[redacted]"},
		{"nats://host:4222, other:4222", "nats://host:4222, other:4222"},
		{"nats://token@host:4222", "nats://%5Bredacted%5D@host:4222"},
		{"token@host:4222", "%5Bredacted%5D@host:4222"},
		{"user:password@host:4222", "%5Bredacted%5D@host:4222"},
		{"nats://user:password@one:4222, token@two:4222 ,tls://user:password@three:4222", "nats://%5Bredacted%5D@one:4222, %5Bredacted%5D@two:4222 ,tls://%5Bredacted%5D@three:4222"},
		{"wss://user:password@host/path?token=value", "wss://%5Bredacted%5D@host/path?token=%5Bredacted%5D"},
		{"nats://host:4222,", "[redacted]"},
		{"nats://user:%zz@host:4222", "[redacted]"},
	} {
		t.Run(test.raw, func(t *testing.T) {
			t.Parallel()
			got := redactNATS(test.raw)
			require.Equal(t, test.want, got)
			require.Equal(t, got, redactNATS(got))
		})
	}
}

func TestRedactPostgresKeywords(t *testing.T) {
	t.Parallel()
	for _, test := range []struct{ raw, want string }{
		{"", ""},
		{"[redacted]", "[redacted]"},
		{"host=database dbname=ledger user=reader sslmode=require", "host=database dbname=ledger user=reader sslmode=require"},
		{"host=database password=secret   \t\n", "host=database password=[redacted]   \t\n"},
		{"host=database dbname=ledger   \t", "host=database dbname=ledger   \t"},
		{"host = database password = secret dbname = ledger", "host = database password = [redacted] dbname = ledger"},
		{"host=database password='with spaces' dbname='ledger space'", "host=database password='[redacted]' dbname='ledger space'"},
		{`host=database password='with \'quote and \\slash' dbname=ledger`, `host=database password='[redacted]' dbname=ledger`},
		{`host=database password=escaped\ space dbname=ledger`, `host=database password=[redacted] dbname=ledger`},
		{"password=one\tsslpassword='two'\npassword=three", "password=[redacted]\tsslpassword='[redacted]'\npassword=[redacted]"},
		{"password='' host=database sslpassword=", "password='' host=database sslpassword="},
		{"password='[redacted]' host=database", "password='[redacted]' host=database"},
		{"password=[redacted] host=database", "password=[redacted] host=database"},
		{"password='one'host=database", "password='[redacted]'host=database"},
		{"host=database password='unterminated", "[redacted]"},
		{"host=database password=trailing\\", "[redacted]"},
		{"host=database password='trailing\\", "[redacted]"},
		{"host=database password=secret invalid", "[redacted]"},
		{"=secret", "[redacted]"},
		{"unknown://user:secret@host", "[redacted]"},
		{"https://host?password=secret", "[redacted]"},
	} {
		t.Run(test.raw, func(t *testing.T) {
			t.Parallel()
			got := redactPostgresKeywords(test.raw)
			require.Equal(t, test.want, got)
			require.Equal(t, got, redactPostgresKeywords(got))
		})
	}
}

func TestEmptySecretsPreserveBytes(t *testing.T) {
	t.Parallel()
	for _, config := range []*commonpb.SinkConfig{
		{Type: &commonpb.SinkConfig_Kafka{Kafka: &commonpb.KafkaSinkConfig{SaslUsername: "writer"}}},
		{Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://host/path?empty="}}},
		{Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_Token{}}}},
		{Type: &commonpb.SinkConfig_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "client"}}}}},
	} {
		before, err := proto.Marshal(config)
		require.NoError(t, err)
		require.False(t, redactSink(config))
		after, err := proto.Marshal(config)
		require.NoError(t, err)
		require.Equal(t, before, after)
	}
	mirror := &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Http{Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "https://host", Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentials{ClientId: "client", TokenEndpoint: "https://host/token"}}}}
	require.False(t, redactMirror(mirror))
}

func TestConfigVariantCoverage(t *testing.T) {
	t.Parallel()
	// Changing a sensitive union requires deliberately extending this package's
	// redactor and fixtures, rather than accidentally introducing a fail-open arm.
	for _, test := range []struct {
		message proto.Message
		oneof   string
		fields  []string
	}{
		{&commonpb.SinkConfig{}, "type", []string{"nats", "clickhouse", "kafka", "http", "databricks"}},
		{&commonpb.DatabricksSinkConfig{}, "auth", []string{"token", "oauth_m2m"}},
		{&commonpb.MirrorSourceConfig{}, "type", []string{"http", "postgres"}},
	} {
		fields := test.message.ProtoReflect().Descriptor().Oneofs().Get(0).Fields()
		var names []string
		for i := range fields.Len() {
			names = append(names, string(fields.Get(i).Name()))
		}
		require.Equal(t, test.fields, names, test.oneof)
	}
}

func assertNoSecrets(t *testing.T, message proto.Message, secrets []string) {
	t.Helper()
	wire, err := proto.Marshal(message)
	require.NoError(t, err)
	json, err := protojson.Marshal(message)
	require.NoError(t, err)
	for _, secret := range secrets {
		require.NotContains(t, string(wire), secret)
		require.NotContains(t, string(json), secret)
	}
}
