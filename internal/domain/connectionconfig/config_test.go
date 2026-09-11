package connectionconfig

import (
	"errors"
	"net"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/pkg/sensitive"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestURLComponentsPreserveRequestBytes(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"https://user:sentinel@host:443/a%2Fb?q=one%20two&q=one+two&flag&blank=&X-Signature=secret", "https://host/path?", "https://host/a%23b?value=%23", "https://[::1]:443/path?z=%2f&a=%2F"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			cfg, err := parseURL(raw, "http")
			require.NoError(t, err)
			require.Equal(t, raw, RenderURL(cfg))
			projected := sensitive.Clone(cfg)
			require.Equal(t, cfg.GetAddress().GetHost(), projected.GetAddress().GetHost())
			require.Equal(t, cfg.GetEscapedPath(), projected.GetEscapedPath())
			require.NotContains(t, RenderURL(projected), "sentinel")
			require.NotContains(t, RenderURL(projected), "X-Signature=secret")
		})
	}
}
func TestDatabaseNormalization(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name, raw string
		pg        bool
	}{
		{"pg uri", "postgres://u:passSentinel@one:5433,two:5434/db?sslmode=require&sslpassword=sslSentinel&application_name=worker", true},
		{"pg keywords", `host=one,two port=5433,5434 user=u password='pass\'Sentinel' dbname='my db' sslmode=require options='-c secret=opaqueSentinel'`, true},
		{"pg socket", "postgres:///db?host=%2Fvar%2Frun%2Fpostgresql&password=passSentinel", true},
		{"pg env omitted", "postgres://host/db", true},
		{"clickhouse", "clickhouse://u:passSentinel@one:9000,two:9000/db?secure=true&password=querySentinel&max_open_conns=2&custom=opaqueSentinel&http_proxy=https%3A%2F%2Fu%3AproxySentinel%40proxy%3Ftoken%3Dx", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg, err := parseDatabase(test.raw, test.pg)
			require.NoError(t, err)
			again, err := parseDatabase(RenderDatabase(cfg), test.pg)
			require.NoError(t, err)
			require.True(t, proto.Equal(cfg, again))
			projected := sensitive.Clone(cfg)
			rendered := RenderDatabase(projected)
			for _, secret := range []string{"passSentinel", "querySentinel", "sslSentinel", "opaqueSentinel", "proxySentinel"} {
				require.NotContains(t, rendered, secret)
			}
			require.Equal(t, cfg.GetDatabase(), projected.GetDatabase())
			require.Equal(t, cfg.GetUsername(), projected.GetUsername())
		})
	}
}
func TestNormalizationDoesNotReadEnvironment(t *testing.T) {
	t.Setenv("PGSERVICE", "nonexistent-sentinel")
	t.Setenv("PGSERVICEFILE", "/nonexistent-service-file")
	t.Setenv("PGPASSWORD", "ambient-secret")
	t.Setenv("PGSSLMODE", "require")
	config, err := parseDatabase("postgres://user@host/db", true)
	require.NoError(t, err)
	require.Nil(t, config.Password)
	require.Empty(t, config.GetOptions())
	require.False(t, PostgresEnforcesTLS(config))
}
func TestNATSAuthenticationVariants(t *testing.T) {
	t.Parallel()
	input := &commonpb.SinkConfigInput{Name: "nats", Type: &commonpb.SinkConfigInput_Nats{Nats: &commonpb.NatsSinkConfigInput{Url: "tokenSentinel@a:4222,nats://user:passwordSentinel@b:4222,tls://user:@c:4222", Topic: "events"}}}
	before := input.CloneVT()
	config, err := Sink(input)
	require.NoError(t, err)
	require.True(t, proto.Equal(before, input))
	servers := config.GetNats().GetServers()
	require.Equal(t, "tokenSentinel", servers[0].GetToken())
	require.Empty(t, servers[0].GetUsername())
	require.Nil(t, servers[0].Password)
	require.Equal(t, "user", servers[1].GetUsername())
	require.Equal(t, "passwordSentinel", servers[1].GetPassword())
	require.NotNil(t, servers[2].Password)
	require.Empty(t, servers[2].GetPassword())
	public := sensitive.Clone(config)
	require.NotContains(t, RenderNATS(public.GetNats()), "tokenSentinel")
	require.NotContains(t, RenderNATS(public.GetNats()), "passwordSentinel")
	require.Equal(t, "events", public.GetNats().GetTopic())
}
func TestIAMUsesNormalizedTLS(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		raw  string
		want bool
	}{
		{"postgres://host/db?sslmode=require", true}, {"host=host dbname=db sslmode=require", true}, {"host=host sslmode=disable application_name='x sslmode=require'", false}, {"postgres://host/db", false}, {"postgres:///db?host=%2Fsocket&sslmode=require", false}, {"postgres://host/db?sslmode=prefer", false},
	} {
		t.Run(test.raw, func(t *testing.T) {
			t.Parallel()
			config, err := parseDatabase(test.raw, true)
			require.NoError(t, err)
			require.Equal(t, test.want, PostgresEnforcesTLS(config))
		})
	}
}
func TestMalformedInputsRejectWithoutEcho(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"postgres://u:secretSentinel%xx@host/db", "password='secretSentinel", "password=secretSentinel\\", "postgres://host/db?password=%xxsecretSentinel"} {
		_, err := parseDatabase(raw, true)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "secretSentinel")
	}
}

func TestPostgresDriverParameterParity(t *testing.T) {
	for _, name := range []string{"PGSERVICE", "PGSERVICEFILE", "PGSSLKEY", "PGSSLCERT", "PGSSLROOTCERT", "PGSSLPASSWORD", "PGSSLNEGOTIATION", "PGTARGETSESSIONATTRS", "PGMINPROTOCOLVERSION", "PGMAXPROTOCOLVERSION", "PGOPTIONS", "PGTZ", "PGAPPNAME"} {
		t.Setenv(name, "")
	}
	t.Setenv("PGHOST", "ambient-host")
	t.Setenv("PGPORT", "5544")
	t.Setenv("PGUSER", "ambient-user")
	t.Setenv("PGDATABASE", "ambient-db")
	t.Setenv("PGPASSWORD", "ambient-pass")
	t.Setenv("PGSSLMODE", "disable")
	t.Setenv("PGPASSFILE", t.TempDir()+"/missing")
	for _, raw := range []string{
		"postgres://u:p@one:5433/db?host=two",
		"postgres://host/",
		"postgres://user@[fe80::1%25eth0]:5432/db?sslmode=disable",
		"postgres://host/db?host=other:5533",
		"host=other:5533 dbname=db",
		"postgres://host///",
		"port=5533 user=u password=p",
		"postgres://?port=5533",
		"host=one user='' dbname='' password=''",
		"postgres://u:p@one:5433/db?user=&dbname=",
		"postgres://u:p@one:5433,two,three:5434/db",
		"postgres://one,two:5433/db",
		"postgres://u:p@[::1]:5433/db?host=2001:db8::1",
		"host=one,two,three port=5433,5434 user=u dbname=db password=p sslmode=require",
		`host=one user=u dbname=db password='quoted \'password' application_name='worker'`,
		`host=one user=u password=escaped\ space`,
		`password='p'host=one user=u`,
		"postgres:///db?host=%2Fvar%2Frun%2Fpostgresql&password=p",
		"postgres://host/db?pool_max_conns=9&pool_min_conns=2&statement_cache_capacity=17&application_name=worker",
	} {
		t.Run(raw, func(t *testing.T) {
			before, err := pgxpool.ParseConfig(raw)
			require.NoError(t, err)
			normalized, err := parseDatabase(raw, true)
			require.NoError(t, err)
			after, err := pgxpool.ParseConfig(RenderDatabase(normalized))
			require.NoError(t, err)
			require.Equal(t, before.ConnConfig.Host, after.ConnConfig.Host)
			require.Equal(t, before.ConnConfig.Port, after.ConnConfig.Port)
			require.Equal(t, before.ConnConfig.User, after.ConnConfig.User)
			require.Equal(t, before.ConnConfig.Password, after.ConnConfig.Password)
			require.Equal(t, before.ConnConfig.Database, after.ConnConfig.Database)
			require.Equal(t, before.ConnConfig.RuntimeParams, after.ConnConfig.RuntimeParams)
			require.Equal(t, before.ConnConfig.TLSConfig == nil, after.ConnConfig.TLSConfig == nil)
			require.Len(t, after.ConnConfig.Fallbacks, len(before.ConnConfig.Fallbacks))
			for i, fb := range before.ConnConfig.Fallbacks {
				require.Equal(t, fb.Host, after.ConnConfig.Fallbacks[i].Host)
				require.Equal(t, fb.Port, after.ConnConfig.Fallbacks[i].Port)
				require.Equal(t, fb.TLSConfig == nil, after.ConnConfig.Fallbacks[i].TLSConfig == nil)
			}
			require.Equal(t, before.MaxConns, after.MaxConns)
			require.Equal(t, before.MinConns, after.MinConns)
			require.Equal(t, before.ConnConfig.StatementCacheCapacity, after.ConnConfig.StatementCacheCapacity)
		})
	}
}

func TestClickHouseDriverParameterParity(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		"tcp://u:p@host:9000/db?max_open_conns=3",
		"clickhouse://host:9000//db",
		"clickhouse://u:p@[fe80::1%25eth0]:9000/db",
		"clickhouse://u:p@one:9000,two:9000/db?secure=true&skip_verify=true&password=query&username=writer&database=other&max_open_conns=9&custom=MixedCase",
		"https://u:p@one:8443/db?secure=true&host=opaque&port=123&user=setting&dbname=setting&http_proxy=http%3A%2F%2Fu%3Ap%40proxy%3A8080",
		"clickhouse://host/db?compress=lz4&compress_level=2&read_timeout=2s&dial_timeout=3s&connection_open_strategy=round_robin&client_info_product=app%2F1.2",
	} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			before, err := clickhouse.ParseDSN(raw)
			require.NoError(t, err)
			normalized, err := parseDatabase(raw, false)
			require.NoError(t, err)
			after, err := clickhouse.ParseDSN(RenderDatabase(normalized))
			require.NoError(t, err)
			require.Equal(t, before.Auth, after.Auth)
			require.Equal(t, before.Addr, after.Addr)
			require.Equal(t, before.Settings, after.Settings)
			require.Equal(t, before.Protocol, after.Protocol)
			require.Equal(t, before.TLS == nil, after.TLS == nil)
			require.Equal(t, before.HTTPProxyURL, after.HTTPProxyURL)
			require.Equal(t, before.MaxOpenConns, after.MaxOpenConns)
			require.Equal(t, before.DialTimeout, after.DialTimeout)
			require.Equal(t, before.ReadTimeout, after.ReadTimeout)
			require.Equal(t, before.Compression, after.Compression)
			require.Equal(t, before.ConnOpenStrategy, after.ConnOpenStrategy)
			require.Equal(t, before.ClientInfo, after.ClientInfo)
		})
	}
}

func TestNormalizationOwnsCopies(t *testing.T) {
	t.Parallel()
	sinks := []*commonpb.SinkConfigInput{
		{Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://u:urlSentinel@host/path?key=querySentinel", Secret: "hmacSentinel"}}},
		{Type: &commonpb.SinkConfigInput_Clickhouse{Clickhouse: &commonpb.ClickHouseSinkConfigInput{Dsn: "clickhouse://u:passwordSentinel@host/db"}}},
		{Type: &commonpb.SinkConfigInput_Kafka{Kafka: &commonpb.KafkaSinkConfig{Brokers: []string{"broker"}, SaslUsername: "u", SaslPassword: "passwordSentinel"}}},
		{Type: &commonpb.SinkConfigInput_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_Token{Token: "tokenSentinel"}}}},
		{Type: &commonpb.SinkConfigInput_Databricks{Databricks: &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "client", ClientSecret: "secretSentinel"}}}}},
	}
	for _, input := range sinks {
		input.Name = "sink"
		input.EventTypes = []commonpb.EventType{commonpb.EventType_COMMITTED_TRANSACTION}
		before := input.CloneVT()
		config, err := Sink(input)
		require.NoError(t, err)
		require.True(t, proto.Equal(input, before))
		public := sensitive.Clone(config)
		require.True(t, proto.Equal(public, sensitive.Clone(public)))
		config.EventTypes[0] = commonpb.EventType(99)
		if config.GetKafka() != nil {
			config.GetKafka().Brokers[0] = "modified"
		}
		if auth := config.GetDatabricks().GetOauthM2M(); auth != nil {
			auth.ClientSecret = "modified"
		}
		require.True(t, proto.Equal(input, before))
	}
	input := &commonpb.MirrorSourceConfigInput{
		LedgerName: "ledger", RewriteRules: []*commonpb.MirrorRewriteRule{{Stop: true}},
		Type: &commonpb.MirrorSourceConfigInput_Http{Http: &commonpb.HttpMirrorSourceConfigInput{
			BaseUrl:                 "https://u:baseSentinel@host/path?key=querySentinel",
			Oauth2ClientCredentials: &commonpb.OAuth2ClientCredentialsInput{ClientId: "client", ClientSecret: "clientSentinel", TokenEndpoint: "https://u:tokenSentinel@auth/token?key=authSentinel", Scopes: []string{"read"}},
		}},
	}
	before := input.CloneVT()
	config, err := Mirror(input)
	require.NoError(t, err)
	require.True(t, proto.Equal(input, before))
	public := sensitive.Clone(config)
	require.True(t, proto.Equal(public, sensitive.Clone(public)))
	config.GetHttp().GetOauth2ClientCredentials().Scopes[0] = "modified"
	config.RewriteRules[0].Stop = false
	require.True(t, proto.Equal(input, before))
}

func TestNATSImplicitWebSocketScheme(t *testing.T) {
	t.Parallel()
	config, err := Sink(&commonpb.SinkConfigInput{Type: &commonpb.SinkConfigInput_Nats{Nats: &commonpb.NatsSinkConfigInput{Url: "wss://first:443,second:80"}}})
	require.NoError(t, err)
	require.Equal(t, "wss://first:443,ws://second:80", RenderNATS(config.GetNats()))
}

// refusingDialer exercises the pinned driver's server parser without networking.
type refusingDialer struct{ addresses []string }

func (d *refusingDialer) Dial(_ string, address string) (net.Conn, error) {
	d.addresses = append(d.addresses, address)

	return nil, errors.New("connection parity probe refused dial")
}
func TestNATSDriverServerParity(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"", ", ,", "native://first:4222", "wss://first:443,second:80", "ws://first:80,second:80", "tls://first:4222,second:4223", "tokenSentinel@first:4222,nats://u:passwordSentinel@second:4223"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			config, err := Sink(&commonpb.SinkConfigInput{Type: &commonpb.SinkConfigInput_Nats{Nats: &commonpb.NatsSinkConfigInput{Url: raw}}})
			require.NoError(t, err)
			before, after := &refusingDialer{}, &refusingDialer{}
			for _, input := range []struct {
				url    string
				dialer *refusingDialer
			}{{raw, before}, {RenderNATS(config.GetNats()), after}} {
				conn, err := nats.Connect(input.url, nats.DontRandomize(), nats.MaxReconnects(0), nats.SetCustomDialer(input.dialer))
				if conn != nil {
					conn.Close()
				}
				require.ErrorContains(t, err, "connection parity probe refused dial")
			}
			require.NotEmpty(t, before.addresses)
			require.Equal(t, before.addresses, after.addresses)
		})
	}
}

func TestURLFragmentsAreRejected(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"http", "nats", "proxy"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			for _, fragment := range []string{"#fragment-secret", "#"} {
				config, err := parseURL("https://user:password@host/path"+fragment, kind)
				require.EqualError(t, err, "connection URL fragments are unsupported")
				require.Nil(t, config)
			}
		})
	}
}

func TestNATSEmptyHostIsRejected(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{"nats://", "tls://", "ws://", " nats:// ", "nats://,nats://second:4223"} {
		t.Run(raw, func(t *testing.T) {
			t.Parallel()
			config, err := Sink(&commonpb.SinkConfigInput{Type: &commonpb.SinkConfigInput_Nats{Nats: &commonpb.NatsSinkConfigInput{Url: raw}}})
			require.EqualError(t, err, "invalid connection URL")
			require.Nil(t, config)
		})
	}
}

func TestPostgresTrailingUnportedHostRejectedLikeDriver(t *testing.T) {
	t.Parallel()
	const raw = "postgres://one:5433,two/db"
	_, err := pgxpool.ParseConfig(raw)
	require.Error(t, err)
	_, err = parseDatabase(raw, true)
	require.Error(t, err)
}
