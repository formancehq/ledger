package sensitive

import (
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Exercise real generated field declarations through the public projection.
func projectConnection(t *testing.T, name protoreflect.FullName, raw string) string {
	t.Helper()
	descriptor, err := protoregistry.GlobalFiles.FindDescriptorByName(name)
	require.NoError(t, err)
	field := descriptor.(protoreflect.FieldDescriptor)
	source := dynamicpb.NewMessage(field.ContainingMessage())
	source.Set(field, protoreflect.ValueOfString(raw))
	original := proto.Clone(source)
	view := Redact(source)
	require.True(t, proto.Equal(original, source))
	for _, marshal := range []func(proto.Message) ([]byte, error){proto.Marshal, protojson.Marshal} {
		data, err := marshal(view)
		require.NoError(t, err)
		require.NotContains(t, string(data), "canary")
	}

	return view.Get(field).String()
}

func TestRedactSelectsConnectionFormatByField(t *testing.T) {
	t.Parallel()
	raw := "host=db password=canary dbname=ledger"
	postgres := &commonpb.PostgresMirrorSourceConfig{Dsn: raw}
	require.Equal(t, "host=db password='xxxxx' dbname=ledger", Redact(postgres).GetDsn())
	require.Equal(t, raw, postgres.GetDsn())
	for _, source := range []proto.Message{
		&commonpb.HttpSinkConfig{Endpoint: raw},
		&commonpb.HttpMirrorSourceConfig{BaseUrl: raw},
		&commonpb.OAuth2ClientCredentials{TokenEndpoint: raw},
		&commonpb.NatsSinkConfig{Url: raw},
		&commonpb.ClickHouseSinkConfig{Dsn: raw},
	} {
		original := proto.Clone(source)
		view := Redact(source).ProtoReflect()
		source.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
			require.Equal(t, Marker, view.Get(field).String())

			return true
		})
		require.True(t, proto.Equal(original, source))
	}
}

func TestRedactConnectionFormats(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		field     protoreflect.FullName
		raw, want string
	}{
		{"postgres socket", "common.PostgresMirrorSourceConfig.dsn", "postgresql:///ledger?host=%2Ftmp&password=canary&sslmode=disable", "postgresql:///ledger?host=%2Ftmp&password=xxxxx&sslmode=disable"},
		{"postgres username", "common.PostgresMirrorSourceConfig.dsn", "postgres://alice@db/ledger", "postgres://alice@db/ledger"},
		{"keyword duplicates and escapes", "common.PostgresMirrorSourceConfig.dsn", `host=db password='first canary' password=second\ canary sslpassword='third\'canary' dbname=ledger`, "host=db password='xxxxx'" + " password='xxxxx' sslpassword='xxxxx' dbname=ledger"},
		{"keyword malformed", "common.PostgresMirrorSourceConfig.dsn", "host=db password='canary", Marker},
		{"postgres opaque", "common.PostgresMirrorSourceConfig.dsn", "postgres:canary", Marker},
		{"postgres opaque with assignment", "common.PostgresMirrorSourceConfig.dsn", "postgres:password=canary", Marker},
		{"unsupported opaque with assignment", "common.PostgresMirrorSourceConfig.dsn", "https:password=canary", Marker},
		{"postgres rejects HTTP", "common.PostgresMirrorSourceConfig.dsn", "https://alice:canary@db/ledger", Marker},
		{"mixed NATS list", "common.NatsSinkConfig.url", "nats://canary@one,tls://alice:canary@two,wss://canary@three/path", "nats://xxxxx@one,tls://alice:xxxxx@two,wss://xxxxx@three/path"},
		{"partial NATS list", "common.NatsSinkConfig.url", "nats://canary@one,not a URL canary", "nats://xxxxx@one,[redacted]"},
		{"NATS rejects HTTP", "common.NatsSinkConfig.url", "https://canary@host", Marker},
		{"ClickHouse HTTP", "common.ClickHouseSinkConfig.dsn", "HTTPS://canary@db:8443/ledger?compress=true", "https://xxxxx@db:8443/ledger?compress=true"},
		{"ClickHouse username", "common.ClickHouseSinkConfig.dsn", "clickhouse://alice@db:9000/ledger", "clickhouse://alice@db:9000/ledger"},
		{"ClickHouse proxy token", "common.ClickHouseSinkConfig.dsn", "clickhouse://one:9000,two:9000/db?http_proxy=https%3A%2F%2Fcanary%40proxy%2F&compress=true", "clickhouse://one:9000,two:9000/db?compress=true&http_proxy=https%3A%2F%2Fxxxxx%40proxy%2F"},
		{"unsupported proxy", "common.ClickHouseSinkConfig.dsn", "clickhouse://db/ledger?http_proxy=nats%3A%2F%2Fcanary%40proxy", "clickhouse://db/ledger?http_proxy=xxxxx"},
		{"HTTP uppercase", "common.HttpSinkConfig.endpoint", "HTTP://canary@host/path?region=eu&token=canary", "http://xxxxx@host/path?region=eu&token=xxxxx"},
		{"HTTP source", "common.HttpMirrorSourceConfig.base_url", "https://alice:canary@host/path?region=eu", "https://alice:xxxxx@host/path?region=eu"},
		{"OAuth endpoint", "common.OAuth2ClientCredentials.token_endpoint", "https://canary@host/token", "https://xxxxx@host/token"},
		{"HTTP rejects NATS", "common.HttpSinkConfig.endpoint", "nats://canary@host/path", Marker},
		{"invalid port", "common.HttpSinkConfig.endpoint", "https://alice:canary@host:bad/path", Marker},
		{"invalid escape", "common.HttpSinkConfig.endpoint", "https://alice:canary@host/%zz", Marker},
		{"malformed query", "common.HttpSinkConfig.endpoint", "https://host/path?region=eu&token=canary%zz", "https://host/path?xxxxx"},
		{"duplicate credential queries", "common.HttpSinkConfig.endpoint", "https://host/path?access-token=canary&Token=canary&Token=canary&region=eu", "https://host/path?Token=xxxxx&Token=xxxxx&access-token=xxxxx&region=eu"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, projectConnection(t, tc.field, tc.raw))
		})
	}
}

func TestRedactUnknownConnectionField(t *testing.T) {
	t.Parallel()
	options := &descriptorpb.FieldOptions{}
	proto.SetExtension(options, commonpb.E_SensitiveUrl, true)
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("unknown_connection.proto"), Package: new("future"), Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{Name: new("Config"), Field: []*descriptorpb.FieldDescriptorProto{{
			Name: new("url"), Number: new(int32(1)), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: options,
		}}}},
	}, nil)
	require.NoError(t, err)
	source := dynamicpb.NewMessage(file.Messages().Get(0))
	field := source.Descriptor().Fields().Get(0)
	source.Set(field, protoreflect.ValueOfString("https://alice:canary@host/path?region=eu"))
	original := proto.Clone(source)
	require.Equal(t, Marker, Redact(source).Get(field).String())
	require.True(t, proto.Equal(original, source))
}

func TestRedactDeclaredConnectionFields(t *testing.T) {
	t.Parallel()
	// Enumerate the schema independently of the registry: a new annotation must
	// acquire an explicit, reviewed format and a representative observable output.
	expected := map[protoreflect.FullName]struct{ raw, want string }{
		"common.PostgresMirrorSourceConfig.dsn":         {"host=db password=canary", "host=db password='xxxxx'"},
		"common.NatsSinkConfig.url":                     {"nats://canary@host", "nats://xxxxx@host"},
		"common.ClickHouseSinkConfig.dsn":               {"clickhouse://alice:canary@host/db", "clickhouse://alice:xxxxx@host/db"},
		"common.HttpSinkConfig.endpoint":                {"https://canary@host/path", "https://xxxxx@host/path"},
		"common.HttpMirrorSourceConfig.base_url":        {"https://canary@host/path", "https://xxxxx@host/path"},
		"common.OAuth2ClientCredentials.token_endpoint": {"https://canary@host/token", "https://xxxxx@host/token"},
	}
	var seen []protoreflect.FullName
	var walk func(protoreflect.MessageDescriptors)
	walk = func(messages protoreflect.MessageDescriptors) {
		for i := range messages.Len() {
			message := messages.Get(i)
			for j := range message.Fields().Len() {
				field := message.Fields().Get(j)
				if proto.GetExtension(field.Options(), commonpb.E_SensitiveUrl).(bool) {
					sample, ok := expected[field.FullName()]
					require.True(t, ok, "unclassified sensitive_url field %s", field.FullName())
					require.Equal(t, sample.want, projectConnection(t, field.FullName(), sample.raw))
					seen = append(seen, field.FullName())
				}
			}
			walk(message.Messages())
		}
	}
	walk(commonpb.File_common_proto.Messages())
	require.Len(t, seen, len(expected))
}

func TestRedactBoundsNestedProxyURLs(t *testing.T) {
	t.Parallel()
	proxy := "https://canary@proxy/leaf?token=canary"
	for range 12 {
		proxy = "https://proxy/path?http_proxy=" + url.QueryEscape(proxy)
	}
	source := &commonpb.ClickHouseSinkConfig{Dsn: "clickhouse://db/ledger?http_proxy=" + url.QueryEscape(proxy) + "&compress=true"}
	original := proto.Clone(source)
	projected := Redact(source)
	data, err := protojson.Marshal(projected)
	require.NoError(t, err)
	require.NotContains(t, string(data), "canary")
	require.Contains(t, string(data), "compress=true")
	require.Contains(t, string(data), "xxxxx")
	require.True(t, proto.Equal(original, source))
}

func BenchmarkRedactNATSServerList(b *testing.B) {
	for _, size := range []int{1, 16, 256} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			raw := strings.TrimSuffix(strings.Repeat("nats://alice:password@host:4222,", size), ",")
			source := &commonpb.NatsSinkConfig{Url: raw, Topic: "events"}
			b.SetBytes(int64(len(raw)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				Redact(source)
			}
		})
	}
}
