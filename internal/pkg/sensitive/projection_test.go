package sensitive

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestClonePreservesSourceAndDropsOpaqueEvidence(t *testing.T) {
	t.Parallel()
	source := &commonpb.Log{Sequence: 17, ResponseSignature: &signaturepb.SignedLog{KeyId: "server", Payload: []byte("secret"), Signature: []byte("signature")}}
	source.ProtoReflect().SetUnknown(protowire.AppendBytes(protowire.AppendTag(nil, 1000, protowire.BytesType), []byte("unknown secret")))
	before := proto.Clone(source)
	view := Redact(source)
	require.Equal(t, uint64(17), view.GetSequence())
	require.Nil(t, view.GetResponseSignature())
	require.Empty(t, view.ProtoReflect().GetUnknown())
	require.True(t, proto.Equal(before, source))
	require.NotSame(t, source, view)
	var absent *commonpb.Log
	require.Nil(t, Redact(absent))
	require.Nil(t, Redact[proto.Message](nil))
}

func TestCloneRecursiveDescriptors(t *testing.T) {
	t.Parallel()
	marked := &descriptorpb.FieldOptions{}
	proto.SetExtension(marked, commonpb.E_Sensitive, true)
	field := func(name string, n int32, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{Name: new(name), Number: new(n), Type: kind.Enum(), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()}
	}
	password := field("password", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING)
	password.Options = marked
	child := &descriptorpb.DescriptorProto{Name: new("Child"), Field: []*descriptorpb.FieldDescriptorProto{password, field("name", 2, descriptorpb.FieldDescriptorProto_TYPE_STRING)}}
	single := field("single", 1, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	single.TypeName = new(".test.Child")
	list := field("list", 2, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	list.TypeName = single.TypeName
	list.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	entry := &descriptorpb.DescriptorProto{Name: new("ByNameEntry"), Options: &descriptorpb.MessageOptions{MapEntry: new(true)}, Field: []*descriptorpb.FieldDescriptorProto{field("key", 1, descriptorpb.FieldDescriptorProto_TYPE_STRING), field("value", 2, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)}}
	entry.Field[1].TypeName = single.TypeName
	mapping := field("by_name", 3, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	mapping.TypeName = new(".test.Parent.ByNameEntry")
	mapping.Label = list.Label
	bytes := field("opaque", 4, descriptorpb.FieldDescriptorProto_TYPE_BYTES)
	bytes.Options = marked
	parent := &descriptorpb.DescriptorProto{Name: new("Parent"), NestedType: []*descriptorpb.DescriptorProto{entry}, Field: []*descriptorpb.FieldDescriptorProto{single, list, mapping, bytes}}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{Name: new("projection_test.proto"), Package: new("test"), Syntax: new("proto3"), MessageType: []*descriptorpb.DescriptorProto{child, parent}}, nil)
	require.NoError(t, err)
	desc := file.Messages().ByName("Parent")
	source := dynamicpb.NewMessage(desc)
	makeChild := func() protoreflect.Value {
		m := dynamicpb.NewMessage(file.Messages().ByName("Child"))
		m.Set(m.Descriptor().Fields().ByName("password"), protoreflect.ValueOfString("credential"))
		m.Set(m.Descriptor().Fields().ByName("name"), protoreflect.ValueOfString("visible"))

		return protoreflect.ValueOfMessage(m)
	}
	source.Set(desc.Fields().ByName("single"), makeChild())
	source.Mutable(desc.Fields().ByName("list")).List().Append(makeChild())
	source.Mutable(desc.Fields().ByName("by_name")).Map().Set(protoreflect.ValueOfString("server").MapKey(), makeChild())
	source.Set(desc.Fields().ByName("opaque"), protoreflect.ValueOfBytes([]byte("credential")))
	before := proto.Clone(source)
	view := Redact(source)
	require.True(t, proto.Equal(before, source))
	require.False(t, view.Has(desc.Fields().ByName("opaque")))
	assertChild := func(v protoreflect.Value) {
		m := v.Message()
		require.Equal(t, Marker, m.Get(m.Descriptor().Fields().ByName("password")).String())
		require.Equal(t, "visible", m.Get(m.Descriptor().Fields().ByName("name")).String())
	}
	assertChild(view.Get(desc.Fields().ByName("single")))
	assertChild(view.Get(desc.Fields().ByName("list")).List().Get(0))
	assertChild(view.Get(desc.Fields().ByName("by_name")).Map().Get(protoreflect.ValueOfString("server").MapKey()))
}

func TestClonePreservesSanitizedSinkDiagnostic(t *testing.T) {
	t.Parallel()
	source := &commonpb.SinkError{Message: "sending request to webhook.example: connection refused"}
	view := Redact(source)
	require.Equal(t, source.GetMessage(), view.GetMessage())
	require.NotSame(t, source, view)
}

func TestCloneNilNestedMessages(t *testing.T) {
	t.Parallel()
	source := &commonpb.PostCommitVolumes{VolumesByAccount: map[string]*commonpb.VolumesByAssets{
		"absent":  nil,
		"present": {Volumes: []*commonpb.VolumeEntry{nil, {Asset: "USD/2"}}},
	}}
	before := proto.Clone(source)
	var view *commonpb.PostCommitVolumes
	require.NotPanics(t, func() { view = Redact(source) })
	require.True(t, proto.Equal(before, source))
	// proto.Clone materializes nil map/list messages before redact sees them.
	require.NotNil(t, view.GetVolumesByAccount()["absent"])
	require.NotNil(t, view.GetVolumesByAccount()["present"].GetVolumes()[0])
	require.Nil(t, source.GetVolumesByAccount()["absent"])
	require.Nil(t, source.GetVolumesByAccount()["present"].GetVolumes()[0])
	require.Len(t, view.GetVolumesByAccount()["present"].GetVolumes(), 2)
	require.Equal(t, "USD/2", view.GetVolumesByAccount()["present"].GetVolumes()[1].GetAsset())
}

func TestCloneSensitiveOneofs(t *testing.T) {
	t.Parallel()
	for _, source := range []*commonpb.DatabricksSinkConfig{
		{Auth: &commonpb.DatabricksSinkConfig_Token{Token: "credential"}},
		{Auth: &commonpb.DatabricksSinkConfig_OauthM2M{OauthM2M: &commonpb.DatabricksOAuthM2M{ClientId: "visible", ClientSecret: "credential"}}},
	} {
		before := proto.Clone(source)
		view := Redact(source)
		require.True(t, proto.Equal(before, source))
		if source.GetToken() != "" {
			require.Equal(t, Marker, view.GetToken())
		} else {
			require.Equal(t, "visible", view.GetOauthM2M().GetClientId())
			require.Equal(t, Marker, view.GetOauthM2M().GetClientSecret())
		}
	}
}

func TestCloneMasksMirrorDiagnostic(t *testing.T) {
	t.Parallel()
	source := &commonpb.MirrorSyncError{Message: "connection password=sentinel"}
	projected := Redact(source)
	require.Equal(t, Marker, projected.GetMessage())
	require.Equal(t, "connection password=sentinel", source.GetMessage())
}

func TestRedactURL(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "empty",
			in:   "",
			want: "",
		},
		{
			name: "clickhouse with password",
			in:   "clickhouse://user:secret@host:9000/db",
			want: "clickhouse://user:xxxxx@host:9000/db",
		},
		{
			name: "clickhouse password in query",
			in:   "clickhouse://host:9000/db?password=secret",
			want: "clickhouse://host:9000/db?password=xxxxx",
		},
		{
			name: "nats token in username",
			in:   "nats://mytoken@host:4222",
			want: "nats://xxxxx@host:4222",
		},
		{
			name: "nats with password",
			in:   "nats://user:secret@host:4222",
			want: "nats://user:xxxxx@host:4222",
		},
		{
			name: "postgres password in url",
			in:   "postgres://user:hunter2@host:5432/ledger?sslmode=disable",
			want: "postgres://user:xxxxx@host:5432/ledger?sslmode=disable",
		},
		{
			name: "no credentials preserved",
			in:   "clickhouse://host:9000/db",
			want: "clickhouse://host:9000/db",
		},
		{
			name: "unparseable falls back to Marker",
			in:   "not a url",
			want: Marker,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, redactURL(tc.in))
		})
	}
}

func TestCloneSensitiveURL(t *testing.T) {
	t.Parallel()
	source := &commonpb.ClickHouseSinkConfig{
		Dsn:   "clickhouse://user:secret@host:9000/db",
		Table: "ledger_events",
	}
	view := Redact(source)
	require.Equal(t, "clickhouse://user:xxxxx@host:9000/db", view.GetDsn())
	require.Equal(t, "ledger_events", view.GetTable())
	require.Equal(t, "clickhouse://user:secret@host:9000/db", source.GetDsn())

	nats := &commonpb.NatsSinkConfig{
		Url:   "nats://mytoken@host:4222",
		Topic: "events",
	}
	natsView := Redact(nats)
	require.Equal(t, "nats://xxxxx@host:4222", natsView.GetUrl())
	require.Equal(t, "events", natsView.GetTopic())
	require.Equal(t, "nats://mytoken@host:4222", nats.GetUrl())
}

func TestRedactURLClickHouseProxy(t *testing.T) {
	t.Parallel()
	// Credential-bearing http_proxy value must be redacted even when URL-encoded.
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "http_proxy with credentials",
			in:   "clickhouse://host:9000/db?http_proxy=http%3A%2F%2Falice%3Aproxy-secret%40proxy%3A8080",
			want: "clickhouse://host:9000/db?http_proxy=http%3A%2F%2Falice%3Axxxxx%40proxy%3A8080",
		},
		{
			name: "https_proxy with credentials",
			in:   "clickhouse://host:9000/db?https_proxy=http%3A%2F%2Fbob%3Asecret%40proxy%3A3128",
			want: "clickhouse://host:9000/db?https_proxy=http%3A%2F%2Fbob%3Axxxxx%40proxy%3A3128",
		},
		{
			name: "non-credential query param preserved",
			in:   "clickhouse://host:9000/db?compress=true",
			want: "clickhouse://host:9000/db?compress=true",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, redactURL(tc.in))
		})
	}
}

func TestRedactURLNATSMultiServer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "multi-server with distinct passwords",
			in:   "nats://alice:first-secret@host1,nats://bob:second-secret@host2:4222",
			want: "nats://alice:xxxxx@host1,nats://bob:xxxxx@host2:4222",
		},
		{
			name: "ws scheme token in username",
			in:   "ws://token-secret@host:4443",
			want: "ws://xxxxx@host:4443",
		},
		{
			name: "wss scheme token in username",
			in:   "wss://token-secret@host:4443",
			want: "wss://xxxxx@host:4443",
		},
		{
			name: "tls scheme token in username",
			in:   "tls://token-secret@host:4222",
			want: "tls://xxxxx@host:4222",
		},
		{
			name: "multi-server mixed scheme",
			in:   "nats://alice:s1@host1,tls://bob:s2@host2",
			want: "nats://alice:xxxxx@host1,tls://bob:xxxxx@host2",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, redactURL(tc.in))
		})
	}
}

func TestRedactURLProxyDoubleEncode(t *testing.T) {
	t.Parallel()
	// Inner proxy URL with encoded delimiters in the token must not expose suffix.
	in := "clickhouse://host:9000/db?http_proxy=http%3A%2F%2Fproxy%2F%3Ftoken%3Dprefix%2526suffix%253Dsecret"
	result := redactURL(in)
	require.NotContains(t, result, "prefix")
	require.NotContains(t, result, "secret")
	require.NotContains(t, result, "suffix")
}

func TestRedactURLClickHouseMultiHost(t *testing.T) {
	t.Parallel()
	// Multi-host ClickHouse DSN must preserve both hosts, database and options.
	in := "clickhouse://alice:review-password@host1:9000,host2:9000/default?compress=true"
	result := redactURL(in)
	require.NotContains(t, result, "review-password")
	require.Contains(t, result, "host1:9000,host2:9000")
	require.Contains(t, result, "/default")
	require.Contains(t, result, "compress=true")
}

func TestRedactHTTPSinkEndpoint(t *testing.T) {
	t.Parallel()
	// HttpSinkConfig.Endpoint annotated sensitive_url — credentials must be masked.
	source := &commonpb.HttpSinkConfig{
		Endpoint: "https://alice:review-password@example.com/events?api_key=review-api-key",
		Secret:   "hmac-secret",
	}
	view := Redact(source)
	require.NotContains(t, view.GetEndpoint(), "review-password")
	require.NotContains(t, view.GetEndpoint(), "review-api-key")
	require.Contains(t, view.GetEndpoint(), "example.com")
	require.Equal(t, Marker, view.GetSecret())
	// Original unchanged.
	require.Contains(t, source.GetEndpoint(), "review-password")
}
