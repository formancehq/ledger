package connectionconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// This test has no build tags: every declared sink must remain normalizable
// on nodes that cannot instantiate its optional runtime driver.
func TestSinkNormalizerRegistryCoversSchema(t *testing.T) {
	t.Parallel()
	fixtures := map[protoreflect.Name]proto.Message{
		"nats":       &commonpb.NatsSinkConfigInput{Url: "nats://user:password@localhost:4222", Topic: "events"},
		"http":       &commonpb.HttpSinkConfigInput{Endpoint: "https://localhost/events", Secret: "secret"},
		"clickhouse": &commonpb.ClickHouseSinkConfigInput{Dsn: "clickhouse://user:password@localhost/default", Table: "events"},
		"kafka":      &commonpb.KafkaSinkConfig{Brokers: []string{"localhost:9092"}, SaslPassword: "secret"},
		"databricks": &commonpb.DatabricksSinkConfig{Auth: &commonpb.DatabricksSinkConfig_Token{Token: "secret"}},
	}
	fields := (&commonpb.SinkConfigInput{}).ProtoReflect().Descriptor().Oneofs().ByName("type").Fields()
	outputs := (&commonpb.SinkConfig{}).ProtoReflect().Descriptor().Oneofs().ByName("type").Fields()
	require.Len(t, sinkNormalizers, fields.Len())
	require.Len(t, fixtures, fields.Len())
	require.Equal(t, fields.Len(), outputs.Len())
	for i := range fields.Len() {
		field := fields.Get(i)
		t.Run(string(field.Name()), func(t *testing.T) {
			t.Parallel()
			normalizer, ok := sinkNormalizers[field.Number()]
			require.True(t, ok, "missing normalizer for %s", field.Name())
			require.Equal(t, field.Name(), normalizer.outputField.Name())
			fixture, ok := fixtures[field.Name()]
			require.True(t, ok, "missing behavioral fixture for %s", field.Name())
			input := &commonpb.SinkConfigInput{Name: "events", Format: "json", BatchSize: 17, BatchDelayMs: 23, EventTypes: []commonpb.EventType{commonpb.EventType_COMMITTED_TRANSACTION}}
			input.ProtoReflect().Set(field, protoreflect.ValueOfMessage(fixture.ProtoReflect()))
			before := proto.Clone(input)
			output, err := Sink(input)
			require.NoError(t, err)
			require.Equal(t, field.Name(), output.ProtoReflect().WhichOneof(outputs.Get(0).ContainingOneof()).Name())
			require.Equal(t, input.GetName(), output.GetName())
			require.Equal(t, input.GetFormat(), output.GetFormat())
			require.Equal(t, input.GetBatchSize(), output.GetBatchSize())
			require.Equal(t, input.GetBatchDelayMs(), output.GetBatchDelayMs())
			require.Equal(t, input.GetEventTypes(), output.GetEventTypes())
			output.EventTypes[0] = commonpb.EventType_DELETED_LEDGER
			require.True(t, proto.Equal(before, input))
		})
	}
}

func TestSinkNormalizerRegistrationRejectsInvalidBindings(t *testing.T) {
	t.Parallel()
	registry := sinkNormalizerRegistry{}
	registerSinkNormalizer(registry, "http", normalizeHttpSink)
	require.PanicsWithValue(t, "duplicate sink normalizer registration for http", func() {
		registerSinkNormalizer(registry, "http", normalizeHttpSink)
	})
	for _, name := range []protoreflect.Name{"missing", "name", "nats"} {
		require.PanicsWithValue(t, "invalid sink normalizer registration for "+string(name), func() {
			registerSinkNormalizer(sinkNormalizerRegistry{}, name, normalizeHttpSink)
		})
	}
	require.PanicsWithValue(t, "invalid sink normalizer registration for http", func() {
		registerSinkNormalizer(sinkNormalizerRegistry{}, "http", func(*commonpb.HttpSinkConfigInput) (*commonpb.NatsSinkConfig, error) {
			return &commonpb.NatsSinkConfig{}, nil
		})
	})
	var missing func(*commonpb.HttpSinkConfigInput) (*commonpb.HttpSinkConfig, error)
	require.PanicsWithValue(t, "invalid sink normalizer registration for http", func() {
		registerSinkNormalizer(sinkNormalizerRegistry{}, "http", missing)
	})
}

func TestSinkNormalizerImpossibleStatesFailLoudly(t *testing.T) {
	t.Parallel()
	input := &commonpb.SinkConfigInput{Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://localhost"}}}
	registry := sinkNormalizerRegistry{}
	require.PanicsWithValue(t, "missing sink normalizer for http", func() {
		_, _ = registry.project(input)
	})
	registerSinkNormalizer(registry, "http", func(*commonpb.HttpSinkConfigInput) (*commonpb.HttpSinkConfig, error) {
		return nil, nil
	})
	require.PanicsWithValue(t, "sink normalizer http returned no configuration", func() {
		_, _ = registry.project(input)
	})
}

func TestSinkNormalizerMissingInput(t *testing.T) {
	t.Parallel()
	output, err := Sink(nil)
	require.NoError(t, err)
	require.Nil(t, output)
	_, err = Sink(&commonpb.SinkConfigInput{})
	require.EqualError(t, err, "sink type is required")
	inputs := []*commonpb.SinkConfigInput{
		{Type: &commonpb.SinkConfigInput_Http{}},
		{Type: &commonpb.SinkConfigInput_Nats{}},
		{Type: &commonpb.SinkConfigInput_Clickhouse{}},
		{Type: &commonpb.SinkConfigInput_Kafka{}},
		{Type: &commonpb.SinkConfigInput_Databricks{}},
	}
	for _, input := range inputs {
		output, err := Sink(input)
		require.ErrorContains(t, err, "configuration is required")
		require.Nil(t, output)
	}
}
