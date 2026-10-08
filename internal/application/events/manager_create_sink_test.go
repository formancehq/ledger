package events

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

func TestCreateSink_HTTP(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &ledgerpb.SinkConfig{
		Name: "http-sink",
		Type: &ledgerpb.SinkConfig_Http{
			Http: &ledgerpb.HttpSinkConfig{
				Endpoint: "https://example.com/webhook",
				Secret:   "my-secret",
			},
		},
		Format: "json",
	}

	sink, err := m.createSink(cfg)
	require.NoError(t, err)
	require.NotNil(t, sink)

	httpSink, ok := sink.(*HTTPSink)
	require.True(t, ok)
	require.Equal(t, "https://example.com/webhook", httpSink.endpoint)
	require.NoError(t, sink.Close())
}

func TestCreateSink_HTTP_EmptyEndpoint(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &ledgerpb.SinkConfig{
		Name: "http-sink",
		Type: &ledgerpb.SinkConfig_Http{
			Http: &ledgerpb.HttpSinkConfig{
				Endpoint: "",
			},
		},
		Format: "json",
	}

	sink, err := m.createSink(cfg)
	require.Error(t, err)
	require.Nil(t, sink)
}

func TestCreateSink_HTTP_DefaultFormat(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &ledgerpb.SinkConfig{
		Name: "http-sink",
		Type: &ledgerpb.SinkConfig_Http{
			Http: &ledgerpb.HttpSinkConfig{
				Endpoint: "https://example.com/webhook",
			},
		},
		Format: "", // Should default to JSON
	}

	sink, err := m.createSink(cfg)
	require.NoError(t, err)
	require.NotNil(t, sink)

	httpSink, ok := sink.(*HTTPSink)
	require.True(t, ok)
	require.Equal(t, FormatJSON, httpSink.format)
	require.NoError(t, sink.Close())
}

func TestCreateSink_UnsupportedType(t *testing.T) {
	t.Parallel()

	m := &Manager{}

	cfg := &ledgerpb.SinkConfig{
		Name:   "unknown-sink",
		Format: "json",
		// No type set
	}

	sink, err := m.createSink(cfg)
	require.Error(t, err)
	require.Nil(t, sink)
	require.Contains(t, err.Error(), "unsupported events sink type")
}
