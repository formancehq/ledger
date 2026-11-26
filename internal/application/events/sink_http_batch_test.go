package events

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestHTTPSinkBatchRetry(t *testing.T) {
	t.Parallel()
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies = append(bodies, body)
		require.Equal(t, "1", r.Header.Get("X-Ledger-Batch-Version"))
		require.Equal(t, "2", r.Header.Get("X-Event-Count"))
		require.Equal(t, "7", r.Header.Get("X-Log-Sequence-First"))
		require.Equal(t, "8", r.Header.Get("X-Log-Sequence-Last"))
		require.Empty(t, r.Header.Get("X-Event-Type"))
		require.Empty(t, r.Header.Get("X-Log-Sequence"))
		mac := hmac.New(sha256.New, []byte("secret"))
		_, err = mac.Write(body)
		require.NoError(t, err)
		require.Equal(t, "sha256="+hex.EncodeToString(mac.Sum(nil)), r.Header.Get("X-Webhook-Signature"))
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL, Secret: "secret", Format: FormatJSON, Batch: true})
	require.NoError(t, err)
	events := []*eventspb.Event{{LogSequence: 7}, {LogSequence: 8}}
	require.Error(t, sink.Publish(context.Background(), events))
	require.NoError(t, sink.Publish(context.Background(), events))
	require.Len(t, bodies, 2)
	require.Equal(t, bodies[0], bodies[1])
	var decoded []map[string]any
	require.NoError(t, json.Unmarshal(bodies[1], &decoded))
	require.Len(t, decoded, 2)
}

func TestHTTPSinkBatchProtobuf(t *testing.T) {
	t.Parallel()
	var body []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		body, err = io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, "application/protobuf", r.Header.Get("Content-Type"))
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL, Format: FormatProto, Batch: true})
	require.NoError(t, err)
	require.NoError(t, sink.Publish(context.Background(), []*eventspb.Event{{LogSequence: 7}, {LogSequence: 8}}))
	for _, want := range []uint64{7, 8} {
		require.GreaterOrEqual(t, len(body), 4)
		size := binary.BigEndian.Uint32(body[:4])
		body = body[4:]
		require.GreaterOrEqual(t, len(body), int(size))
		var event eventspb.Event
		require.NoError(t, event.UnmarshalVT(body[:size]))
		require.Equal(t, want, event.GetLogSequence())
		body = body[size:]
	}
	require.Empty(t, body)
}

func TestHTTPSinkBatchSplitsAtByteLimit(t *testing.T) {
	t.Parallel()
	var sizes []int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sizes = append(sizes, len(body))
		require.LessOrEqual(t, len(body), maxHTTPBatchBytes)
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL, Format: FormatJSON, Batch: true})
	require.NoError(t, err)
	ledger := strings.Repeat("x", maxHTTPBatchBytes/2)
	require.NoError(t, sink.Publish(context.Background(), []*eventspb.Event{
		{LogSequence: 1, Ledger: ledger}, {LogSequence: 2, Ledger: ledger},
	}))
	require.Len(t, sizes, 2)
}

func TestHTTPSinkBatchOversizedEventRetriesThenContinues(t *testing.T) {
	t.Parallel()
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		bodies = append(bodies, body)
		require.Equal(t, "1", r.Header.Get("X-Ledger-Batch-Version"))
		if len(bodies) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL, Format: FormatJSON, Batch: true})
	require.NoError(t, err)
	events := []*eventspb.Event{
		{LogSequence: 7, Ledger: strings.Repeat("x", maxHTTPBatchBytes)},
		{LogSequence: 8, Ledger: "next"},
	}
	require.Error(t, sink.Publish(context.Background(), events))
	require.NoError(t, sink.Publish(context.Background(), events))
	require.Len(t, bodies, 3)
	require.Equal(t, bodies[0], bodies[1])
	require.Greater(t, len(bodies[0]), maxHTTPBatchBytes)
	var first, second []map[string]any
	require.NoError(t, json.Unmarshal(bodies[1], &first))
	require.NoError(t, json.Unmarshal(bodies[2], &second))
	require.Len(t, first, 1)
	require.Len(t, second, 1)
	require.Equal(t, float64(7), first[0]["logSequence"])
	require.Equal(t, float64(8), second[0]["logSequence"])
}

func TestHTTPSinkBatchConfigWireRoundTrip(t *testing.T) {
	t.Parallel()
	original := &commonpb.HttpSinkConfig{Endpoint: "https://example.com/events", Batch: true}
	data, err := original.MarshalVT()
	require.NoError(t, err)
	var decoded commonpb.HttpSinkConfig
	require.NoError(t, decoded.UnmarshalVT(data))
	require.True(t, decoded.GetBatch())
	require.Equal(t, original.GetEndpoint(), decoded.GetEndpoint())
}
