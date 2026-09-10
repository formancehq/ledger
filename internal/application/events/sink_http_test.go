package events

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestNewHTTPSink_EmptyEndpoint(t *testing.T) {
	t.Parallel()

	_, err := NewHTTPSink(HTTPSinkConfig{Endpoint: ""})
	require.Error(t, err)
	require.Contains(t, err.Error(), "endpoint is required")
}

func TestNewHTTPSink_Success(t *testing.T) {
	t.Parallel()

	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: "https://example.com/webhook",
		Secret:   "my-secret",
		Format:   FormatJSON,
	})
	require.NoError(t, err)
	require.NotNil(t, sink)
}

func TestHTTPSink_Publish_Success(t *testing.T) {
	t.Parallel()

	var (
		receivedBody    []byte
		receivedHeaders http.Header
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedHeaders = r.Header

		body, err := io.ReadAll(r.Body)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)

			return
		}

		receivedBody = body

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: server.URL,
		Format:   FormatJSON,
	})
	require.NoError(t, err)

	events := []*eventspb.Event{
		{
			Type:        commonpb.EventType_COMMITTED_TRANSACTION,
			Ledger:      "orders",
			LogSequence: 1,
			Date:        &commonpb.Timestamp{Data: 1000},
		},
	}

	err = sink.Publish(context.Background(), events)
	require.NoError(t, err)
	require.NotEmpty(t, receivedBody)
	require.Equal(t, "application/json", receivedHeaders.Get("Content-Type"))
	require.Equal(t, "orders", receivedHeaders.Get("X-Ledger"))
	require.Equal(t, "1", receivedHeaders.Get("X-Log-Sequence"))
}

func TestHTTPSink_Publish_WithSignature(t *testing.T) {
	t.Parallel()

	var signature string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		signature = r.Header.Get("X-Webhook-Signature")

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: server.URL,
		Secret:   "my-hmac-secret",
		Format:   FormatJSON,
	})
	require.NoError(t, err)

	events := []*eventspb.Event{
		{
			Type:        commonpb.EventType_COMMITTED_TRANSACTION,
			Ledger:      "orders",
			LogSequence: 1,
		},
	}

	err = sink.Publish(context.Background(), events)
	require.NoError(t, err)
	require.NotEmpty(t, signature)
	require.Contains(t, signature, "sha256=")
}

func TestHTTPSink_Publish_ProtoFormat(t *testing.T) {
	t.Parallel()

	var contentType string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		contentType = r.Header.Get("Content-Type")

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: server.URL,
		Format:   FormatProto,
	})
	require.NoError(t, err)

	events := []*eventspb.Event{
		{
			Type:        commonpb.EventType_COMMITTED_TRANSACTION,
			Ledger:      "orders",
			LogSequence: 1,
		},
	}

	err = sink.Publish(context.Background(), events)
	require.NoError(t, err)
	require.Equal(t, "application/protobuf", contentType)
}

func TestHTTPSink_Publish_ErrorStatus(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: server.URL,
		Format:   FormatJSON,
	})
	require.NoError(t, err)

	events := []*eventspb.Event{
		{
			Type:        commonpb.EventType_COMMITTED_TRANSACTION,
			Ledger:      "orders",
			LogSequence: 1,
		},
	}

	err = sink.Publish(context.Background(), events)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected status code: 500")
}

func TestHTTPSink_Close(t *testing.T) {
	t.Parallel()

	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: "https://example.com/webhook",
		Format:   FormatJSON,
	})
	require.NoError(t, err)

	err = sink.Close()
	require.NoError(t, err)
}

func TestHTTPSinkTransportErrorIsPublishable(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{
		Endpoint: "http://alice:EXAMPLE_PASSWORD@" + server.URL[len("http://"):] + "/events?api_key=EXAMPLE_API_KEY",
		Format:   FormatProto,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()
	err = sink.Publish(context.Background(), []*eventspb.Event{{LogSequence: 1}})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "EXAMPLE_PASSWORD")
	require.NotContains(t, err.Error(), "EXAMPLE_API_KEY")
	require.Contains(t, err.Error(), server.URL[len("http://"):])
	require.Contains(t, err.Error(), "posting event seq=1")
	require.Contains(t, err.Error(), "EOF")
	require.ErrorIs(t, err, io.EOF)
}

func TestHTTPSinkRedirectFailureSanitizesActualURL(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/events" {
			http.Redirect(w, r, "/next?credential=EXAMPLE_CREDENTIAL", http.StatusTemporaryRedirect)
			return
		}
		panic(http.ErrAbortHandler)
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL + "/events?credential=EXAMPLE_CREDENTIAL", Format: FormatProto})
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()
	err = sink.Publish(t.Context(), []*eventspb.Event{{LogSequence: 1}})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "EXAMPLE_CREDENTIAL")
	require.Contains(t, err.Error(), "/next?")
	require.Contains(t, err.Error(), "EOF")
	require.ErrorIs(t, err, io.EOF)
}

func TestHTTPSinkMalformedURLDiagnosticIsSafe(t *testing.T) {
	t.Parallel()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: "https://alice:EXAMPLE_PASSWORD@example.com/\n", Format: FormatProto})
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()
	err = sink.Publish(t.Context(), []*eventspb.Event{{LogSequence: 1}})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid control character")
	require.NotContains(t, err.Error(), "EXAMPLE_PASSWORD")
}
