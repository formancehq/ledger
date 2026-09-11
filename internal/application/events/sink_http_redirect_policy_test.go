package events

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func TestHTTPSink_RedirectLoopIsBounded(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		attempts++
		mu.Unlock()
		http.Redirect(w, r, "/loop", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL, Format: FormatJSON})
	require.NoError(t, err)
	defer func() { require.NoError(t, sink.Close()) }()
	err = sink.Publish(context.Background(), []*eventspb.Event{{LogSequence: 1}})
	require.ErrorContains(t, err, "stopped after 10 redirects")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 10, attempts)
}

func TestHTTPSink_PreservingRedirectThenFailure(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusFound, http.StatusInternalServerError} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			var (
				mu      sync.Mutex
				methods []string
				bodies  [][]byte
			)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)

					return
				}
				mu.Lock()
				methods = append(methods, r.Method)
				bodies = append(bodies, body)
				mu.Unlock()
				switch r.URL.Path {
				case "/webhook":
					http.Redirect(w, r, "/next", http.StatusTemporaryRedirect)
				case "/next":
					w.Header().Set("Location", "/login")
					w.WriteHeader(status)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer server.Close()
			sink, err := NewHTTPSink(HTTPSinkConfig{Endpoint: server.URL + "/webhook", Format: FormatJSON})
			require.NoError(t, err)
			defer func() { require.NoError(t, sink.Close()) }()
			err = sink.Publish(context.Background(), []*eventspb.Event{{LogSequence: 1}})
			require.ErrorContains(t, err, fmt.Sprintf("unexpected status code: %d", status))
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{http.MethodPost, http.MethodPost}, methods)
			require.Len(t, bodies, 2)
			require.NotEmpty(t, bodies[0])
			require.Equal(t, bodies[0], bodies[1])
		})
	}
}
