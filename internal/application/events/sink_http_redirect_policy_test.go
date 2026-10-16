package events

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
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
	require.ErrorContains(t, err, "returned redirect 307")
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, attempts, "the sink must not follow any redirect")
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
			require.ErrorContains(t, err, "returned redirect 307")
			require.ErrorContains(t, err, "/next")
			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, []string{http.MethodPost}, methods, "/next must never be reached")
			require.Len(t, bodies, 1)
			require.NotEmpty(t, bodies[0])
		})
	}
}

// Regression for s5-http-sink-follows-redirect-to-internal-1: a method-preserving
// redirect to another host must not deliver the body, signature or endpoint URL
// (through Referer) to that host, and its response must not acknowledge the event.
func TestHTTPSink_CrossHostRedirectIsNotFollowed(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			t.Parallel()
			var targetRequests atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				targetRequests.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer target.Close()
			targetURL, err := url.Parse(target.URL)
			require.NoError(t, err)
			location := "http://EXAMPLE_LOCATION_USER:EXAMPLE_LOCATION_PASSWORD@" + targetURL.Host +
				"/internal?token=EXAMPLE_LOCATION_TOKEN#EXAMPLE_FRAGMENT"
			var endpointRequests atomic.Int32
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				endpointRequests.Add(1)
				w.Header().Set("Location", location)
				w.WriteHeader(status)
			}))
			defer endpoint.Close()
			sink, err := NewHTTPSink(HTTPSinkConfig{
				Endpoint: endpoint.URL + "/webhook?token=EXAMPLE_ENDPOINT_TOKEN",
				Secret:   "EXAMPLE_SECRET",
				Format:   FormatJSON,
			})
			require.NoError(t, err)
			defer func() { require.NoError(t, sink.Close()) }()
			err = sink.Publish(context.Background(), []*eventspb.Event{{LogSequence: 1}})
			require.ErrorContains(t, err, "returned redirect "+strconv.Itoa(status))
			require.ErrorContains(t, err, "configure the final URL")
			require.ErrorContains(t, err, targetURL.Host+"/internal")
			for _, leaked := range []string{"EXAMPLE_ENDPOINT_TOKEN", "EXAMPLE_LOCATION_TOKEN", "EXAMPLE_LOCATION_USER", "EXAMPLE_LOCATION_PASSWORD", "EXAMPLE_FRAGMENT", "EXAMPLE_SECRET"} {
				require.NotContains(t, err.Error(), leaked)
			}
			require.Equal(t, int32(1), endpointRequests.Load())
			require.Zero(t, targetRequests.Load(), "the redirect target must receive no request")
		})
	}
}
