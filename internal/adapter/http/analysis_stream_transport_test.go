package http

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func TestAnalysisStream_FlushAndDisconnect(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"accounts", "transactions"} {
		t.Run(route, func(t *testing.T) {
			t.Parallel()
			canceled := make(chan error, 1)
			handler := exerciseAnalysisRoute(t, route, func(ctx context.Context, progress func(uint64, uint64)) error {
				progress(500, 0)
				<-ctx.Done()
				canceled <- ctx.Err()

				return ctx.Err()
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			r, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL+"/v3/books/analyze-"+route, nil)
			require.NoError(t, err)
			r.Header.Set("Accept", "text/event-stream")
			response, err := server.Client().Do(r)
			require.NoError(t, err)
			defer func() { require.NoError(t, response.Body.Close()) }()
			require.Equal(t, "text/event-stream", response.Header.Get("Content-Type"))
			reader := bufio.NewReader(response.Body)
			// Read two full events while the backend is still blocked. This proves
			// middleware preserves flush capability; buffered completion cannot pass.
			for range 2 {
				for {
					line, err := reader.ReadString('\n')
					require.NoError(t, err)
					if line == "\n" {
						break
					}
				}
			}
			require.NoError(t, response.Body.Close())
			select {
			case err := <-canceled:
				require.ErrorIs(t, err, context.Canceled)
			case <-ctx.Done():
				t.Fatal("disconnect did not cancel backend")
			}
		})
	}
}

// failingAnalysisWriter models a sink whose second event cannot be written or
// flushed, while retaining ResponseController's real unwrap path to a recorder.
type failingAnalysisWriter struct {
	*httptest.ResponseRecorder

	failFlush bool
	writes    int
	flushes   int
	deadlines []time.Time
	mu        sync.Mutex
}

func (w *failingAnalysisWriter) Write(data []byte) (int, error) {
	w.writes++
	if !w.failFlush && w.writes == 2 {
		return 0, io.ErrClosedPipe
	}

	return w.ResponseRecorder.Write(data)
}

func (w *failingAnalysisWriter) FlushError() error {
	w.flushes++
	if w.failFlush && w.flushes == 2 {
		return io.ErrClosedPipe
	}
	w.Flush()

	return nil
}

func (w *failingAnalysisWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.deadlines = append(w.deadlines, deadline)

	return nil
}

func TestAnalysisStream_TransportFailureCancelsBackend(t *testing.T) {
	t.Parallel()
	for _, route := range []string{"accounts", "transactions"} {
		for _, failFlush := range []bool{false, true} {
			t.Run(route+"/"+map[bool]string{false: "write", true: "flush"}[failFlush], func(t *testing.T) {
				t.Parallel()
				w := &failingAnalysisWriter{ResponseRecorder: httptest.NewRecorder(), failFlush: failFlush}
				handler := exerciseAnalysisRoute(t, route, func(ctx context.Context, progress func(uint64, uint64)) error {
					progress(500, 0)
					require.ErrorIs(t, ctx.Err(), context.Canceled)
					progress(501, 0)

					return nil
				})
				handler.ServeHTTP(w, analysisRequest(route, "text/event-stream"))
				require.Equal(t, 2, w.writes)
				require.NotContains(t, w.Body.String(), "event: result")
				require.NotContains(t, w.Body.String(), "event: error")
			})
		}
	}
}

// deadlineAnalysisWriter blocks at the network boundary, using the deadline
// supplied by ResponseController. The synctest clock makes the timeout exact.
type deadlineAnalysisWriter struct {
	*httptest.ResponseRecorder

	expires  chan struct{}
	timer    *time.Timer
	mu       sync.Mutex
	once     sync.Once
	timedOut bool
}

func (w *deadlineAnalysisWriter) SetWriteDeadline(deadline time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.timer != nil {
		w.timer.Stop()
	}
	if deadline.IsZero() {
		return nil
	}
	w.timer = time.AfterFunc(time.Until(deadline), func() { w.once.Do(func() { close(w.expires) }) })

	return nil
}

func (w *deadlineAnalysisWriter) Write(_ []byte) (int, error) {
	<-w.expires
	w.timedOut = true

	return 0, os.ErrDeadlineExceeded
}

func TestAnalysisStream_WriteDeadlineAndRequestCancellation(t *testing.T) {
	t.Parallel()
	for _, cancelRequest := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "cancel"}[cancelRequest], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				w := &deadlineAnalysisWriter{ResponseRecorder: httptest.NewRecorder(), expires: make(chan struct{})}
				// Starting the stream itself must be bounded, even before a backend call.
				handler := newHandlerForBlockedAnalysis(t)
				finished := make(chan struct{})
				started := time.Now()
				go func() {
					defer close(finished)
					handler.ServeHTTP(w, analysisRequest("accounts", "text/event-stream").WithContext(ctx))
				}()
				synctest.Wait()
				if cancelRequest {
					cancel()
				}
				<-finished
				require.True(t, w.timedOut)
				if cancelRequest {
					require.Equal(t, time.Duration(0), time.Since(started))
				} else {
					require.Equal(t, 10*time.Second, time.Since(started))
				}
				require.Empty(t, w.Body.String())
			})
		})
	}
}

func TestAnalysisStream_DeadlineClearedBetweenEvents(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		w := &failingAnalysisWriter{ResponseRecorder: httptest.NewRecorder()}
		// No progress event: the second write is the result, so disable injected failure.
		w.writes = 2
		handler := exerciseAnalysisRoute(t, "accounts", func(_ context.Context, _ func(uint64, uint64)) error {
			require.Len(t, w.deadlines, 2)
			require.False(t, w.deadlines[0].IsZero())
			require.True(t, w.deadlines[1].IsZero())
			<-time.After(30 * time.Second)

			return nil
		})
		handler.ServeHTTP(w, analysisRequest("accounts", "text/event-stream"))
		require.Contains(t, w.Body.String(), "event: result")
		require.Len(t, w.deadlines, 4)
		require.True(t, w.deadlines[3].IsZero())
	})
}

func newHandlerForBlockedAnalysis(t *testing.T) http.Handler {
	t.Helper()
	// The boundary mock deliberately has no expected backend calls: a failed
	// started event must return before any scan begins.
	backend := NewMockBackend(gomock.NewController(t))

	return NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
}

func TestAnalysisStream_AbortHandlerRemainsAbort(t *testing.T) {
	t.Parallel()
	handler := exerciseAnalysisRoute(t, "accounts", func(_ context.Context, _ func(uint64, uint64)) error { panic(http.ErrAbortHandler) })
	require.PanicsWithValue(t, http.ErrAbortHandler, func() { handler.ServeHTTP(httptest.NewRecorder(), analysisRequest("accounts", "text/event-stream")) })
}
