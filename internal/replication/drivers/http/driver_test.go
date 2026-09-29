package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

func TestHTTPDriver(t *testing.T) {
	t.Parallel()

	messages := make(chan []drivers.LogWithLedger, 1)
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newMessages := make([]drivers.LogWithLedger, 0)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&newMessages))

		messages <- newMessages
	}))
	t.Cleanup(testServer.Close)

	// Create our driver
	driver, err := NewDriver(Config{
		URL: testServer.URL,
	}, logging.Testing())
	require.NoError(t, err)

	// We will insert numberOfLogs logs split across numberOfModules modules
	const (
		numberOfLogs    = 50
		numberOfModules = 2
	)
	logs := make([]drivers.LogWithLedger, numberOfLogs)
	for i := 0; i < numberOfLogs; i++ {
		logs[i] = drivers.NewLogWithLedger(
			fmt.Sprintf("module%d", i%numberOfModules),
			ledger.NewLog(ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}),
		)
	}

	// Send all logs to the driver
	itemsErrors, err := driver.Accept(context.TODO(), logs...)
	require.NoError(t, err)
	require.Len(t, itemsErrors, numberOfLogs)
	for index := range logs {
		require.Nil(t, itemsErrors[index])
	}

	// Ensure data has been inserted
	select {
	case receivedMessages := <-messages:
		require.Len(t, receivedMessages, numberOfLogs)
	default:
		require.Fail(t, fmt.Sprintf("should have received %d messages", numberOfLogs))
	}
}

func TestHTTPDriverClosesResponseBodies(t *testing.T) {
	t.Parallel()

	// Every connection the driver opens must be closed once Accept returns. An unread,
	// unclosed body keeps the connection and its transport goroutines alive for the life
	// of the process, one per push.
	var opened, closed atomic.Int32
	testServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))
	testServer.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			opened.Add(1)
		case http.StateClosed:
			closed.Add(1)
		}
	}
	testServer.Start()
	t.Cleanup(testServer.Close)

	driver, err := NewDriver(Config{URL: testServer.URL}, logging.Testing())
	require.NoError(t, err)

	const pushes = 20
	for i := 0; i < pushes; i++ {
		_, err := driver.Accept(context.TODO(), drivers.NewLogWithLedger(
			"module",
			ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()}),
		))
		require.NoError(t, err)
	}

	require.EqualValues(t, pushes, opened.Load())
	require.Eventually(t, func() bool {
		return closed.Load() == opened.Load()
	}, 5*time.Second, 10*time.Millisecond, "opened %d connections, closed %d", opened.Load(), closed.Load())
}

func TestHTTPDriverReturnsWithoutReadingBody(t *testing.T) {
	t.Parallel()

	// Accept must complete as soon as the status line is known, whatever the exporter
	// does with the body afterwards, and the next push must still work.
	for _, tc := range []struct {
		name       string
		status     int
		body       func(w http.ResponseWriter, r *http.Request)
		expectsErr bool
	}{
		{
			name:   "stalled body after 202",
			status: http.StatusAccepted,
			body: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("{"))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			},
		},
		{
			name:   "stalled body after 500",
			status: http.StatusInternalServerError,
			body: func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte("{"))
				w.(http.Flusher).Flush()
				<-r.Context().Done()
			},
			expectsErr: true,
		},
		{
			name:   "endless body after 202",
			status: http.StatusAccepted,
			body: func(w http.ResponseWriter, r *http.Request) {
				chunk := make([]byte, 32<<10)
				for {
					if _, err := w.Write(chunk); err != nil {
						return
					}
					w.(http.Flusher).Flush()
					select {
					case <-r.Context().Done():
						return
					default:
					}
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.WriteHeader(tc.status)
				tc.body(w, r)
			}))
			t.Cleanup(func() {
				testServer.CloseClientConnections()
				testServer.Close()
			})

			driver, err := NewDriver(Config{URL: testServer.URL}, logging.Testing())
			require.NoError(t, err)
			log := drivers.NewLogWithLedger("module", ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()}))

			for i := 0; i < 2; i++ {
				done := make(chan error, 1)
				go func() {
					_, err := driver.Accept(context.TODO(), log)
					done <- err
				}()
				select {
				case err := <-done:
					if tc.expectsErr {
						require.Error(t, err)
					} else {
						require.NoError(t, err)
					}
				case <-time.After(5 * time.Second):
					require.Fail(t, "Accept did not return: it must not wait for the response body")
				}
			}
		})
	}
}
