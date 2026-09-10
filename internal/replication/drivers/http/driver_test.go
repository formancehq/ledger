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

func TestHTTPDriverReusesConnections(t *testing.T) {
	// Not parallel: the driver uses http.DefaultClient, whose transport is shared with the
	// other tests in this package; the assertion below is about connection reuse.

	var newConns atomic.Int32
	testServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"accepted":true}`))
	}))
	testServer.Config.ConnState = func(c net.Conn, state http.ConnState) {
		t.Logf("conn %s -> %s", c.RemoteAddr(), state)
		if state == http.StateNew {
			newConns.Add(1)
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

	// Each push must drain and close the response body, otherwise the keep-alive
	// connection cannot be reused and every push opens (and leaks) a new socket.
	require.EqualValues(t, 1, newConns.Load(), "expected a single reused connection for %d pushes", pushes)
}

func TestHTTPDriverBoundsResponseDrain(t *testing.T) {
	t.Parallel()

	// The exporter streams an endless body; Accept must still return promptly and the
	// driver must keep working for the next push.
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
		chunk := make([]byte, 32<<10)
		for {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			default:
			}
		}
	}))
	t.Cleanup(func() {
		// Force the streaming handler to stop even if a client is still reading.
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
			require.NoError(t, err)
		case <-time.After(10 * time.Second):
			require.Fail(t, "Accept did not return: response body draining is not bounded")
		}
	}
}

func TestHTTPDriverDrainIsTimeBounded(t *testing.T) {
	t.Parallel()

	// The exporter sends headers and a few bytes, then stalls without closing. Accept
	// must give up on the body after the drain timeout and the next push must still work.
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("{"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		<-r.Context().Done()
	}))
	t.Cleanup(func() {
		testServer.CloseClientConnections()
		testServer.Close()
	})

	driver, err := NewDriver(Config{URL: testServer.URL}, logging.Testing())
	require.NoError(t, err)
	driver.drainTimeout = 200 * time.Millisecond
	log := drivers.NewLogWithLedger("module", ledger.NewLog(ledger.CreatedTransaction{Transaction: ledger.NewTransaction()}))

	for i := 0; i < 2; i++ {
		done := make(chan error, 1)
		go func() {
			_, err := driver.Accept(context.TODO(), log)
			done <- err
		}()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			require.Fail(t, "Accept did not return: response body draining is not time-bounded")
		}
	}
}
