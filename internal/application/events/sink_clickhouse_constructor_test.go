//go:build clickhouse

package events

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

type clickHouseConstructorConn struct {
	driver.Conn

	ping       func(context.Context) error
	exec       func(context.Context) error
	pingErr    error
	execErr    error
	execCalls  int
	closeCalls int
}

func (c *clickHouseConstructorConn) Ping(ctx context.Context) error {
	if c.ping != nil {
		return c.ping(ctx)
	}

	return c.pingErr
}

func (c *clickHouseConstructorConn) Exec(ctx context.Context, _ string, _ ...any) error {
	c.execCalls++
	if c.exec != nil {
		return c.exec(ctx)
	}

	return c.execErr
}

func (c *clickHouseConstructorConn) Close() error {
	c.closeCalls++

	return nil
}

func TestInitializeClickHouseSink_ClosesConnectionOnFailure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		pingErr       error
		execErr       error
		wantExecCalls int
	}{
		{
			name:          "ping",
			pingErr:       errors.New("clickhouse unavailable"),
			wantExecCalls: 0,
		},
		{
			name:          "create table",
			execErr:       errors.New("create table failed"),
			wantExecCalls: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			conn := &clickHouseConstructorConn{
				pingErr: tc.pingErr,
				execErr: tc.execErr,
			}

			sink, err := initializeClickHouseSink(t.Context(), conn, "ledger_events_test")
			require.Error(t, err)
			require.Nil(t, sink)
			require.Equal(t, tc.wantExecCalls, conn.execCalls)
			require.Equal(t, 1, conn.closeCalls,
				"each failed construction attempt must release its opened connection")
		})
	}
}

func TestInitializeClickHouseSink_TransfersConnectionOwnershipOnSuccess(t *testing.T) {
	t.Parallel()

	conn := &clickHouseConstructorConn{}
	sink, err := initializeClickHouseSink(t.Context(), conn, "")
	require.NoError(t, err)
	require.Equal(t, defaultClickHouseTable, sink.table)
	require.Equal(t, 1, conn.execCalls)
	require.Zero(t, conn.closeCalls, "a live sink must retain its connection")

	require.NoError(t, sink.Close())
	require.Equal(t, 1, conn.closeCalls)
}

// The driver interface is not mockgen-managed; reuse the constructor fixture
// to inject a database error before a batch can be allocated.
type clickHousePublishErrorConn struct {
	clickHouseConstructorConn

	prepareErr error
}

func (c *clickHousePublishErrorConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	return nil, c.prepareErr
}

func TestClickHouseSinkPublishSanitizesDriverError(t *testing.T) {
	t.Parallel()
	cause := errors.New("authentication failed for password=warehouse-secret at warehouse.example")
	sink := &ClickHouseSink{
		conn:   &clickHousePublishErrorConn{prepareErr: cause},
		table:  "ledger_events",
		errors: newSinkErrorSanitizer([]string{"clickhouse://user:warehouse-secret@warehouse.example"}),
	}
	err := sink.Publish(t.Context(), []*eventspb.Event{{LogSequence: 42}})
	require.Error(t, err)
	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), "preparing ClickHouse batch")
	require.Contains(t, err.Error(), "authentication failed")
	require.Contains(t, err.Error(), "warehouse.example")
	require.NotContains(t, err.Error(), "warehouse-secret")
}

func TestClickHouseConstructorSanitizesNestedProxy(t *testing.T) {
	t.Parallel()
	proxy := "http://alice:proxy-password@localhost:invalid-port"
	sink, err := NewClickHouseSink(context.Background(), ClickHouseSinkConfig{DSN: "http://localhost:8123/default?http_proxy=" + url.QueryEscape(proxy)})
	require.Nil(t, sink)
	require.ErrorContains(t, err, "http_proxy")
	require.ErrorContains(t, err, "invalid port")
	require.NotContains(t, err.Error(), "proxy-password")
}

func TestInitializeClickHouseSink_CancellationClosesConnection(t *testing.T) {
	t.Parallel()
	for _, stage := range []string{"ping", "create table"} {
		t.Run(stage, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entered := make(chan struct{})
			block := func(callCtx context.Context) error {
				close(entered)
				<-callCtx.Done()

				return callCtx.Err()
			}
			conn := &clickHouseConstructorConn{}
			if stage == "ping" {
				conn.ping = block
			} else {
				conn.exec = block
			}
			result := make(chan error, 1)
			go func() {
				sink, err := initializeClickHouseSink(ctx, conn, "events")
				if sink != nil {
					_ = sink.Close() // Avoid leaking an unexpected successful construction.
				}
				result <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("initialization did not reach blocking operation")
			}
			cancel()
			select {
			case err := <-result:
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, 1, conn.closeCalls)
				if stage == "ping" {
					require.Zero(t, conn.execCalls)
				} else {
					require.Equal(t, 1, conn.execCalls)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("initialization ignored cancellation")
			}
		})
	}
}

// The HTTP driver uses the supplied context for its initialization request.
// This exercises the registered factory and real transport without ClickHouse.
func TestClickHouseFactory_CancelsInitialization(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body) // Consume the request before observing client disconnect.
		close(entered)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		sink, err := sinkFactories["clickhouse"](ctx, &ledgerpb.SinkConfig{Type: &ledgerpb.SinkConfig_Clickhouse{
			Clickhouse: &ledgerpb.ClickHouseSinkConfig{Dsn: server.URL + "/default?dial_timeout=1s&read_timeout=30s"},
		}}, FormatJSON)
		if err == nil && sink != nil {
			_ = sink.Close() // Avoid leaking an unexpected successful construction.
		}
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("factory did not start its initialization request")
	}
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("factory initialization ignored cancellation")
	}
}
