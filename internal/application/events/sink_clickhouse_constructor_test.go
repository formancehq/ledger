//go:build clickhouse

package events

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/stretchr/testify/require"
)

type clickHouseConstructorConn struct {
	driver.Conn

	pingErr    error
	execErr    error
	execCalls  int
	closeCalls int
}

func (c *clickHouseConstructorConn) Ping(context.Context) error {
	return c.pingErr
}

func (c *clickHouseConstructorConn) Exec(context.Context, string, ...any) error {
	c.execCalls++

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
