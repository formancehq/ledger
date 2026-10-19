//go:build databricks

package events

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// These external database/sql driver interfaces are not mockgen-managed.
// A real sql.DB retains ownership and performs connection-pool cleanup.
type databricksConstructorConnector struct {
	driver.Connector

	conn *databricksConstructorConn
}

func (c databricksConstructorConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

type databricksConstructorConn struct {
	driver.Conn

	ping       func(context.Context) error
	exec       func(context.Context) error
	execCalls  int
	closeCalls int
}

func (c *databricksConstructorConn) Ping(ctx context.Context) error {
	if c.ping != nil {
		return c.ping(ctx)
	}

	return nil
}

func (c *databricksConstructorConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	c.execCalls++
	if c.exec != nil {
		return nil, c.exec(ctx)
	}

	return driver.RowsAffected(0), nil
}

func (c *databricksConstructorConn) Close() error {
	c.closeCalls++

	return nil
}

func TestInitializeDatabricksSink_CancellationClosesConnection(t *testing.T) {
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
			conn := &databricksConstructorConn{}
			if stage == "ping" {
				conn.ping = block
			} else {
				conn.exec = block
			}
			db := sql.OpenDB(databricksConstructorConnector{conn: conn})
			result := make(chan error, 1)
			go func() {
				sink, err := initializeDatabricksSink(ctx, db, DatabricksSinkConfig{Catalog: "main", Schema: "default"})
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
				require.NoError(t, db.Close())
				require.Equal(t, 1, conn.closeCalls, "the failed constructor already released its acquired connection")
			case <-time.After(5 * time.Second):
				t.Fatal("initialization ignored cancellation")
			}
		})
	}
}

func TestInitializeDatabricksSink_TransfersConnectionOwnershipOnSuccess(t *testing.T) {
	t.Parallel()
	conn := &databricksConstructorConn{}
	db := sql.OpenDB(databricksConstructorConnector{conn: conn})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	sink, err := initializeDatabricksSink(t.Context(), db, DatabricksSinkConfig{Catalog: "main", Schema: "default"})
	require.NoError(t, err)
	require.Equal(t, "main.default.ledger_events", sink.qualifiedTable)
	require.Equal(t, 1, conn.execCalls)
	require.Zero(t, conn.closeCalls, "a live sink retains its connection")
	require.NoError(t, sink.Close())
	require.Equal(t, 1, conn.closeCalls)
}
