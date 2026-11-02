//go:build integration

package v2

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/testpostgres"
)

type postgresCancellationSQLKey struct{}

// Cancel at the driver boundary after PostgreSQL has granted SHARE, before the
// adapter can issue its fresh-snapshot SELECT. No timer or scheduling race is
// involved in reaching the cancellation branch.
type cancelAfterPostgresShare struct {
	cancel context.CancelFunc
	locked atomic.Bool
	reread atomic.Bool
}

func (c *cancelAfterPostgresShare) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if c.locked.Load() && strings.HasPrefix(data.SQL, "SELECT id, type,") {
		c.reread.Store(true)
	}

	return context.WithValue(ctx, postgresCancellationSQLKey{}, data.SQL)
}

func (c *cancelAfterPostgresShare) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	sql, _ := ctx.Value(postgresCancellationSQLKey{}).(string)
	if data.Err == nil && strings.HasPrefix(sql, "LOCK TABLE ") && c.locked.CompareAndSwap(false, true) {
		c.cancel()
	}
}

func TestPostgresSource_CancelAfterSuccessfulGapLock(t *testing.T) {
	t.Parallel()
	dsn, observer := testpostgres.NewDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := observer.Exec(ctx, `CREATE SCHEMA source_bucket;
CREATE SEQUENCE source_bucket.log_id_1;
CREATE TABLE source_bucket.logs(id bigint NOT NULL, ledger text NOT NULL, type text NOT NULL, date timestamptz NOT NULL, data jsonb NOT NULL, hash bytea, PRIMARY KEY(ledger,id));
INSERT INTO source_bucket.logs VALUES(1,'source-ledger','SET_METADATA',now(),'{}',NULL),(3,'source-ledger','SET_METADATA',now(),'{}',NULL);
SELECT setval('source_bucket.log_id_1',3);`)
	require.NoError(t, err)
	fetchCtx, cancelFetch := context.WithCancel(ctx)
	defer cancelFetch()
	tracer := &cancelAfterPostgresShare{cancel: cancelFetch}
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	config.MaxConns = 1
	config.ConnConfig.Tracer = tracer
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	source := &PostgresSource{pool: pool, ledgerName: "source-ledger", bucket: "source_bucket"}
	logs, more, err := source.FetchLogs(fetchCtx, 1, 1)
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, tracer.locked.Load(), "cancellation must follow a successful source SHARE lock")
	require.True(t, tracer.reread.Load(), "the cancelled operation must be the synchronized reread")
	require.Nil(t, logs)
	require.False(t, more)

	// An unrelated writer must not retain any lock from the failed fetch. Bound
	// the INSERT at the server as well, so a cleanup regression fails directly.
	_, err = observer.Exec(ctx, `SET lock_timeout='500ms'; INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'other-ledger','SET_METADATA',now(),'{}',NULL)`)
	require.NoError(t, err, "cancelled reread must release its SHARE lock")
	// MaxConns=1 also proves cancellation returned or discarded the connection;
	// the source must recover instead of retaining a transaction in its pool.
	logs, more, err = source.FetchLogs(ctx, 1, 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(3), logs[0].ID)
	require.Equal(t, "{}", string(logs[0].Data))
	require.Equal(t, int32(0), pool.Stat().AcquiredConns())
}
