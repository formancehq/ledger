//go:build integration

package mirror

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/pkg/testpostgres"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func newPostgresLogFixture(t testing.TB) (string, *pgx.Conn) {
	t.Helper()
	dsn, observer := testpostgres.NewDatabase(t)
	_, err := observer.Exec(context.Background(), `CREATE SCHEMA _system;
CREATE TABLE _system.ledgers(id bigint PRIMARY KEY, name text UNIQUE NOT NULL, bucket text NOT NULL);
INSERT INTO _system.ledgers VALUES (1,'source-ledger','source_bucket');
CREATE SCHEMA source_bucket;
CREATE SEQUENCE source_bucket.log_id_1;
CREATE TABLE source_bucket.logs(id bigint NOT NULL, ledger text NOT NULL, type text NOT NULL, date timestamptz NOT NULL, data jsonb NOT NULL, hash bytea, PRIMARY KEY(ledger,id));`)
	require.NoError(t, err)

	return dsn, observer
}

func TestPostgresSourceRequirements(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, alter, options, want string
	}{
		{name: "upstream sequence"},
		{name: "cached sequence", alter: "ALTER SEQUENCE source_bucket.log_id_1 CACHE 2", want: "CACHE 1"},
		{name: "nonunit sequence", alter: "ALTER SEQUENCE source_bucket.log_id_1 INCREMENT 2", want: "increment 1"},
		{name: "cycling sequence", alter: "ALTER SEQUENCE source_bucket.log_id_1 CYCLE", want: "NO CYCLE"},
		{name: "missing sequence", alter: "DROP SEQUENCE source_bucket.log_id_1", want: "log sequence"},
		{name: "read only session", options: "-c default_transaction_read_only=on", want: "writable PostgreSQL primary"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dsn, observer := newPostgresLogFixture(t)
			if tc.alter != "" {
				_, err := observer.Exec(context.Background(), tc.alter)
				require.NoError(t, err)
			}
			if tc.options != "" {
				u, err := url.Parse(dsn)
				require.NoError(t, err)
				q := u.Query()
				q.Set("options", tc.options)
				u.RawQuery = q.Encode()
				dsn = u.String()
			}
			source, err := v2.NewPostgresSource(context.Background(), &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
			if tc.want != "" {
				require.ErrorContains(t, err, tc.want)
				require.Nil(t, source)

				return
			}
			require.NoError(t, err)
			require.NoError(t, source.Close())
		})
	}
}

func TestPostgresSourceRejectsSelectOnlyCredentials(t *testing.T) {
	t.Parallel()
	dsn, observer := newPostgresLogFixture(t)
	ctx := context.Background()
	role := observer.Config().Database
	quoted := pgx.Identifier{role}.Sanitize()
	_, err := observer.Exec(ctx, "CREATE ROLE "+quoted)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := observer.Exec(ctx, "DROP OWNED BY "+quoted)
		require.NoError(t, err)
		_, err = observer.Exec(ctx, "DROP ROLE "+quoted)
		require.NoError(t, err)
	})
	_, err = observer.Exec(ctx, fmt.Sprintf("GRANT USAGE ON SCHEMA _system, source_bucket TO %s; GRANT SELECT ON _system.ledgers, source_bucket.logs TO %s", quoted, quoted))
	require.NoError(t, err)
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("options", "-c role="+role)
	u.RawQuery = q.Encode()
	source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: u.String()}, "source-ledger")
	require.ErrorContains(t, err, "SHARE lock privilege")
	require.Nil(t, source)
	_, err = observer.Exec(ctx, "GRANT UPDATE ON source_bucket.logs TO "+quoted)
	require.NoError(t, err)
	source, err = v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: u.String()}, "source-ledger")
	require.NoError(t, err, "SELECT plus UPDATE suffices without sequence usage or table ownership")
	t.Cleanup(func() { require.NoError(t, source.Close()) })
}

func TestPostgresSourceCacheChangeInvalidatesReservations(t *testing.T) {
	t.Parallel()
	dsn, observer := newPostgresLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := observer.Exec(ctx, "ALTER SEQUENCE source_bucket.log_id_1 CACHE 4")
	require.NoError(t, err)
	writer, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, writer.Close(context.Background())) })
	var id, reserved uint64
	require.NoError(t, writer.QueryRow(ctx, "SELECT nextval('source_bucket.log_id_1')").Scan(&id))
	require.Equal(t, uint64(1), id)
	require.NoError(t, observer.QueryRow(ctx, "SELECT last_value FROM source_bucket.log_id_1").Scan(&reserved))
	require.Equal(t, uint64(4), reserved, "the idle writer must have unissued reservations before ALTER")
	// ALTER CACHE transactionally replaces the sequence storage. The existing
	// backend must invalidate its cached 2..4 on its next nextval call.
	_, err = observer.Exec(ctx, "ALTER SEQUENCE source_bucket.log_id_1 CACHE 1")
	require.NoError(t, err)
	var cacheSize int64
	require.NoError(t, observer.QueryRow(ctx, "SELECT cache_size FROM pg_sequences WHERE schemaname='source_bucket' AND sequencename='log_id_1'").Scan(&cacheSize))
	require.Equal(t, int64(1), cacheSize)
	require.NoError(t, observer.QueryRow(ctx, `INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL) RETURNING id`).Scan(&id))
	require.Equal(t, uint64(5), id)
	source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	logs, more, err := source.FetchLogs(ctx, 0, 10)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(5), logs[0].ID)
	// Keep the original backend alive across both ALTER and the locked gap
	// read. It must not insert an old reserved ID behind the mirror boundary.
	require.NoError(t, writer.QueryRow(ctx, `INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL) RETURNING id`).Scan(&id))
	require.Equal(t, uint64(6), id, "the original session must discard its old cached reservations")
	logs, more, err = source.FetchLogs(ctx, 5, 10)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(6), logs[0].ID, "the next committed row must remain available after the permanent gap")
}

func TestPostgresSourceContiguousPageDoesNotLockWriters(t *testing.T) {
	t.Parallel()
	dsn, observer := newPostgresLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := observer.Exec(ctx, `INSERT INTO source_bucket.logs SELECT nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL FROM generate_series(1,3)`)
	require.NoError(t, err)
	source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	writer, err := observer.Begin(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, writer.Rollback(context.Background())) }()
	_, err = writer.Exec(ctx, `INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL)`)
	require.NoError(t, err)
	logs, more, err := source.FetchLogs(ctx, 0, 2)
	require.NoError(t, err, "a contiguous page must not request a SHARE lock against the active later writer")
	require.True(t, more)
	require.Len(t, logs, 2)
	require.Equal(t, uint64(1), logs[0].ID)
	require.Equal(t, uint64(2), logs[1].ID)
	logs, more, err = source.FetchLogs(ctx, 2, 2)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(3), logs[0].ID)
}

func TestPostgresSourceGapConflictReleasesTransaction(t *testing.T) {
	t.Parallel()
	dsn, observer := newPostgresLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := observer.Exec(ctx, `INSERT INTO source_bucket.logs VALUES(3,'source-ledger','SET_METADATA',now(),'{}',NULL); SELECT setval('source_bucket.log_id_1',3)`)
	require.NoError(t, err)
	// One pooled connection makes a leaked failed transaction observable on retry.
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("pool_max_conns", "1")
	u.RawQuery = q.Encode()
	source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: u.String()}, "source-ledger")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	writer, err := observer.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := writer.Rollback(context.Background()); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			require.NoError(t, err)
		}
	})
	_, err = writer.Exec(ctx, "LOCK TABLE source_bucket.logs IN ROW EXCLUSIVE MODE")
	require.NoError(t, err)
	logs, more, err := source.FetchLogs(ctx, 1, 1)
	var conflict *pgconn.PgError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, "55P03", conflict.Code)
	require.Empty(t, logs)
	require.False(t, more)
	require.NoError(t, writer.Rollback(ctx))
	logs, more, err = source.FetchLogs(ctx, 1, 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(3), logs[0].ID)
	// Successful reread also releases its lock before returning to the worker.
	_, err = observer.Exec(ctx, "SET lock_timeout='100ms'; INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL)")
	require.NoError(t, err)
}

func TestPostgresSourceQuotesBucketAndFiltersLedger(t *testing.T) {
	t.Parallel()
	dsn, observer := newPostgresLogFixture(t)
	ctx := context.Background()
	_, err := observer.Exec(ctx, `INSERT INTO source_bucket.logs VALUES(3,'source-ledger','SET_METADATA',now(),'{}',NULL),(999,'other-ledger','SET_METADATA',now(),'{}',NULL);
SELECT setval('source_bucket.log_id_1',3);
ALTER SCHEMA source_bucket RENAME TO "source""bucket";
UPDATE _system.ledgers SET bucket='source"bucket'`)
	require.NoError(t, err)
	source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	logs, more, err := source.FetchLogs(ctx, 1, 1)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(3), logs[0].ID)
	require.Empty(t, logs[0].Hash)
	head, err := source.GetLatestLogID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), head)
}

func TestPostgresSourceGapInsidePage(t *testing.T) {
	t.Parallel()
	dsn, observer := newPostgresLogFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := observer.Exec(ctx, `INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL)`)
	require.NoError(t, err)
	connection, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, connection.Close(context.Background())) })
	writer, err := connection.Begin(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := writer.Rollback(context.Background()); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			require.NoError(t, err)
		}
	})
	_, err = writer.Exec(ctx, `INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL)`)
	require.NoError(t, err)
	_, err = observer.Exec(ctx, `INSERT INTO source_bucket.logs VALUES(nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL)`)
	require.NoError(t, err)
	source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, source.Close()) })
	// The lookahead hole does not affect the safe first page of size one.
	logs, more, err := source.FetchLogs(ctx, 0, 1)
	require.NoError(t, err)
	require.True(t, more)
	require.Len(t, logs, 1)
	require.Equal(t, uint64(1), logs[0].ID)
	// When the page includes the higher row, the entire speculative page is
	// discarded, including its otherwise safe first row.
	logs, more, err = source.FetchLogs(ctx, 0, 100)
	var conflict *pgconn.PgError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, "55P03", conflict.Code)
	require.Empty(t, logs)
	require.False(t, more)
	require.NoError(t, writer.Commit(ctx))
	logs, more, err = source.FetchLogs(ctx, 0, 100)
	require.NoError(t, err)
	require.False(t, more)
	require.Len(t, logs, 3)
	for i, log := range logs {
		require.Equal(t, uint64(i+1), log.ID)
	}
}

func BenchmarkPostgresSourceFetch(b *testing.B) {
	for _, rows := range []int{100, 10000} {
		for _, gap := range []bool{false, true} {
			b.Run(fmt.Sprintf("rows=%d/gap=%t", rows, gap), func(b *testing.B) {
				dsn, observer := newPostgresLogFixture(b)
				ctx := context.Background()
				_, err := observer.Exec(ctx, `INSERT INTO source_bucket.logs SELECT nextval('source_bucket.log_id_1'),'source-ledger','SET_METADATA',now(),'{}',NULL FROM generate_series(1,$1::int)`, rows)
				require.NoError(b, err)
				if gap {
					_, err = observer.Exec(ctx, "DELETE FROM source_bucket.logs WHERE id=50")
					require.NoError(b, err)
				}
				source, err := v2.NewPostgresSource(ctx, &commonpb.PostgresMirrorSourceConfig{Dsn: dsn}, "source-ledger")
				require.NoError(b, err)
				b.Cleanup(func() { require.NoError(b, source.Close()) })
				b.ResetTimer()
				for b.Loop() {
					logs, _, err := source.FetchLogs(ctx, 0, 100)
					require.NoError(b, err)
					require.NotEmpty(b, logs)
				}
			})
		}
	}
}
