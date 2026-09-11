//go:build integration

package mirror

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// blockedPostgresSource holds a real relation lock across lifecycle assertions.
// The observer connection is separate from both the lock holder and source pool.
type blockedPostgresSource struct {
	observer        *pgx.Conn
	lock            pgx.Tx
	applicationName string
	dsn             string
}

func newBlockedPostgresSource(t *testing.T) *blockedPostgresSource {
	t.Helper()

	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16.4-alpine",
			Env:          map[string]string{"POSTGRES_USER": "mirror", "POSTGRES_PASSWORD": "mirror", "POSTGRES_DB": "mirror"},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, container.Terminate(context.Background())) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	dsn := fmt.Sprintf("postgres://mirror:mirror@%s/mirror?sslmode=disable", net.JoinHostPort(host, port.Port()))
	observer, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, observer.Close(context.Background())) })
	_, err = observer.Exec(ctx, `
 CREATE SCHEMA _system;
 CREATE TABLE _system.ledgers (name text PRIMARY KEY, bucket text NOT NULL);
 INSERT INTO _system.ledgers VALUES ('source-ledger', 'source_bucket');
 CREATE SCHEMA source_bucket;
 CREATE TABLE source_bucket.logs (id bigint NOT NULL, ledger text NOT NULL,
 type text NOT NULL, date timestamptz NOT NULL, data jsonb NOT NULL, hash bytea);`)
	require.NoError(t, err)
	holder, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, holder.Close(context.Background())) })
	lock, err := holder.Begin(ctx)
	require.NoError(t, err)
	_, err = lock.Exec(ctx, "LOCK TABLE _system.ledgers IN ACCESS EXCLUSIVE MODE")
	require.NoError(t, err)
	fixture := &blockedPostgresSource{observer: observer, lock: lock, applicationName: container.GetContainerID()[:16]}
	u, err := url.Parse(dsn)
	require.NoError(t, err)
	q := u.Query()
	q.Set("application_name", fixture.applicationName)
	q.Set("statement_timeout", "0")
	q.Set("lock_timeout", "0")
	u.RawQuery = q.Encode()
	fixture.dsn = u.String()
	t.Cleanup(func() { fixture.unlock(t) })

	return fixture
}

func (f *blockedPostgresSource) unlock(t *testing.T) {
	t.Helper()

	if f.lock != nil {
		require.NoError(t, f.lock.Rollback(context.Background()))
		f.lock = nil
	}
}

func (f *blockedPostgresSource) ledgerInfo() *commonpb.LedgerInfo {
	return &commonpb.LedgerInfo{
		Name: "postgres-mirror",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource: &commonpb.MirrorSourceConfig{
			LedgerName: "source-ledger",
			Type:       &commonpb.MirrorSourceConfig_Postgres{Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: f.dsn}},
		},
	}
}

func (f *blockedPostgresSource) requireBlocked(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var blocked bool
		err := f.observer.QueryRow(ctx, `SELECT EXISTS (
   SELECT 1 FROM pg_stat_activity a JOIN pg_locks l ON l.pid = a.pid
   WHERE a.application_name = $1 AND a.state = 'active'
   AND a.query = 'SELECT bucket FROM _system.ledgers WHERE name = $1'
   AND a.wait_event_type = 'Lock' AND l.locktype = 'relation'
   AND l.relation = '_system.ledgers'::regclass AND NOT l.granted
  )`, f.applicationName).Scan(&blocked)

		return err == nil && blocked
	}, 10*time.Second, 10*time.Millisecond, "source bucket lookup must be waiting for the fixture's relation lock")
}

func (f *blockedPostgresSource) requireSourceClosed(t *testing.T) {
	t.Helper()

	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		var count int
		err := f.observer.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", f.applicationName).Scan(&count)

		return err == nil && count == 0
	}, 5*time.Second, 10*time.Millisecond, "source shutdown must close all PostgreSQL connections")
}

func TestManager_StopCancelsBlockedPostgresStartup(t *testing.T) {
	t.Parallel()

	fixture := newBlockedPostgresSource(t)
	builder, store := newTestBuilder(t)
	saveLedgerInfo(t, store, fixture.ledgerInfo())
	m := newTestManager(t, store, builder)
	// This cleanup runs before newTestManager's Stop, so unfixed code cannot
	// strand the suite while joining a source blocked on the fixture lock.
	stopped := make(chan struct{})
	stopStarted := false
	t.Cleanup(func() {
		fixture.unlock(t)
		if stopStarted {
			select {
			case <-stopped:
			case <-time.After(10 * time.Second):
				t.Error("manager did not stop after fixture lock release")
			}
		}
	})
	m.OnLeadershipChange(true)
	fixture.requireBlocked(t)
	stopStarted = true
	go func() {
		m.Stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop must interrupt blocked PostgreSQL startup without releasing the database lock")
	}
	fixture.requireSourceClosed(t)
	require.NotNil(t, fixture.lock, "fixture lock must remain held through cancellation assertions")
	require.Empty(t, workerNames(m))
}

func TestManager_LeadershipLossCancelsBlockedPostgresStartup(t *testing.T) {
	t.Parallel()

	fixture := newBlockedPostgresSource(t)
	builder, store := newTestBuilder(t)
	saveLedgerInfo(t, store, mirrorLedgerInfo("http-mirror", quietV2Source(t)))
	m := newTestManager(t, store, builder)
	t.Cleanup(func() { fixture.unlock(t) })
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "http-mirror")
	m.mu.Lock()
	previous := m.workers["http-mirror"]
	m.mu.Unlock()

	// Add the blocked source within the same generation, retaining the HTTP
	// worker until loss. Another leadership callback here would tear it down.
	saveLedgerInfo(t, store, fixture.ledgerInfo())
	m.notifications.NotifyConfigChanged()
	fixture.requireBlocked(t)
	lost := make(chan struct{})
	go func() {
		m.OnLeadershipChange(false)
		close(lost)
	}()
	select {
	case <-lost:
	case <-time.After(5 * time.Second):
		t.Fatal("leadership callback must return while PostgreSQL startup is blocked")
	}
	select {
	case <-previous.w.StopCh():
	case <-time.After(5 * time.Second):
		t.Fatal("leadership loss must unblock initialization and stop the existing HTTP worker")
	}
	fixture.requireSourceClosed(t)
	require.NotNil(t, fixture.lock, "fixture lock must remain held through cancellation assertions")
	requireWorkerNames(t, m)

	fixture.unlock(t)
	m.OnLeadershipChange(true)
	requireWorkerNames(t, m, "http-mirror", "postgres-mirror")
	m.mu.Lock()
	current := m.workers["http-mirror"]
	m.mu.Unlock()
	require.NotSame(t, previous, current, "regained leadership must create a fresh worker generation")
	m.Stop()
	fixture.requireSourceClosed(t)
}
