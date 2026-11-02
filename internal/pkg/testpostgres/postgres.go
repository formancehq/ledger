//go:build integration

// Package testpostgres provides isolated PostgreSQL databases for integration tests.
package testpostgres

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// NewDatabase creates an isolated database and registers its cleanup.
// LEDGER_TEST_POSTGRES_DSN must identify a disposable PostgreSQL administrator.
// Without that override, tests use PostgreSQL 16.4 through testcontainers.
func NewDatabase(t testing.TB) (string, *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	dsn := os.Getenv("LEDGER_TEST_POSTGRES_DSN")
	if dsn == "" {
		container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image:        "postgres:16.4-alpine",
				Env:          map[string]string{"POSTGRES_USER": "mirror", "POSTGRES_PASSWORD": "mirror", "POSTGRES_DB": "mirror"},
				ExposedPorts: []string{"5432/tcp"},
				WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(time.Minute),
			}, Started: true,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			require.NoError(t, container.Terminate(cleanupCtx))
		})
		host, err := container.Host(ctx)
		require.NoError(t, err)
		port, err := container.MappedPort(ctx, "5432/tcp")
		require.NoError(t, err)
		dsn = fmt.Sprintf("postgres://mirror:mirror@%s/mirror?sslmode=disable", net.JoinHostPort(host, port.Port()))
	}
	admin, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		require.NoError(t, admin.Close(cleanupCtx))
	})
	var entropy [12]byte
	_, err = rand.Read(entropy[:])
	require.NoError(t, err)
	database := fmt.Sprintf("mirror_gap_%x", entropy)
	_, err = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{database}.Sanitize())
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, dropErr := admin.Exec(cleanupCtx, "DROP DATABASE "+pgx.Identifier{database}.Sanitize()+" WITH (FORCE)")
		require.NoError(t, dropErr)
	})
	databaseURL, err := url.Parse(dsn)
	require.NoError(t, err)
	require.Contains(t, []string{"postgres", "postgresql"}, databaseURL.Scheme, "test override must use a PostgreSQL URI")
	databaseURL.Path = "/" + database
	databaseURL.RawPath = ""
	query := databaseURL.Query()
	query.Del("dbname")
	databaseURL.RawQuery = query.Encode()
	databaseDSN := databaseURL.String()
	connection, err := pgx.Connect(ctx, databaseDSN)
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		require.NoError(t, connection.Close(cleanupCtx))
	})
	var actualDatabase string
	require.NoError(t, connection.QueryRow(ctx, "SELECT current_database()").Scan(&actualDatabase))
	require.Equal(t, database, actualDatabase, "fixture must never write into the shared administration database")

	return databaseDSN, connection
}
