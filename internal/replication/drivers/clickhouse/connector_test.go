//go:build it

package clickhouse

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/docker"
	"github.com/formancehq/go-libs/v2/testing/platform/clickhousetesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClickhouseConnector(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	// Start a new clickhouse server
	dockerPool := docker.NewPool(t, logging.Testing())
	srv := clickhousetesting.CreateServer(dockerPool)

	// Create our connector
	connector, err := NewConnector(drivers.NewServiceConfig(uuid.NewString(), testing.Verbose()), Config{
		DSN: srv.GetDSN(),
	}, logging.Testing())
	require.NoError(t, err)
	require.NoError(t, connector.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, connector.Stop(ctx))
	})

	// We will insert numberOfLogs logs split across numberOfModules modules
	const (
		numberOfLogs    = 50
		numberOfModules = 2
	)
	logs := make([]replication.LogWithLedger, numberOfLogs)
	for i := 0; i < numberOfLogs; i++ {
		log := ledger.NewLog(ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction(),
		})
		log.ID = i
		logs[i] = replication.NewLogWithLedger(
			fmt.Sprintf("module%d", i%numberOfModules),
			log,
		)
	}

	// Send all logs to the connector
	itemsErrors, err := connector.Accept(ctx, logs...)
	require.NoError(t, err)
	require.Len(t, itemsErrors, numberOfLogs)
	for index := range logs {
		require.Nil(t, itemsErrors[index])
	}

	// Ensure data has been inserted
	require.Equal(t, numberOfLogs, count(t, ctx, connector, `select count(*) from logs`))
}

func count(t *testing.T, ctx context.Context, connector *Connector, query string) int {
	rows, err := connector.db.Query(ctx, query)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()
	require.True(t, rows.Next())
	var count uint64
	require.NoError(t, rows.Scan(&count))

	return int(count)
}
