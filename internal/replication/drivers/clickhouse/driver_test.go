//go:build it

package clickhouse

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/testing/docker"
	"github.com/formancehq/go-libs/v3/testing/platform/clickhousetesting"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestClickhouseDriver(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	// Start a new clickhouse server
	dockerPool := docker.NewPool(t, logging.Testing())
	srv := clickhousetesting.CreateServer(dockerPool, clickhousetesting.WithVersion("24.12"))

	// Create our driver
	driver, err := NewDriver(Config{
		DSN: srv.GetDSN(),
	}, logging.Testing())
	require.NoError(t, err)
	require.NoError(t, driver.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, driver.Stop(ctx))
	})

	// We will insert numberOfLogs logs split across numberOfModules modules
	const (
		numberOfLogs    = 50
		numberOfModules = 2
	)
	now := time.Now()
	logs := make([]drivers.LogWithLedger, numberOfLogs)
	for i := 0; i < numberOfLogs; i++ {
		log := ledger.NewLog(ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().
				WithInsertedAt(now).
				WithTimestamp(now),
		})
		log.ID = pointer.For(uint64(i))
		log.Date = now
		logs[i] = drivers.NewLogWithLedger(
			fmt.Sprintf("module%d", i%numberOfModules),
			log,
		)
	}

	// Send all logs to the driver
	itemsErrors, err := driver.Accept(ctx, logs...)
	require.NoError(t, err)
	require.Len(t, itemsErrors, numberOfLogs)
	for index := range logs {
		require.Nil(t, itemsErrors[index])
	}

	// Ensure data has been inserted
	require.Equal(t, numberOfLogs, count(t, ctx, driver, `select count(*) from logs`))
	_, err = readLogs(ctx, driver.db)
	require.NoError(t, err)
}

func readLogs(ctx context.Context, client driver.Conn) ([]drivers.LogWithLedger, error) {
	rows, err := client.Query(ctx, "select ledger, id, type, date, toJSONString(data) from logs final")
	if err != nil {
		return nil, err
	}

	ret := make([]drivers.LogWithLedger, 0)
	for rows.Next() {
		var (
			payload string
			id      int64
		)
		newLog := drivers.LogWithLedger{}
		if err := rows.Scan(&newLog.Ledger, &id, &newLog.Type, &newLog.Date, &payload); err != nil {
			return nil, errors.Wrap(err, "scanning data from database")
		}
		newLog.ID = pointer.For(uint64(id))

		newLog.Data, err = ledger.HydrateLog(newLog.Type, []byte(payload))
		if err != nil {
			return nil, errors.Wrap(err, "hydrating log data")
		}

		ret = append(ret, newLog)
	}

	return ret, nil
}

func count(t *testing.T, ctx context.Context, driver *Driver, query string) int {
	rows, err := driver.db.Query(ctx, query)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, rows.Close())
	}()
	require.True(t, rows.Next())
	var count uint64
	require.NoError(t, rows.Scan(&count))

	return int(count)
}
