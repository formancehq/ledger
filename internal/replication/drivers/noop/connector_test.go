package noop

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	ledger "github.com/formancehq/ledger/internal"
	ingester "github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestNoOpConnector(t *testing.T) {
	t.Parallel()

	ctx := context.TODO()

	// Create our connector
	connector, err := NewConnector(drivers.NewServiceConfig(uuid.NewString(), testing.Verbose()), struct{}{}, logging.Testing())
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
	logs := make([]ingester.LogWithLedger, numberOfLogs)
	for i := 0; i < numberOfLogs; i++ {
		logs[i] = ingester.NewLogWithLedger(
			fmt.Sprintf("module%d", i%numberOfModules),
			ledger.NewLog(ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}),
		)
	}

	// Send all logs to the connector
	itemsErrors, err := connector.Accept(ctx, logs...)
	require.NoError(t, err)
	require.Len(t, itemsErrors, numberOfLogs)
	for index := range logs {
		require.Nil(t, itemsErrors[index])
	}
}
