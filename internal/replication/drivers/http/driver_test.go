package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/logging"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/replication/drivers"
)

func TestHTTPDriver(t *testing.T) {
	t.Parallel()

	messages := make(chan []drivers.LogWithLedger, 1)
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		newMessages := make([]drivers.LogWithLedger, 0)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&newMessages))

		messages <- newMessages
	}))
	t.Cleanup(testServer.Close)

	// Create our driver
	driver, err := NewDriver(Config{
		URL: testServer.URL,
	}, logging.Testing())
	require.NoError(t, err)

	// We will insert numberOfLogs logs split across numberOfModules modules
	const (
		numberOfLogs    = 50
		numberOfModules = 2
	)
	logs := make([]drivers.LogWithLedger, numberOfLogs)
	for i := 0; i < numberOfLogs; i++ {
		logs[i] = drivers.NewLogWithLedger(
			fmt.Sprintf("module%d", i%numberOfModules),
			ledger.NewLog(ledger.CreatedTransaction{
				Transaction: ledger.NewTransaction(),
			}),
		)
	}

	// Send all logs to the driver
	itemsErrors, err := driver.Accept(context.TODO(), logs...)
	require.NoError(t, err)
	require.Len(t, itemsErrors, numberOfLogs)
	for index := range logs {
		require.Nil(t, itemsErrors[index])
	}

	// Ensure data has been inserted
	select {
	case receivedMessages := <-messages:
		require.Len(t, receivedMessages, numberOfLogs)
	default:
		require.Fail(t, fmt.Sprintf("should have received %d messages", numberOfLogs))
	}
}
