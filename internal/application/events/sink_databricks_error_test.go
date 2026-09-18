//go:build databricks

package events

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

// Only Connect is used: database/sql cannot execute a statement when opening
// the connection fails. This external driver interface is not mockgen-managed.
type databricksPublishFailureConnector struct {
	driver.Connector

	cause error
}

func (c databricksPublishFailureConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, c.cause
}

func TestDatabricksSinkPublishSanitizesDriverError(t *testing.T) {
	t.Parallel()
	cause := errors.New("warehouse.example rejected token databricks-secret")
	db := sql.OpenDB(databricksPublishFailureConnector{cause: cause})
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	sink := &DatabricksSink{
		db:             db,
		qualifiedTable: "main.default.events",
		errors:         newSinkErrorSanitizer(nil, "databricks-secret"),
	}
	err := sink.Publish(t.Context(), []*eventspb.Event{{LogSequence: 42}})
	require.ErrorIs(t, err, cause)
	require.Contains(t, err.Error(), "inserting events into Databricks")
	require.Contains(t, err.Error(), "warehouse.example rejected token")
	require.NotContains(t, err.Error(), "databricks-secret")
}
