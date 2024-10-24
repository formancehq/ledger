//go:build it

package bucket_test

import (
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"go.opentelemetry.io/otel/trace/noop"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuckets(t *testing.T) {
	ctx := logging.TestingContext()
	name := uuid.NewString()[:8]

	pgDatabase := srv.NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	require.NoError(t, driver.Migrate(ctx, db))

	b := bucket.New(db, name)
	require.NoError(t, b.Migrate(ctx, noop.Tracer{}))
}
