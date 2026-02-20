//go:build it

package bucket_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/formancehq/go-libs/v4/bun/bunconnect"
	"github.com/formancehq/go-libs/v4/bun/bundebug"
	"github.com/formancehq/go-libs/v4/logging"

	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/system"
)

func TestBuckets(t *testing.T) {
	ctx := logging.TestingContext()
	name := uuid.NewString()[:8]

	pgDatabase := srv.NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	if testing.Verbose() {
		db.AddQueryHook(bundebug.NewQueryHook())
	}

	require.NoError(t, system.Migrate(ctx, db))

	b := bucket.NewDefault(noop.Tracer{}, name)
	require.NoError(t, b.Migrate(ctx, db))
}
