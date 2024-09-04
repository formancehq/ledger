package bucket

import (
	"testing"

	"github.com/formancehq/go-libs/bun/bunconnect"

	"github.com/formancehq/go-libs/logging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestBuckets(t *testing.T) {
	ctx := logging.TestingContext()
	name := uuid.NewString()[:8]

	<-srv.Done()

	pgDatabase := srv.GetValue().NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	bucket := New(db, name)
	require.NoError(t, bucket.Migrate(ctx))
}
