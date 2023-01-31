package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/formancehq/webhooks/cmd/flag"
	webhooks "github.com/formancehq/webhooks/pkg"
	"github.com/formancehq/webhooks/pkg/storage/postgres"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
)

func TestStore(t *testing.T) {
	flagSet := pflag.NewFlagSet("storage test", pflag.ContinueOnError)
	_, err := flag.Init(flagSet)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sqldb := sql.OpenDB(
		pgdriver.NewConnector(
			pgdriver.WithDSN(viper.GetString(flag.StoragePostgresConnString))))
	db := bun.NewDB(sqldb, pgdialect.New())
	defer db.Close()

	require.NoError(t, db.Ping())

	// Cleanup tables
	require.NoError(t, db.ResetModel(ctx, (*webhooks.Config)(nil)))
	require.NoError(t, db.ResetModel(ctx, (*webhooks.Attempt)(nil)))

	store, err := postgres.NewStore()
	require.NoError(t, err)

	cfgs, err := store.FindManyConfigs(context.Background(), map[string]any{})
	require.NoError(t, err)
	require.Equal(t, 0, len(cfgs))

	ids, err := store.FindWebhookIDsToRetry(context.Background())
	require.NoError(t, err)
	require.Equal(t, 0, len(ids))

	atts, err := store.FindAttemptsToRetryByWebhookID(context.Background(), "")
	require.NoError(t, err)
	require.Equal(t, 0, len(atts))
}
