package sqlstorage

import (
	"context"
	"os"
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewDriver(t *testing.T) {
	d := NewDriver("sqlite", &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
	})

	assert.NoError(t, d.Initialize(context.Background()))

	appId, err := d.AppID(context.Background())
	require.NoError(t, err)
	require.NotEmpty(t, appId)

	defer func(d *Driver, ctx context.Context) {
		assert.NoError(t, d.Close(ctx))
	}(d, context.Background())

	store, _, err := d.GetStore(context.Background(), "foo", true)
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	assert.NoError(t, store.Close(context.Background()))

	_, err = store.(*Store).schema.QueryContext(context.Background(), "select * from transactions")
	assert.Error(t, err)
	assert.Equal(t, "sql: database is closed [UNKNOWN]", err.Error())
}
