package sqlstorage

import (
	"context"
	"github.com/numary/ledger/pkg/logging"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewOpenCloseDBDriver(t *testing.T) {
	d := NewOpenCloseDBDriver(logging.DefaultLogger(), "sqlite", SQLite, func(name string) string {
		return SQLiteMemoryConnString
	})
	err := d.Initialize(context.Background())
	assert.NoError(t, err)
	defer d.Close(context.Background())

	store, err := d.NewStore("foo")
	assert.NoError(t, err)

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	store.Close(context.Background())

	_, err = store.(*Store).db.Query("select * from transactions")
	assert.NotNil(t, err)
	assert.Equal(t, "sql: database is closed", err.Error())
}

func TestNewCachedDBDriver(t *testing.T) {
	d := NewCachedDBDriver(logging.DefaultLogger(), "sqlite", SQLite, SQLiteMemoryConnString)
	err := d.Initialize(context.Background())
	assert.NoError(t, err)
	defer d.Close(context.Background())

	store, err := d.NewStore("foo")
	assert.NoError(t, err)
	store.Close(context.Background())

	_, err = store.Initialize(context.Background())
	assert.NoError(t, err)

	_, err = store.(*Store).db.Query("select * from transactions")
	assert.NoError(t, err, "database should have been closed")
}
