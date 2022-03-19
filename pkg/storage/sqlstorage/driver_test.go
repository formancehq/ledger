package sqlstorage

import (
	"context"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewOpenCloseDBDriver(t *testing.T) {
	d := NewOpenCloseDBDriver("sqlite", SQLite, func() (DB, error) {
		return &sqliteDB{
			directory: os.TempDir(),
			dbName:    uuid.New(),
		}, nil
	})
	err := d.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	defer d.Close(context.Background())

	store, err := d.NewStore(context.Background(), "foo")
	if !assert.NoError(t, err) {
		return
	}

	_, err = store.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	store.Close(context.Background())

	_, err = store.(*Store).schema.Query("select * from transactions")
	if !assert.NotNil(t, err) {
		return
	}
	assert.Equal(t, "sql: database is closed", err.Error())
}

func TestNewCachedDBDriver(t *testing.T) {
	d := NewCachedDBDriver("sqlite", SQLite, func() (DB, error) {
		return &sqliteDB{
			directory: os.TempDir(),
			dbName:    uuid.New(),
		}, nil
	})
	defer d.Close(context.Background())

	err := d.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	defer d.Close(context.Background())

	store, err := d.NewStore(context.Background(), "foo")
	if !assert.NoError(t, err) {
		return
	}
	store.Close(context.Background())

	_, err = store.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	_, err = store.(*Store).schema.Query("select * from transactions")
	if !assert.NoError(t, err, "database should have been closed") {
		return
	}
}
