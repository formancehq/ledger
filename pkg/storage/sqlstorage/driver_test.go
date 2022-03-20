package sqlstorage

import (
	"context"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewDriver(t *testing.T) {
	d := NewDriver("sqlite", SQLite, &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
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

	_, err = store.(*Store).schema.QueryContext(context.Background(), "select * from transactions")
	if !assert.NotNil(t, err) {
		return
	}
	assert.Equal(t, "sql: database is closed", err.Error())
}
