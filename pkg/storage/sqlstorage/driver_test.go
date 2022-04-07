package sqlstorage

import (
	"context"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestNewDriver(t *testing.T) {
	d := NewDriver("sqlite", &sqliteDB{
		directory: os.TempDir(),
		dbName:    uuid.New(),
	})
	err := d.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}
	defer d.Close(context.Background())

	store, _, err := d.GetStore(context.Background(), "foo", true)
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
	assert.Equal(t, "sql: database is closed [UNKNOWN]", err.Error())
}
