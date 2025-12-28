//go:build it

package service

import (
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestSQLiteLogStoreIntegration(t *testing.T) {
	TestSQLiteLogStoreIntegrationCommon(t, func(t *testing.T) LogStore {
		return createSQLiteStore(t)
	})
}

func createSQLiteStore(t *testing.T) *SQLiteLogStore {
	tmpDir := t.TempDir()
	dsn := fmt.Sprintf("file:%s/test.db", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := NewSQLiteMattnLogStore(ctx, dsn, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}
