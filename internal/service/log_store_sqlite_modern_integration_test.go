//go:build it

package service

import (
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestSQLiteLogStoreModernIntegration(t *testing.T) {
	TestSQLiteLogStoreIntegrationCommon(t, func(t *testing.T) LogStore {
		return createSQLiteModernStore(t)
	})
}

func createSQLiteModernStore(t *testing.T) *SQLiteLogStore {
	tmpDir := t.TempDir()
	dsn := fmt.Sprintf("file:%s/test.db", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	store, err := NewSQLiteModernLogStore(ctx, dsn, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}
