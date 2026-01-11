//go:build it

package service

import (
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestSQLiteLogStoreIntegration(t *testing.T) {
	TestLogStoreIntegrationCommon(t, func(t *testing.T) LogStore {
		return createSQLiteLogStore(t)
	})
}

func createSQLiteLogStore(t *testing.T) *SQLiteLogStore {
	tmpDir := t.TempDir()
	logsDSN := fmt.Sprintf("file:%s/test-logs.db", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	
	// Create log store (stores logs only)
	logStore, err := NewSQLiteMattnLogStore(logsDSN, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = logStore.Close() })
	
	return logStore
}
