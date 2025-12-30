//go:build it

package service

import (
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestSQLiteRuntimeStoreIntegration(t *testing.T) {
	TestRuntimeStoreIntegrationCommon(t, func(t *testing.T) interface {
		RuntimeStore
		LogWriter
	} {
		return createSQLiteRuntimeStore(t)
	})
}

func createSQLiteRuntimeStore(t *testing.T) *SQLiteRuntimeStore {
	tmpDir := t.TempDir()
	runtimeDSN := fmt.Sprintf("file:%s/test-runtime.db", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	
	// Create runtime store (stores balances and metadata only)
	runtimeStore, err := NewSQLiteMattnRuntimeStore(ctx, runtimeDSN, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runtimeStore.Close() })
	
	return runtimeStore
}


