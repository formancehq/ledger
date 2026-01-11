//go:build it

package service

import (
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
)

func TestSQLiteRuntimeStoreModernIntegration(t *testing.T) {
	TestRuntimeStoreIntegrationCommon(t, func(t *testing.T) RuntimeStore {
		return createSQLiteModernRuntimeStore(t)
	})
}

func createSQLiteModernRuntimeStore(t *testing.T) RuntimeStore {
	tmpDir := t.TempDir()
	runtimeDSN := fmt.Sprintf("file:%s/test-runtime.db", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	// Create runtime store (stores balances, metadata, and logs)
	runtimeStore, err := NewSQLiteModernRuntimeStore(runtimeDSN, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = runtimeStore.Close() })

	return runtimeStore
}
