//go:build it

package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/stretchr/testify/require"
)

func TestSQLiteLogStoreModernIntegration(t *testing.T) {
	TestLogStoreIntegrationCommon(t, func(t *testing.T) interface {
		LogWriter
		LogReader
		GetLogWithID(ctx context.Context, id uint64) (*ledgerpb.Log, error)
		GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledgerpb.Log, error)
		GetLastLog(ctx context.Context) (*ledgerpb.Log, error)
	} {
		return createSQLiteModernLogStore(t)
	})
}

func createSQLiteModernLogStore(t *testing.T) *SQLiteLogStore {
	tmpDir := t.TempDir()
	logsDSN := fmt.Sprintf("file:%s/test-logs.db", tmpDir)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	
	// Create log store (stores logs only)
	logStore, err := NewSQLiteModernLogStore(ctx, logsDSN, logger)
	require.NoError(t, err)
	t.Cleanup(func() { _ = logStore.Close() })
	
	return logStore
}
