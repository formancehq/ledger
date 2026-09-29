package ctrl

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	pebble "github.com/formancehq/ledger/v3/internal/storage/kv"
)

func TestScanAccountLogsCompletionAtTrace(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })

	logger := logging.NewMockLogger(gomock.NewController(t))
	logger.EXPECT().WithFields(map[string]any{
		"account":     "users:alice",
		"volEntries":  0,
		"metaEntries": 0,
	}).Return(logger)
	logger.EXPECT().Tracef("scanAccount complete")

	_, err = scanAccount(db, attributes.New(), "test", "users:alice", false, logger)
	require.NoError(t, err)
}
