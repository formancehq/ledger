package query

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/monitor"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/stretchr/testify/require"
)

func TestInitQuery(t *testing.T) {
	t.Parallel()

	now := core.Now()

	tx0 := core.NewTransaction().WithPostings(
		core.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
	)
	tx1 := core.NewTransaction().WithPostings(
		core.NewPosting("bank", "user:1", "USD/2", big.NewInt(10)),
	)

	appliedMetadataOnTX1 := metadata.Metadata{
		"paymentID": "1234",
	}
	appliedMetadataOnAccount := metadata.Metadata{
		"category": "gold",
	}

	log0 := core.NewTransactionLog(tx0, nil)
	log1 := core.NewTransactionLog(tx1, nil)
	log2 := core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeTransaction,
		TargetID:   tx1.ID,
		Metadata:   appliedMetadataOnTX1,
	})
	log3 := core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "bank",
		Metadata:   appliedMetadataOnAccount,
	})
	log4 := core.NewSetMetadataLog(now, core.SetMetadataLogPayload{
		TargetType: core.MetaTargetTypeAccount,
		TargetID:   "another:account",
		Metadata:   appliedMetadataOnAccount,
	})

	logs := make([]*core.PersistedLog, 0)
	var previous *core.PersistedLog
	for _, l := range []*core.Log{
		log0, log1, log2, log3, log4,
	} {
		next := l.ComputePersistentLog(previous)
		logs = append(logs, next)
		previous = next
	}

	ledgerStore := &mockStore{
		accounts: map[string]*core.AccountWithVolumes{},
		logs:     logs,
	}

	nextLogID, err := ledgerStore.GetNextLogID(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), nextLogID)

	lastProcessedId, err := initLedger(
		context.Background(),
		&InitLedgerConfig{
			LimitReadLogs: 2,
		},
		"default_test",
		ledgerStore,
		monitor.NewNoOpMonitor(),
		metrics.NewNoOpMetricsRegistry(),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(4), lastProcessedId)

	lastReadLogID, err := ledgerStore.GetNextLogID(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(5), lastReadLogID)
}
