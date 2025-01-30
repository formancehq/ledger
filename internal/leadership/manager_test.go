//go:build it

package leadership

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"testing"
	"time"
)

func TestLeaderShip(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	pgDB := srv.NewDatabase(t)
	db, err := bunconnect.OpenSQLDB(ctx, pgDB.ConnectionOptions())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	const count = 10

	instances := make([]*Manager, count)
	for i := range count {
		m := NewDefaultLocker(db)
		manager := NewManager(m, logging.Testing(), WithRetryPeriod(10*time.Millisecond))

		go manager.Run(ctx)
		instances[i] = manager
	}

	selectedLeader := -1
	require.Eventually(t, func() bool {
		for index, manager := range instances {
			actual := manager.GetSignal().Actual()
			if actual.Acquired {
				selectedLeader = index
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)
	leaderCount := 0
	for _, manager := range instances {
		if manager.GetSignal().Actual().Acquired {
			leaderCount++
		}
	}
	require.Equal(t, 1, leaderCount)
	require.GreaterOrEqual(t, selectedLeader, 0)

	// ensure the provided db connection is still functional
	instances[selectedLeader].
		GetSignal().
		Actual().DB.
		Exec(func(db bun.IDB) {
			require.NoError(t, db.
				NewSelect().
				Model(&map[string]any{}).
				ColumnExpr("1 as v").
				Scan(ctx),
			)
		})

	require.NoError(t, instances[selectedLeader].Stop(ctx))

	require.Eventually(t, func() bool {
		for index, manager := range instances {
			if manager.GetSignal().Actual().Acquired {
				selectedLeader = index
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond)

	for _, i := range instances {
		require.NoError(t, i.Stop(ctx))
	}
}
