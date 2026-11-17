//go:build it

package restore_test

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/pkg/features"
)

// Use production controllers, migrations and PostgreSQL serialization rather
// than deriving the expected log from the same mock that implements the fix.
func TestSEC90CanonicalVolumesPersistThroughRealRestore(t *testing.T) {
	server := newServer(t)
	for _, moves := range []string{"ON", "OFF"} {
		for _, effective := range []string{"SYNC", "DISABLED"} {
			t.Run(moves+"/"+effective, func(t *testing.T) {
				f := newFixture(t, server)
				fs := features.DefaultFeatures.With(features.FeatureMovesHistory, moves).
					With(features.FeatureMovesHistoryPostCommitEffectiveVolumes, effective)
				makeLedger := func() (ledgercontroller.Controller, *ledger.Ledger) {
					cfg := ledger.NewDefaultConfiguration()
					cfg.Bucket = "_default"
					cfg.Features = fs
					item, err := ledger.New("sec90_"+uuid.NewString()[:8], cfg)
					require.NoError(t, err)
					_, err = f.driver.CreateLedger(f.ctx, item)
					require.NoError(t, err)
					controller, err := f.system.GetLedgerController(f.ctx, item.Name)
					require.NoError(t, err)
					return controller, item
				}
				from, _ := makeLedger()
				for range 2 {
					f.create(t, from, `send [USD/2 100] (
 source = @world
 destination = @alice
)`)
				}
				logs := f.export(t, from)
				expected := make([]ledger.CreatedTransaction, len(logs))
				originalHashes := make([][]byte, len(logs))
				stale := func() ledger.PostCommitVolumes {
					return ledger.PostCommitVolumes{
						"world":     {"USD/2": ledger.NewVolumesInt64(0, 999999)},
						"alice":     {"USD/2": ledger.NewVolumesInt64(999999, 0)},
						"untouched": {"EUR": ledger.NewVolumesInt64(42, 0)},
					}
				}
				for i := range logs {
					payload := logs[i].Data.(ledger.CreatedTransaction)
					expected[i] = payload
					originalHashes[i] = append([]byte(nil), logs[i].Hash...)
					payload.Transaction.PostCommitVolumes = stale()
					payload.Transaction.PostCommitEffectiveVolumes = stale()
					logs[i].Data = payload
				}
				target, item := makeLedger()
				require.NoError(t, f.restore(target, logs))
				exported := f.export(t, target)
				require.Len(t, exported, len(logs))
				var persisted []struct {
					Data json.RawMessage `bun:"data"`
				}
				require.NoError(t, f.db.NewSelect().TableExpr("?.?", bun.Ident(item.Bucket), bun.Ident("logs")).Column("data").Where("ledger = ?", item.Name).OrderExpr("id ASC").Scan(f.ctx, &persisted))
				require.Len(t, persisted, len(logs))
				for i := range exported {
					require.Equal(t, originalHashes[i], exported[i].Hash)
					actual := exported[i].Data.(ledger.CreatedTransaction)
					require.Equal(t, expected[i].Transaction.PostCommitVolumes, actual.Transaction.PostCommitVolumes)
					var stored ledger.CreatedTransaction
					require.NoError(t, json.Unmarshal(persisted[i].Data, &stored))
					require.Equal(t, expected[i].Transaction.PostCommitVolumes, stored.Transaction.PostCommitVolumes)
					require.Equal(t, actual.Transaction.PostCommitEffectiveVolumes, stored.Transaction.PostCommitEffectiveVolumes)
					require.NotContains(t, stored.Transaction.PostCommitVolumes, "untouched")
					if moves != "ON" || effective != "SYNC" {
						require.Nil(t, stored.Transaction.PostCommitEffectiveVolumes)
					} else {
						require.Equal(t, expected[i].Transaction.PostCommitEffectiveVolumes, stored.Transaction.PostCommitEffectiveVolumes)
					}
				}
			})
		}
	}
}
