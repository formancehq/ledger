package state

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/antithesistest"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Prepare(N+1) may run before Commit(N), so the live cache can already hold
// the next transfer. A sentinel must compare the durable batch against N's
// captured expectations, including when N's corrupted writes happen to match
// that newer cache and still conserve double entry.
func TestSentinelRaftPipelinedCommitBoundary(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"healthy", "balanced_future_volumes", "missing_balanced_pair"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fsm, store, attrs := newTestMachine(t)
			fsm.sentinelMode = true
			fsm.sentinel = dal.NewSentinelFactory(store, true)
			const ledger = "pipelined-sentinel"
			ctx := t.Context()
			first, err := fsm.PrepareEntries(ctx, store, makeEntry(t, 1, makeProposal(1,
				createLedgerOrder(ledger),
				createTransactionOrder(ledger, true, newPosting("world", "treasury", "EUR", 10)))))
			require.NoError(t, err)
			defer first.Close()
			require.NoError(t, first.Result.Results[0].Error)

			second, err := fsm.PrepareEntries(ctx, store, makeEntry(t, 2, makeProposal(2,
				createTransactionOrder(ledger, true, newPosting("world", "treasury", "EUR", 20)))))
			require.NoError(t, err)
			defer second.Close()
			require.NoError(t, second.Result.Results[0].Error)
			require.Equal(t, uint64(2), fsm.State.LastAppliedIndex)
			require.Equal(t, uint64(0), fsm.LastPersistedIndex())

			for _, account := range []string{"world", "treasury"} {
				key := domain.NewVolumeKey(ledger, account, "EUR", "")
				switch mode {
				case "balanced_future_volumes":
					pair := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(30), Output: commonpb.NewUint256FromUint64(0)}
					if account == "world" {
						pair.Input, pair.Output = pair.GetOutput(), pair.GetInput()
					}
					_, err := attrs.Volume.Set(first.batch, key.Bytes(), pair)
					require.NoError(t, err)
				case "missing_balanced_pair":
					require.NoError(t, attrs.Volume.Delete(first.batch, key.Bytes()))
				}
			}

			err = fsm.CommitPreparedBatch(ctx, first)
			if mode == "healthy" {
				require.NoError(t, err, "preparing the next transfer cannot change this commit's expected volumes")
				require.Equal(t, uint64(1), fsm.LastPersistedIndex())
			} else {
				require.ErrorContains(t, err, "post-commit volume assertion failed")
				if mode == "balanced_future_volumes" {
					var divergence *ErrVolumeCachePebbleDivergence
					require.ErrorAs(t, err, &divergence)
					require.Equal(t, uint64(1), divergence.RaftIndex)
					require.Equal(t, ledger, divergence.Key.LedgerName)
					require.Contains(t, []string{"world", "treasury"}, divergence.Key.Account)
					if divergence.Key.Account == "world" {
						require.Equal(t, "10", divergence.CacheOutput)
						require.Equal(t, "30", divergence.PebbleOutput)
					} else {
						require.Equal(t, "10", divergence.CacheInput)
						require.Equal(t, "30", divergence.PebbleInput)
					}
				} else {
					require.ErrorContains(t, err, "volume missing from pebble after commit")
				}
				require.Zero(t, fsm.LastPersistedIndex(), "failed verification must not publish successful application")
			}

			// Verification is after Pebble commit: failure cannot roll back the
			// committed index or bad bytes. Both injected defects preserve global
			// input/output equality, so the exact per-key comparison is essential.
			persisted, err := query.ReadLastAppliedIndex(store)
			require.NoError(t, err)
			require.Equal(t, uint64(1), persisted)
			for _, account := range []string{"world", "treasury"} {
				pair, err := attrs.Volume.Get(store, domain.NewVolumeKey(ledger, account, "EUR", "").Bytes())
				require.NoError(t, err)
				if mode == "missing_balanced_pair" {
					require.Nil(t, pair)

					continue
				}
				require.NotNil(t, pair)
				amount := "10"
				if mode == "balanced_future_volumes" {
					amount = "30"
				}
				if account == "world" {
					require.Equal(t, amount, pair.GetOutput().ToBigInt().String())
					require.Zero(t, pair.GetInput().ToBigInt().Sign())
				} else {
					require.Equal(t, amount, pair.GetInput().ToBigInt().String())
					require.Zero(t, pair.GetOutput().ToBigInt().Sign())
				}
			}
			if mode == "healthy" {
				require.NoError(t, fsm.CommitPreparedBatch(ctx, second))
				require.Equal(t, uint64(2), fsm.LastPersistedIndex())
				pair, err := attrs.Volume.Get(store, domain.NewVolumeKey(ledger, "treasury", "EUR", "").Bytes())
				require.NoError(t, err)
				require.Equal(t, "30", pair.GetInput().ToBigInt().String())
			}
		})
	}
}

func TestSentinelRaftCommitAntithesisEmission(t *testing.T) {
	if !antithesistest.Armed {
		t.Skip(antithesistest.UnarmedSkip)
	}
	t.Parallel()
	for _, tc := range []struct {
		name      string
		mode      string
		property  string
		condition bool
		wantHit   bool
	}{
		{"healthy_success", "healthy", "nonempty sentinel verification completed", true, true},
		{"healthy_no_divergence", "healthy", "cache pebble volume divergence", false, false},
		{"wrong_volumes_reported", "balanced_future_volumes", "cache pebble volume divergence", false, true},
		{"wrong_volumes_not_success", "balanced_future_volumes", "nonempty sentinel verification completed", true, false},
		{"missing_pair_reported", "missing_balanced_pair", "committed volume is present in pebble", false, true},
		{"missing_pair_not_success", "missing_balanced_pair", "nonempty sentinel verification completed", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			found, err := antithesistest.Emitted(ctx, t.TempDir(), "TestSentinelRaftPipelinedCommitBoundary/"+tc.mode, tc.property, tc.condition)
			require.NoError(t, err)
			require.Equal(t, tc.wantHit, found, "property %q with condition %v", tc.property, tc.condition)
		})
	}
}
