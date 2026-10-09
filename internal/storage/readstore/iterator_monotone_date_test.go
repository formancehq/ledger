package readstore

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestMonotoneDateIterator_FirstPageDoesNotDrainDateRange(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)
	kb := dal.NewKeyBuilder()
	for id := uint64(1); id <= 10_000; id++ {
		require.NoError(t, s.DB().Set(LedgerLogDateKey(kb, "l", id/10, id), nil, pebble.NoSync))
	}
	prefix := LedgerLogDateRangePrefix(kb, "l")
	lower, upper := prefix, IncrementBytes(prefix)
	for _, tc := range []struct {
		name    string
		newIter func() (*pebble.Iterator, func() bool, func(), error)
	}{
		{"ascending", func() (*pebble.Iterator, func() bool, func(), error) {
			it, err := NewMonotoneDateIterator[Asc](s.DB(), lower, upper, len(prefix)+8)
			if err != nil {
				return nil, nil, nil, err
			}

			return it.iter, it.Next, it.Close, nil
		}},
		{"descending", func() (*pebble.Iterator, func() bool, func(), error) {
			it, err := NewMonotoneDateIterator[Desc](s.DB(), lower, upper, len(prefix)+8)
			if err != nil {
				return nil, nil, nil, err
			}

			return it.iter, it.Next, it.Close, nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			underlying, next, closeIter, err := tc.newIter()
			require.NoError(t, err)
			defer closeIter()
			require.True(t, next())
			require.True(t, next()) // one-row page plus lookahead
			stats := underlying.Stats()
			steps := stats.ForwardStepCount[pebble.InterfaceCall] + stats.ReverseStepCount[pebble.InterfaceCall]
			require.LessOrEqual(t, steps, 2, "first page scanned beyond its lookahead")
		})
	}
}
