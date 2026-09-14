package ctrl

import (
	"encoding/binary"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// Exercise the controller's actual compile/filter/paginate path. Instrument
// the compiled iterator with its profiling counters and the horizon wrapper's
// predicate, so a drain added before OR after pagination fails this test.
//
// The parity and benchmark suites call CompileReverse/PaginateReverse
// directly, so a drain reintroduced in listDescFiltered itself — around the
// horizon wrapper, or after pagination has already produced the right page —
// returns the correct rows and leaves them green.
//
// The bound belongs to listDescFiltered and PaginateReverse, not to any leaf:
// the target only selects which leaves CompileReverse builds. So one target is
// enough, and the fixture uses LOGS because the LOGS arm of compileUniverseRev
// reads ctx.indexReader — the reader passed here — while the ACCOUNTS and
// TRANSACTIONS arms read ctx.pebbleReader. Retargeting this test therefore
// means supplying pebbleReader and a pin as well.
//
// This must stay on listDescFiltered rather than listEntities: listEntities
// overwrites horizonKeep with query.MainHorizonKeep, which would discard the
// counting predicate and with it the ability to see a drain that happens
// outside the compiled tree.
func TestListDescFilteredStopsAtLookahead(t *testing.T) {
	t.Parallel()

	const ledger = "pagination"

	rs, err := readstore.New(t.TempDir(), logging.NopZap(), readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rs.Close()) })

	kb := dal.NewKeyBuilder()
	for id := uint64(2); id <= 100; id += 2 {
		require.NoError(t, rs.DB().Set(readstore.LedgerLogKey(kb, ledger, id), nil, pebble.NoSync))
	}

	encode := func(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }

	filter := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{
		Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: ledger}},
	}}}

	for _, tc := range []struct {
		name                         string
		after                        uint64
		want                         []uint64
		visits, nextCalls, seekCalls int64
	}{
		{"first page", 0, []uint64{100, 98, 96}, 4, 4, 0},
		{"existing exclusive cursor", 96, []uint64{94, 92, 90}, 5, 4, 1},
		{"absent cursor", 95, []uint64{94, 92, 90}, 4, 3, 1},
		{"cursor above last entity", 101, []uint64{100, 98, 96}, 4, 3, 1},
		{"exact final page", 8, []uint64{6, 4, 2}, 4, 4, 1},
		{"short final page", 6, []uint64{4, 2}, 3, 3, 1},
		{"cursor below first entity", 1, nil, 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			profile := &query.QueryProfile{}

			var visits int64

			var out [][]byte

			err := listDescFiltered(rs.DB(), entityListParams[uint64]{
				target:     commonpb.QueryTarget_QUERY_TARGET_LOGS,
				ledgerName: ledger, pageSize: 3, after: tc.after, afterToBytes: encode,
				filter: filter, profile: profile,
				horizonKeep: func([]byte) (bool, error) {
					visits++
					// The cursor entity itself may be visited once by SeekLE, then
					// excluded. All other visits must fit within pageSize+1.
					require.LessOrEqual(t, visits, tc.visits, "controller drained beyond cursor positioning and pageSize+1")

					return true, nil
				},
			}, &out)
			require.NoError(t, err)

			var got []uint64
			for _, id := range out {
				got = append(got, binary.BigEndian.Uint64(id))
			}

			require.Equal(t, tc.want, got)
			require.Equal(t, tc.visits, visits)
			require.NotNil(t, profile.Root)
			require.Equal(t, tc.nextCalls, profile.Root.NextCalls)
			require.Equal(t, tc.seekCalls, profile.Root.SeekCalls)
		})
	}
}
