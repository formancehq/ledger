package state

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A prepared batch may cross several generation boundaries. Only the first
// rotation of each Bloom rebuild epoch needs a complete namespace replacement.
// A different prepared batch must still stage a complete replacement while the
// first batch has not committed.
func TestPrepareEntries_BloomReplacementOncePerBatchAndEpoch(t *testing.T) {
	t.Parallel()

	newMachine := func(t *testing.T) (*Machine, *dal.Store) {
		t.Helper()
		machine, store, _ := newTestMachineWithThreshold(t, 1)
		filters := bloom.NewFilterSet(&commonpb.ClusterConfig{
			BloomVolumes: &commonpb.BloomTypeConfig{ExpectedKeys: 4096, FpRate: 0.01},
		}, noop.NewMeterProvider().Meter("test"))
		for i := range 512 {
			filters.AddCanonicalKeys(&bloom.BloomUpdates{
				Volumes: []attributes.U128{attributes.HashU128([]byte(fmt.Sprintf("volume-%d", i)))},
			})
		}
		filters.SetReady(true)
		machine.BloomFilters = filters

		return machine, store
	}

	prepare := func(t *testing.T, machine *Machine, store *dal.Store, first, last uint64) *PreparedBatch {
		t.Helper()
		entries := make([]*raftpb.Entry, 0, last-first+1)
		for index := first; index <= last; index++ {
			entries = append(entries, &raftpb.Entry{
				Index: new(index),
				Term:  proto.Uint64(1),
				Type:  new(raftpb.EntryNormal),
			})
		}
		pb, err := machine.PrepareEntries(context.Background(), store, entries...)
		require.NoError(t, err)
		t.Cleanup(pb.Close)

		return pb
	}

	one, oneStore := newMachine(t)
	first := prepare(t, one, oneStore, 1, 2) // one rotation, one complete projection
	fullBytes := first.batch.BufferedBytes()
	require.Greater(t, fullBytes, 4_000, "fixture must stage a meaningful full projection")

	many, manyStore := newMachine(t)
	rotations := prepare(t, many, manyStore, 1, 6) // five rotations in one Pebble batch
	require.Less(t, rotations.batch.BufferedBytes(), 2*fullBytes,
		"later rotations in the same batch must not restage the full projection")

	// The preceding prepared batch is still uncommitted, so a separate batch
	// must be self-contained even though it sees the same Bloom epoch.
	next := prepare(t, one, oneStore, 3, 3)
	require.Greater(t, next.batch.BufferedBytes(), fullBytes/2,
		"a pipelined batch cannot rely on an uncommitted predecessor's replacement")
}
