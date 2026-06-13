package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestSentinelTracerIsolatedAcrossBatches reproduces the diagnostic bug we
// observed in the Antithesis log: the pre-fix code stored a single shared
// tracer on the FSM and Reset()'d it at the start of every PrepareEntries,
// so a panic during pb_N's pipelined commit dumped pb_{N+1}'s entries
// (whatever the next PrepareEntries had populated by then).
//
// The fix allocates a fresh tracer per PrepareEntries and threads the pointer
// through PreparedBatch.sentinelTracer. After PrepareEntries(2) runs, the
// tracer captured by PreparedBatch_1 must still hold PrepareEntries(1)'s
// entries.
func TestSentinelTracerIsolatedAcrossBatches(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dataStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dataStore.Close() })

	c, err := cache.New(1000, meter)
	require.NoError(t, err)

	machine, err := NewMachine(
		logger, dataStore, meter, c, attributes.New(),
		keystore.NewKeyStore(), NewSharedState(), noopNotifier{}, nil,
		"test-cluster",
		0,
		true, // sentinelMode
		0,
	)
	require.NoError(t, err)

	// PrepareEntries #1 carries one entry. Capture its tracer pointer.
	pb1, err := machine.PrepareEntries(context.Background(),
		makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-1"))),
	)
	require.NoError(t, err)
	require.NotNil(t, pb1.sentinelTracer)

	tracer1 := pb1.sentinelTracer
	entriesBefore := len(tracer1.entries)
	require.Greater(t, entriesBefore, 0, "PrepareEntries #1 should have produced at least one tracer entry")

	// Now commit and run PrepareEntries #2 — this is what used to clobber
	// the shared tracer. After the second call, tracer1 must be unchanged
	// because PrepareEntries #2 allocates a brand-new tracer.
	require.NoError(t, machine.CommitPreparedBatch(context.Background(), pb1))

	pb2, err := machine.PrepareEntries(context.Background(),
		makeEntry(t, 2, makeProposal(2, createLedgerOrder("ledger-2"))),
	)
	require.NoError(t, err)

	require.NotSame(t, tracer1, pb2.sentinelTracer,
		"each PrepareEntries must allocate a fresh tracer instance")
	require.Equal(t, entriesBefore, len(tracer1.entries),
		"PrepareEntries #2 must not mutate the tracer captured by PreparedBatch_1")

	require.NoError(t, machine.CommitPreparedBatch(context.Background(), pb2))
}
