package bloom

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newBloomTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// writeBloomRow writes a raw persisted bloom row at
// [ZoneGlobal][SubGlobBloom][attrCode][blockIndex BE 8] with a valid
// block-sized value, so tests can control exactly which attrCode/shape is on
// disk.
func writeBloomRow(t *testing.T, store *dal.Store, attrCode byte, blockIdx uint64) {
	t.Helper()

	key := []byte{dal.ZoneGlobal, dal.SubGlobBloom, attrCode}
	key = binary.BigEndian.AppendUint64(key, blockIdx)

	batch := store.OpenWriteSession()
	require.NoError(t, batch.Set(key, make([]byte, blockBytes), nil))
	require.NoError(t, batch.Commit())
}

// TestFilterSet_EN1527_RestoreRejectsOrphanAttrCode is the regression for the
// EN-1527 blocker: a persisted bloom row for an attribute that is disabled (or
// unknown) in the current config is visited by no enabled per-attribute
// iterator. Before the whole-namespace validation, FilterSet.RestoreFromStore
// silently skipped it and still published a ready filter — reintroducing the
// false negative the strict-recovery work is meant to close. Restore must now
// fail closed.
func TestFilterSet_EN1527_RestoreRejectsOrphanAttrCode(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter) // only Volumes enabled
	store := newBloomTestStore(t)

	// A block persisted for Metadata, which bloomCfg() leaves disabled: no
	// enabled filter's sub-range covers it.
	writeBloomRow(t, store, dal.SubAttrMetadata, 0)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	err = fs.RestoreFromStore(context.Background(), handle)
	require.Error(t, err, "restore must fail on a persisted bloom row for a disabled/unknown attribute")
	require.Contains(t, err.Error(), "no enabled filter")
}

// TestFilterSet_EN1527_RestoreRejectsShortBloomRow pins that a row too short to
// carry an attribute-type byte (offset 2) is rejected rather than silently
// ignored.
func TestFilterSet_EN1527_RestoreRejectsShortBloomRow(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter)
	store := newBloomTestStore(t)

	// [ZoneGlobal][SubGlobBloom] with no attrCode byte.
	batch := store.OpenWriteSession()
	require.NoError(t, batch.Set([]byte{dal.ZoneGlobal, dal.SubGlobBloom}, make([]byte, blockBytes), nil))
	require.NoError(t, batch.Commit())

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	err = fs.RestoreFromStore(context.Background(), handle)
	require.Error(t, err, "restore must fail on a persisted bloom row too short to carry an attribute-type byte")
	require.Contains(t, err.Error(), "too short")
}

// TestFilterSet_EN1527_RestoreAcceptsEnabledAttrRows confirms the validation is
// not over-eager: a well-formed row for an enabled attribute restores cleanly.
func TestFilterSet_EN1527_RestoreAcceptsEnabledAttrRows(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter) // Volumes enabled
	store := newBloomTestStore(t)

	writeBloomRow(t, store, dal.SubAttrVolume, 0)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	require.NoError(t, fs.RestoreFromStore(context.Background(), handle))
}
