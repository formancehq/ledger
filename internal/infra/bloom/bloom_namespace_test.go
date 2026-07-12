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

// TestFilterSet_EN1527_ClassifyRejectsUnknownAttrCode is the regression for the
// EN-1527 blocker: a persisted bloom row whose attribute-type byte is not a
// known bloom type at all is visited by no enabled per-attribute iterator.
// Before the whole-namespace classification, FilterSet.RestoreFromStore
// silently skipped it and still published a ready filter — reintroducing the
// false negative the strict-recovery work is meant to close. Classification
// must now fail closed on genuine corruption.
func TestFilterSet_EN1527_ClassifyRejectsUnknownAttrCode(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter) // only Volumes enabled
	store := newBloomTestStore(t)

	// 0x7F is not a registered SubAttr* bloom type: no filter, enabled or not,
	// ever writes it — genuine corruption.
	writeBloomRow(t, store, 0x7F, 0)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	_, err = fs.ClassifyPersistedNamespace(context.Background(), handle)
	require.Error(t, err, "classification must fail on a persisted bloom row with an unknown attribute-type byte")
	require.Contains(t, err.Error(), "unknown attribute-type byte")
}

// TestFilterSet_EN1527_ClassifyReportsConfigDrift pins the config-drift path:
// a persisted row for a KNOWN attribute type that is disabled in the current
// config is not corruption. Classification must report configDrift=true (so the
// caller takes the rebuild path) rather than aborting — otherwise a node
// restarted with CLI settings that disable a previously-enabled attribute could
// never boot far enough to propose the reconciling config update.
func TestFilterSet_EN1527_ClassifyReportsConfigDrift(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter) // only Volumes enabled
	store := newBloomTestStore(t)

	// Metadata is a known bloom type but bloomCfg() leaves it disabled.
	writeBloomRow(t, store, dal.SubAttrMetadata, 0)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	configDrift, err := fs.ClassifyPersistedNamespace(context.Background(), handle)
	require.NoError(t, err, "a known-but-disabled attribute is config drift, not corruption")
	require.True(t, configDrift, "classification must flag config drift for a known-but-disabled attribute")
}

// TestFilterSet_EN1527_ClassifyRejectsShortBloomRow pins that a row too short to
// carry an attribute-type byte (offset 2) is rejected as corruption rather than
// silently ignored.
func TestFilterSet_EN1527_ClassifyRejectsShortBloomRow(t *testing.T) {
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

	_, err = fs.ClassifyPersistedNamespace(context.Background(), handle)
	require.Error(t, err, "classification must fail on a persisted bloom row too short to carry an attribute-type byte")
	require.Contains(t, err.Error(), "too short")
}

// TestFilterSet_EN1527_ClassifyAcceptsEnabledAttrRows confirms the
// classification is not over-eager: a well-formed row for an enabled attribute
// is neither corruption nor drift.
func TestFilterSet_EN1527_ClassifyAcceptsEnabledAttrRows(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	fs := NewFilterSet(bloomCfg(), meter) // Volumes enabled
	store := newBloomTestStore(t)

	writeBloomRow(t, store, dal.SubAttrVolume, 0)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	configDrift, err := fs.ClassifyPersistedNamespace(context.Background(), handle)
	require.NoError(t, err)
	require.False(t, configDrift, "an enabled-attribute row is neither corruption nor drift")

	require.NoError(t, fs.RestoreFromStore(context.Background(), handle))
}
