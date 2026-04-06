package check

import (
	"math/big"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestReplayStoreMoveVolumeTransfersAndDeletes(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	oldKey := []byte("ledger\x00old-account\x00USD")
	newKey := []byte("ledger\x00new-account\x00USD")

	// Seed the old key with some volume
	require.NoError(t, rs.addVolumeDelta(oldKey, big.NewInt(500), big.NewInt(200)))

	// Move old -> new
	require.NoError(t, rs.moveVolume(oldKey, newKey))

	// New key should have the transferred volume
	pair := readVolume(t, rs, newKey)
	require.Equal(t, "500", pair.GetInput().ToBigInt().String())
	require.Equal(t, "200", pair.GetOutput().ToBigInt().String())

	// Old key should be deleted
	_, _, err := rs.db.Get(replayKey(replayPrefixVolume, oldKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestReplayStoreMoveVolumeAccumulatesIntoExisting(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	oldKey := []byte("ledger\x00old-account\x00USD")
	newKey := []byte("ledger\x00new-account\x00USD")

	// Seed both keys
	require.NoError(t, rs.addVolumeDelta(oldKey, big.NewInt(100), big.NewInt(50)))
	require.NoError(t, rs.addVolumeDelta(newKey, big.NewInt(200), big.NewInt(80)))

	// Move old -> new (should add to existing)
	require.NoError(t, rs.moveVolume(oldKey, newKey))

	pair := readVolume(t, rs, newKey)
	require.Equal(t, "300", pair.GetInput().ToBigInt().String())
	require.Equal(t, "130", pair.GetOutput().ToBigInt().String())

	// Old key should be deleted
	_, _, err := rs.db.Get(replayKey(replayPrefixVolume, oldKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestReplayStoreMoveVolumeNoOpWhenSourceMissing(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	oldKey := []byte("ledger\x00nonexistent\x00USD")
	newKey := []byte("ledger\x00target\x00USD")

	// Move from a key that does not exist — should be a no-op
	require.NoError(t, rs.moveVolume(oldKey, newKey))

	// New key should not exist either
	_, _, err := rs.db.Get(replayKey(replayPrefixVolume, newKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestReplayStoreMoveMetadataTransfersAndDeletes(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	oldKey := []byte("ledger\x00old-account\x01role")
	newKey := []byte("ledger\x00new-account\x01role")

	require.NoError(t, rs.setMetadata(oldKey, "admin"))

	require.NoError(t, rs.moveMetadata(oldKey, newKey))

	// New key should have the value
	val, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, newKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Equal(t, byte(metaFlagSet), val[0])
	require.Equal(t, "admin", string(val[1:]))

	// Old key should be deleted
	_, _, err = rs.db.Get(replayKey(replayPrefixMetadata, oldKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestReplayStoreMoveMetadataNoOpWhenSourceMissing(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	oldKey := []byte("ledger\x00nonexistent\x01role")
	newKey := []byte("ledger\x00target\x01role")

	require.NoError(t, rs.moveMetadata(oldKey, newKey))

	// New key should not exist
	_, _, err := rs.db.Get(replayKey(replayPrefixMetadata, newKey))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestReplayStoreMoveMetadataOverwritesExistingTarget(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	oldKey := []byte("ledger\x00src\x01role")
	newKey := []byte("ledger\x00dst\x01role")

	require.NoError(t, rs.setMetadata(newKey, "user"))
	require.NoError(t, rs.setMetadata(oldKey, "admin"))

	require.NoError(t, rs.moveMetadata(oldKey, newKey))

	val, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, newKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()
	require.Equal(t, "admin", string(val[1:]))
}

func TestReplayStoreDeleteVolume(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00USD")

	require.NoError(t, rs.addVolumeDelta(key, big.NewInt(100), big.NewInt(100)))

	require.NoError(t, rs.deleteVolume(key))

	_, _, err := rs.db.Get(replayKey(replayPrefixVolume, key))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestReplayStoreDeleteVolumeNonExistentKey(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	// Deleting a non-existent key should not error
	require.NoError(t, rs.deleteVolume([]byte("ledger\x00ghost\x00USD")))
}

func TestReplayStoreGetVolumeNonExistent(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	pair, err := rs.getVolume([]byte("ledger\x00ghost\x00USD"))
	require.NoError(t, err)
	require.Nil(t, pair)
}
