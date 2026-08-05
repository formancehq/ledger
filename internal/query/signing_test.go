package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func deleteAllSigningKeys(b *dal.WriteSession) error {
	return b.DeleteRangeNoSync(
		[]byte{dal.ZoneGlobal, dal.SubGlobSigningKey},
		[]byte{dal.ZoneGlobal, dal.SubGlobSigningKey + 1},
	)
}

func TestReadSigningKeys(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	t.Run("empty store has no signing keys", func(t *testing.T) {
		keys, malformed, err := query.ReadSigningKeys(handle)
		require.NoError(t, err)
		require.Empty(t, keys)
		require.Empty(t, malformed)

		requireSig, err := query.ReadSigningConfig(s)
		require.NoError(t, err)
		require.False(t, requireSig)
	})

	t.Run("save and load signing keys", func(t *testing.T) {
		pubKey1 := make([]byte, 32)
		pubKey2 := make([]byte, 32)

		for i := range pubKey1 {
			pubKey1[i] = byte(i)
			pubKey2[i] = byte(i + 100)
		}

		batch := s.OpenWriteSession()
		require.NoError(t, state.SaveSigningKey(batch, "key-1", pubKey1, ""))
		require.NoError(t, state.SaveSigningKey(batch, "key-2", pubKey2, ""))
		require.NoError(t, batch.Commit())

		keys, malformed, err := query.ReadSigningKeys(handle)
		require.NoError(t, err)
		require.Empty(t, malformed)
		require.Len(t, keys, 2)
		require.Equal(t, pubKey1, keys["key-1"].PublicKey)
		require.Equal(t, pubKey2, keys["key-2"].PublicKey)
	})

	t.Run("delete signing key", func(t *testing.T) {
		batch := s.OpenWriteSession()
		require.NoError(t, state.DeleteSigningKey(batch, "key-1"))
		require.NoError(t, batch.Commit())

		keys, malformed, err := query.ReadSigningKeys(handle)
		require.NoError(t, err)
		require.Empty(t, malformed)
		require.Len(t, keys, 1)
		_, hasKey1 := keys["key-1"]
		require.False(t, hasKey1)

		_, hasKey2 := keys["key-2"]
		require.True(t, hasKey2)
	})

	t.Run("save and load signing config", func(t *testing.T) {
		batch := s.OpenWriteSession()
		require.NoError(t, state.SaveSigningConfig(batch, true))
		require.NoError(t, batch.Commit())

		requireSig, err := query.ReadSigningConfig(s)
		require.NoError(t, err)
		require.True(t, requireSig)

		batch = s.OpenWriteSession()
		require.NoError(t, state.SaveSigningConfig(batch, false))
		require.NoError(t, batch.Commit())

		requireSig, err = query.ReadSigningConfig(s)
		require.NoError(t, err)
		require.False(t, requireSig)
	})

	t.Run("delete all signing keys", func(t *testing.T) {
		// Add some keys first
		batch := s.OpenWriteSession()
		require.NoError(t, state.SaveSigningKey(batch, "a", make([]byte, 32), ""))
		require.NoError(t, state.SaveSigningKey(batch, "b", make([]byte, 32), ""))
		require.NoError(t, state.SaveSigningKey(batch, "c", make([]byte, 32), ""))
		require.NoError(t, batch.Commit())

		keys, malformed, err := query.ReadSigningKeys(handle)
		require.NoError(t, err)
		require.Empty(t, malformed)
		require.Len(t, keys, 4) // key-2 from previous test + a, b, c

		batch = s.OpenWriteSession()
		require.NoError(t, deleteAllSigningKeys(batch))
		require.NoError(t, batch.Commit())

		keys, malformed, err = query.ReadSigningKeys(handle)
		require.NoError(t, err)
		require.Empty(t, malformed)
		require.Empty(t, keys)
	})
}

// TestReadSigningKeysMalformedRows pins the bounds check on the public-key slice:
// value[:32] is length-checked before copy runs, so a row shorter than an Ed25519
// public key is REPORTED as malformed rather than panicking the reader. The boot
// path calls this, so a panic here is a crash-loop one corrupt byte can trigger —
// and the row is Raft-replicated, so it would be a crash-loop on every replica.
func TestReadSigningKeysMalformedRows(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name        string
		valueLength int
	}{
		{name: "empty value", valueLength: 0},
		{name: "single byte value", valueLength: 1},
		{name: "one byte short of a public key", valueLength: 31},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newTestStore(t)

			handle, err := s.NewDirectReadHandle()
			require.NoError(t, err)
			t.Cleanup(func() { _ = handle.Close() })

			key := append([]byte{dal.ZoneGlobal, dal.SubGlobSigningKey}, []byte("short-key")...)
			value := make([]byte, tc.valueLength)

			batch := s.OpenWriteSession()
			require.NoError(t, batch.SetBytes(key, value))
			require.NoError(t, batch.Commit())

			keys, malformed, err := query.ReadSigningKeys(handle)
			require.NoError(t, err)
			require.Empty(t, keys)
			require.Len(t, malformed, 1)
			require.Equal(t, "short-key", malformed[0].KeyID)
			require.Equal(t, tc.valueLength, malformed[0].ValueLength)
			require.Contains(t, malformed[0].Reason, "shorter than an Ed25519 public key")
		})
	}
}

// TestReadSigningKeysWellFormedRowsStillDecode guards against a regression
// where the malformed-row check would also reject well-formed rows.
func TestReadSigningKeysWellFormedRowsStillDecode(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	handle, err := s.NewDirectReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	rootPubKey := make([]byte, 32)
	childPubKey := make([]byte, 32)

	for i := range rootPubKey {
		rootPubKey[i] = byte(i)
		childPubKey[i] = byte(i + 1)
	}

	batch := s.OpenWriteSession()
	require.NoError(t, state.SaveSigningKey(batch, "root", rootPubKey, ""))
	require.NoError(t, state.SaveSigningKey(batch, "child", childPubKey, "root"))
	require.NoError(t, batch.Commit())

	keys, malformed, err := query.ReadSigningKeys(handle)
	require.NoError(t, err)
	require.Empty(t, malformed)
	require.Len(t, keys, 2)
	require.Equal(t, rootPubKey, keys["root"].PublicKey)
	require.Empty(t, keys["root"].ParentKeyID)
	require.Equal(t, childPubKey, keys["child"].PublicKey)
	require.Equal(t, "root", keys["child"].ParentKeyID)
}
