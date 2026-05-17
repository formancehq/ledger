package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func deleteAllSigningKeys(b *dal.Batch) error {
	return b.DeleteRangeNoSync(
		[]byte{dal.ZoneGlobal, dal.SubGlobSigningKey},
		[]byte{dal.ZoneGlobal, dal.SubGlobSigningKey + 1},
	)
}

func TestReadSigningKeys(t *testing.T) {
	t.Parallel()
	s := newTestStore(t)

	t.Run("empty store has no signing keys", func(t *testing.T) {
		keys, err := query.ReadSigningKeys(s)
		require.NoError(t, err)
		require.Empty(t, keys)

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

		batch := s.NewBatch()
		require.NoError(t, state.SaveSigningKey(batch, "key-1", pubKey1, ""))
		require.NoError(t, state.SaveSigningKey(batch, "key-2", pubKey2, ""))
		require.NoError(t, batch.Commit())

		keys, err := query.ReadSigningKeys(s)
		require.NoError(t, err)
		require.Len(t, keys, 2)
		require.Equal(t, pubKey1, keys["key-1"].PublicKey)
		require.Equal(t, pubKey2, keys["key-2"].PublicKey)
	})

	t.Run("delete signing key", func(t *testing.T) {
		batch := s.NewBatch()
		require.NoError(t, state.DeleteSigningKey(batch, "key-1"))
		require.NoError(t, batch.Commit())

		keys, err := query.ReadSigningKeys(s)
		require.NoError(t, err)
		require.Len(t, keys, 1)
		_, hasKey1 := keys["key-1"]
		require.False(t, hasKey1)

		_, hasKey2 := keys["key-2"]
		require.True(t, hasKey2)
	})

	t.Run("save and load signing config", func(t *testing.T) {
		batch := s.NewBatch()
		require.NoError(t, state.SaveSigningConfig(batch, true))
		require.NoError(t, batch.Commit())

		requireSig, err := query.ReadSigningConfig(s)
		require.NoError(t, err)
		require.True(t, requireSig)

		batch = s.NewBatch()
		require.NoError(t, state.SaveSigningConfig(batch, false))
		require.NoError(t, batch.Commit())

		requireSig, err = query.ReadSigningConfig(s)
		require.NoError(t, err)
		require.False(t, requireSig)
	})

	t.Run("delete all signing keys", func(t *testing.T) {
		// Add some keys first
		batch := s.NewBatch()
		require.NoError(t, state.SaveSigningKey(batch, "a", make([]byte, 32), ""))
		require.NoError(t, state.SaveSigningKey(batch, "b", make([]byte, 32), ""))
		require.NoError(t, state.SaveSigningKey(batch, "c", make([]byte, 32), ""))
		require.NoError(t, batch.Commit())

		keys, err := query.ReadSigningKeys(s)
		require.NoError(t, err)
		require.Len(t, keys, 4) // key-2 from previous test + a, b, c

		batch = s.NewBatch()
		require.NoError(t, deleteAllSigningKeys(batch))
		require.NoError(t, batch.Commit())

		keys, err = query.ReadSigningKeys(s)
		require.NoError(t, err)
		require.Empty(t, keys)
	})
}
