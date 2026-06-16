package attributes

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/kv"
)

// testKey is a Key implementation for testing.
type testKey struct {
	name string
}

func (k testKey) AppendBytes(dst []byte) []byte { return append(dst, k.name...) }

// newTestKV creates a simple KV store for tests using ShardedMap with a hash function for U128.
func newTestKV[V any]() kv.KV[U128, V] {
	return kv.NewShardedMap[U128, V](func(k U128) uint64 { return k.Hi() })
}

func newTestKeyStore() *KeyStore[testKey, string] {
	return NewKeyStore[testKey, string](newTestKV[Entry[string]]())
}

func TestKeyStorePut(t *testing.T) {
	t.Parallel()

	t.Run("new key returns no old value", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		oldVal, idWithTag, err := store.Put([]byte("account:alice"), "balance-1000")
		require.NoError(t, err)
		require.False(t, oldVal.IsDefined())
		require.NotEqual(t, U128{}, idWithTag.ID)
		require.NotEqual(t, uint64(0), idWithTag.Tag)
	})

	t.Run("overwrite returns old value", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, _, err := store.Put([]byte("account:alice"), "old-value")
		require.NoError(t, err)

		oldVal, _, err := store.Put([]byte("account:alice"), "new-value")
		require.NoError(t, err)
		require.True(t, oldVal.IsDefined())
		require.Equal(t, "old-value", oldVal.Value())
	})

	t.Run("different keys produce different IDs", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, id1, err := store.Put([]byte("account:alice"), "val-a")
		require.NoError(t, err)
		_, id2, err := store.Put([]byte("account:bob"), "val-b")
		require.NoError(t, err)

		require.NotEqual(t, id1.ID, id2.ID)
	})
}

func TestKeyStoreGet(t *testing.T) {
	t.Parallel()

	t.Run("existing key", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, _, err := store.Put([]byte("my-key"), "my-value")
		require.NoError(t, err)

		val, id, err := store.Get([]byte("my-key"))
		require.NoError(t, err)
		require.Equal(t, "my-value", val)
		require.NotEqual(t, U128{}, id)
	})

	t.Run("non-existent key returns ErrNotFound", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, _, err := store.Get([]byte("missing-key"))
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("overwritten value returns latest", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, _, err := store.Put([]byte("key"), "first")
		require.NoError(t, err)
		_, _, err = store.Put([]byte("key"), "second")
		require.NoError(t, err)

		val, _, err := store.Get([]byte("key"))
		require.NoError(t, err)
		require.Equal(t, "second", val)
	})
}

func TestKeyStoreDelete(t *testing.T) {
	t.Parallel()

	t.Run("existing key", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, _, err := store.Put([]byte("to-delete"), "some-value")
		require.NoError(t, err)

		id, _, err := store.Delete([]byte("to-delete"))
		require.NoError(t, err)
		require.NotEqual(t, U128{}, id)

		// Should not be found after deletion
		_, _, err = store.Get([]byte("to-delete"))
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("non-existent key returns ErrNotFound", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()

		_, _, err := store.Delete([]byte("never-existed"))
		require.ErrorIs(t, err, domain.ErrNotFound)
	})
}

func TestErrCollisionDetected(t *testing.T) {
	t.Parallel()

	err := newErrCollisionDetected([]byte{0xAB, 0xCD}, 111, 222)
	require.Error(t, err)
	require.Contains(t, err.Error(), "collision detected")
	require.Contains(t, err.Error(), "ABCD")
	require.Contains(t, err.Error(), "111")
	require.Contains(t, err.Error(), "222")
	require.Equal(t, []byte{0xAB, 0xCD}, err.Bytes)
	require.Equal(t, uint64(111), err.OriginalTag)
	require.Equal(t, uint64(222), err.NewTag)
}

func TestKeyHasherMakeKey(t *testing.T) {
	t.Parallel()

	kh := NewKeyHasher()

	t.Run("deterministic", func(t *testing.T) {
		t.Parallel()

		id1, tag1 := kh.MakeKey([]byte("test"))
		id2, tag2 := kh.MakeKey([]byte("test"))
		require.Equal(t, id1, id2)
		require.Equal(t, tag1, tag2)
	})

	t.Run("different inputs", func(t *testing.T) {
		t.Parallel()

		id1, tag1 := kh.MakeKey([]byte("alpha"))
		id2, tag2 := kh.MakeKey([]byte("beta"))
		require.NotEqual(t, id1, id2)
		require.NotEqual(t, tag1, tag2)
	})
}

func TestDerivedKeyStorePutGetDelete(t *testing.T) {
	t.Parallel()

	t.Run("put and get local value", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		derived := NewDerivedKeyStore[testKey, string](store)

		derived.Put(testKey{name: "key1"}, "value1")
		val, err := derived.Get(testKey{name: "key1"})
		require.NoError(t, err)
		require.Equal(t, "value1", val)
	})

	t.Run("get falls back to underlying store", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		_, _, err := store.Put([]byte("key2"), "from-store")
		require.NoError(t, err)

		derived := NewDerivedKeyStore[testKey, string](store)
		val, err := derived.Get(testKey{name: "key2"})
		require.NoError(t, err)
		require.Equal(t, "from-store", val)
	})

	t.Run("get returns ErrNotFound for deleted key", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		_, _, err := store.Put([]byte("key3"), "exists")
		require.NoError(t, err)

		derived := NewDerivedKeyStore[testKey, string](store)
		derived.Delete(testKey{name: "key3"})

		// A deleted key reads as absent (ErrNotFound), like a committed tombstone.
		_, err = derived.Get(testKey{name: "key3"})
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("get returns a present zero value without error", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		derived := NewDerivedKeyStore[testKey, string](store)

		// A staged write of the zero value is present: Get returns it with no error.
		derived.Put(testKey{name: "z"}, "")
		val, err := derived.Get(testKey{name: "z"})
		require.NoError(t, err)
		require.Empty(t, val)
	})

	t.Run("put after delete restores the value", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		derived := NewDerivedKeyStore[testKey, string](store)

		derived.Put(testKey{name: "k"}, "first")
		derived.Delete(testKey{name: "k"})

		_, err := derived.Get(testKey{name: "k"})
		require.ErrorIs(t, err, domain.ErrNotFound)

		derived.Put(testKey{name: "k"}, "restored")
		val, err := derived.Get(testKey{name: "k"})
		require.NoError(t, err)
		require.Equal(t, "restored", val)
	})

	t.Run("local value shadows underlying store", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		_, _, err := store.Put([]byte("shadow-key"), "original")
		require.NoError(t, err)

		derived := NewDerivedKeyStore[testKey, string](store)
		derived.Put(testKey{name: "shadow-key"}, "override")

		val, err := derived.Get(testKey{name: "shadow-key"})
		require.NoError(t, err)
		require.Equal(t, "override", val)
	})
}

func TestDerivedKeyStoreMerge(t *testing.T) {
	t.Parallel()

	t.Run("merge puts into underlying store", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		derived := NewDerivedKeyStore[testKey, string](store)

		derived.Put(testKey{name: "a"}, "alpha")
		derived.Put(testKey{name: "b"}, "beta")

		updates, deletions, err := derived.Merge()
		require.NoError(t, err)
		require.Len(t, updates, 2)
		require.Empty(t, deletions)

		// Verify underlying store has the values
		val, _, err := store.Get([]byte("a"))
		require.NoError(t, err)
		require.Equal(t, "alpha", val)

		val, _, err = store.Get([]byte("b"))
		require.NoError(t, err)
		require.Equal(t, "beta", val)
	})

	t.Run("merge with overwrite reports old value and old base index", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		_, _, err := store.Put([]byte("existing"), "old")
		require.NoError(t, err)

		derived := NewDerivedKeyStore[testKey, string](store)
		derived.Put(testKey{name: "existing"}, "new")

		updates, _, err := derived.Merge()
		require.NoError(t, err)
		require.Len(t, updates, 1)
		require.True(t, updates[0].Old.IsDefined())
		require.Equal(t, "old", updates[0].Old.Value())
		require.Equal(t, "new", updates[0].New)
	})

	t.Run("merge deletions", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		_, _, err := store.Put([]byte("to-remove"), "value")
		require.NoError(t, err)

		derived := NewDerivedKeyStore[testKey, string](store)
		derived.Delete(testKey{name: "to-remove"})

		updates, deletions, err := derived.Merge()
		require.NoError(t, err)
		require.Empty(t, updates)
		require.Len(t, deletions, 1)
		require.Equal(t, testKey{name: "to-remove"}, deletions[0].Key)

		// Verify the key is removed from the underlying store
		_, _, err = store.Get([]byte("to-remove"))
		require.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("merge deletion of non-existent key is not an error", func(t *testing.T) {
		t.Parallel()

		store := newTestKeyStore()
		derived := NewDerivedKeyStore[testKey, string](store)

		derived.Delete(testKey{name: "ghost"})

		updates, deletions, err := derived.Merge()
		require.NoError(t, err)
		require.Empty(t, updates)
		require.Len(t, deletions, 1)
	})
}
