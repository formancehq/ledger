package keystore

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewKeyStoreEmpty(t *testing.T) {
	t.Parallel()

	ks := NewKeyStore()
	require.False(t, ks.HasKeys())
	require.False(t, ks.RequireSignatures())
	require.Nil(t, ks.GetPublicKey("nonexistent"))
}

func TestAddAndGetPublicKey(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("admin-key", pub)

	require.True(t, ks.HasKeys())
	require.Equal(t, ed25519.PublicKey(pub), ks.GetPublicKey("admin-key"))
	require.Nil(t, ks.GetPublicKey("nonexistent"))
}

func TestRemovePublicKey(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("key-1", pub)
	require.True(t, ks.HasKeys())

	ks.RemovePublicKey("key-1")
	require.False(t, ks.HasKeys())
	require.Nil(t, ks.GetPublicKey("key-1"))
}

func TestSetRequireSignatures(t *testing.T) {
	t.Parallel()

	ks := NewKeyStore()
	require.False(t, ks.RequireSignatures())

	ks.SetRequireSignatures(true)
	require.True(t, ks.RequireSignatures())

	ks.SetRequireSignatures(false)
	require.False(t, ks.RequireSignatures())
}

func TestReset(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("key-1", pub)
	ks.SetRequireSignatures(true)

	require.True(t, ks.HasKeys())
	require.True(t, ks.RequireSignatures())

	ks.Reset()

	require.False(t, ks.HasKeys())
	require.False(t, ks.RequireSignatures())
	require.Nil(t, ks.GetPublicKey("key-1"))
}

func TestMultipleKeys(t *testing.T) {
	t.Parallel()

	pub1, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub2, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("key-1", pub1)
	ks.AddPublicKey("key-2", pub2)

	require.True(t, ks.HasKeys())
	require.Equal(t, ed25519.PublicKey(pub1), ks.GetPublicKey("key-1"))
	require.Equal(t, ed25519.PublicKey(pub2), ks.GetPublicKey("key-2"))

	// Remove one key, the other should still be there
	ks.RemovePublicKey("key-1")
	require.True(t, ks.HasKeys())
	require.Nil(t, ks.GetPublicKey("key-1"))
	require.Equal(t, ed25519.PublicKey(pub2), ks.GetPublicKey("key-2"))
}
