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
	require.Nil(t, ks.GetPublicKey("nonexistent"))
}

func TestAddAndGetPublicKey(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("admin-key", pub, "")

	require.True(t, ks.HasKeys())
	require.Equal(t, ed25519.PublicKey(pub), ks.GetPublicKey("admin-key"))
	require.Nil(t, ks.GetPublicKey("nonexistent"))
}

func TestRemovePublicKey(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("key-1", pub, "")
	require.True(t, ks.HasKeys())

	ks.RemovePublicKey("key-1")
	require.False(t, ks.HasKeys())
	require.Nil(t, ks.GetPublicKey("key-1"))
}

func TestReset(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("key-1", pub, "")

	require.True(t, ks.HasKeys())

	ks.Reset()

	require.False(t, ks.HasKeys())
	require.Nil(t, ks.GetPublicKey("key-1"))
}

func TestGetChildren_WithParent(t *testing.T) {
	t.Parallel()

	pub1, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub2, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub3, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("root", pub1, "")
	ks.AddPublicKey("child-1", pub2, "root")
	ks.AddPublicKey("child-2", pub3, "root")

	children := ks.GetChildren("root")
	require.Len(t, children, 2)
	require.ElementsMatch(t, []string{"child-1", "child-2"}, children)

	// No children for a leaf key
	require.Empty(t, ks.GetChildren("child-1"))

	// No children for nonexistent key
	require.Empty(t, ks.GetChildren("nonexistent"))
}

func TestRemovePublicKey_ClearsParent(t *testing.T) {
	t.Parallel()

	pub1, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub2, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("root", pub1, "")
	ks.AddPublicKey("child", pub2, "root")

	require.Len(t, ks.GetChildren("root"), 1)

	ks.RemovePublicKey("child")
	require.Empty(t, ks.GetChildren("root"))
}

func TestAddPublicKey_WithParent(t *testing.T) {
	t.Parallel()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("child-key", pub, "parent-key")

	require.True(t, ks.HasKeys())
	require.Equal(t, ed25519.PublicKey(pub), ks.GetPublicKey("child-key"))
	children := ks.GetChildren("parent-key")
	require.Len(t, children, 1)
	require.Equal(t, "child-key", children[0])
}

func TestMultipleKeys(t *testing.T) {
	t.Parallel()

	pub1, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	pub2, _, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	ks := NewKeyStore()
	ks.AddPublicKey("key-1", pub1, "")
	ks.AddPublicKey("key-2", pub2, "")

	require.True(t, ks.HasKeys())
	require.Equal(t, ed25519.PublicKey(pub1), ks.GetPublicKey("key-1"))
	require.Equal(t, ed25519.PublicKey(pub2), ks.GetPublicKey("key-2"))

	// Remove one key, the other should still be there
	ks.RemovePublicKey("key-1")
	require.True(t, ks.HasKeys())
	require.Nil(t, ks.GetPublicKey("key-1"))
	require.Equal(t, ed25519.PublicKey(pub2), ks.GetPublicKey("key-2"))
}
