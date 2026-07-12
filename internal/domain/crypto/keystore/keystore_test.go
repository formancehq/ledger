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
	require.Equal(t, pub, ks.GetPublicKey("admin-key"))
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

// TestGetChildren_SortedDeterministic pins EN-1521: GetChildren feeds signing
// cascade revocation (RevokedSigningKeyLog.cascadedKeyIds + BFS order), which is
// chain-bound FSM output, so it must return children in a canonical order
// independent of the underlying map's iteration order (invariant #2). The keys
// are registered in a non-sorted order to prove the returned slice is sorted,
// not merely insertion-ordered.
func TestGetChildren_SortedDeterministic(t *testing.T) {
	t.Parallel()

	pub := func() ed25519.PublicKey {
		p, _, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		return p
	}

	ks := NewKeyStore()
	ks.AddPublicKey("root", pub(), "")
	for _, child := range []string{"child-m", "child-a", "child-z", "child-b"} {
		ks.AddPublicKey(child, pub(), "root")
	}

	want := []string{"child-a", "child-b", "child-m", "child-z"}
	// Repeat: Go randomizes map iteration order per range, so a raw map range
	// would eventually return an unsorted slice.
	for range 32 {
		require.Equal(t, want, ks.GetChildren("root"),
			"GetChildren must return a canonically-sorted, deterministic slice")
	}
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
	require.Equal(t, pub, ks.GetPublicKey("child-key"))
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
	require.Equal(t, pub1, ks.GetPublicKey("key-1"))
	require.Equal(t, pub2, ks.GetPublicKey("key-2"))

	// Remove one key, the other should still be there
	ks.RemovePublicKey("key-1")
	require.True(t, ks.HasKeys())
	require.Nil(t, ks.GetPublicKey("key-1"))
	require.Equal(t, pub2, ks.GetPublicKey("key-2"))
}
