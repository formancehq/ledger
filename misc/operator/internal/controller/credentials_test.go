package controller

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestGenerateEd25519KeyPair(t *testing.T) {
	t.Parallel()

	seed, pubKey, keyID, err := generateEd25519KeyPair()
	require.NoError(t, err)

	assert.Len(t, seed, 32, "seed must be 32 bytes")
	assert.Len(t, pubKey, 32, "public key must be 32 bytes")
	assert.Len(t, keyID, 16, "keyID must be 16 hex chars (8 bytes)")

	// keyID must be valid hex.
	_, err = hex.DecodeString(keyID)
	require.NoError(t, err, "keyID must be valid hex")

	// Two calls must produce different keys.
	seed2, pubKey2, keyID2, err := generateEd25519KeyPair()
	require.NoError(t, err)
	assert.NotEqual(t, seed, seed2, "seeds must differ")
	assert.NotEqual(t, pubKey, pubKey2, "public keys must differ")
	assert.NotEqual(t, keyID, keyID2, "keyIDs must differ")
}

func TestComputeAuthKeysHash_Deterministic(t *testing.T) {
	t.Parallel()

	credentials := []credentialsKeyInfo{
		{ConfigMapPrefix: "credentials", CredentialsName: "credentials-a", KeyID: "abc123", PublicKey: "deadbeef", Scopes: []string{"read"}},
		{ConfigMapPrefix: "credentials", CredentialsName: "credentials-b", KeyID: "def456", PublicKey: "cafebabe", Scopes: []string{"write"}},
	}

	hash1 := computeAuthKeysHash(credentials)
	hash2 := computeAuthKeysHash(credentials)

	assert.Equal(t, hash1, hash2, "same input must produce same hash")
	assert.Len(t, hash1, 64, "SHA-256 hex digest must be 64 chars")
}

func TestComputeAuthKeysHash_DifferentInput(t *testing.T) {
	t.Parallel()

	credentials1 := []credentialsKeyInfo{
		{ConfigMapPrefix: "credentials", CredentialsName: "credentials-a", KeyID: "abc123", PublicKey: "deadbeef", Scopes: []string{"read"}},
	}
	credentials2 := []credentialsKeyInfo{
		{ConfigMapPrefix: "credentials", CredentialsName: "credentials-b", KeyID: "def456", PublicKey: "cafebabe", Scopes: []string{"write"}},
	}

	hash1 := computeAuthKeysHash(credentials1)
	hash2 := computeAuthKeysHash(credentials2)

	assert.NotEqual(t, hash1, hash2, "different inputs must produce different hashes")
}

func TestAuthKeysConfigMapName(t *testing.T) {
	t.Parallel()

	ledger := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-ledger"},
	}
	assert.Equal(t, "ledger-my-ledger-auth-keys", authKeysConfigMapName(ledger.Name))
}
