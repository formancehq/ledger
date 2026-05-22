package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/signaturepb"
)

func TestResponseSigner(t *testing.T) {
	t.Parallel()

	seed := make([]byte, ed25519.SeedSize)
	_, err := rand.Read(seed)
	require.NoError(t, err)

	signer := NewResponseSigner(seed)

	t.Run("key ID is SHA256 fingerprint of public key", func(t *testing.T) {
		t.Parallel()

		hash := sha256.Sum256(signer.PublicKey())
		expected := hex.EncodeToString(hash[:8])
		require.Equal(t, expected, signer.KeyID())
	})

	t.Run("public key matches seed", func(t *testing.T) {
		t.Parallel()

		privKey := ed25519.NewKeyFromSeed(seed)
		expectedPubKey, ok := privKey.Public().(ed25519.PublicKey)
		require.True(t, ok)
		require.Equal(t, expectedPubKey, signer.PublicKey())
	})

	t.Run("sign and verify log", func(t *testing.T) {
		t.Parallel()

		log := &commonpb.Log{
			Sequence: 42,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "test-ledger",
					},
				},
			},
		}

		sig := signer.SignLog(log)
		require.NotNil(t, sig)
		require.Equal(t, signer.KeyID(), sig.GetKeyId())
		require.Len(t, sig.GetSignature(), ed25519.SignatureSize)
		require.NotEmpty(t, sig.GetSignedPayload())

		// Verify should succeed with the correct public key
		err := VerifyResponseSignature(sig, signer.PublicKey())
		require.NoError(t, err)
	})

	t.Run("verify fails with wrong public key", func(t *testing.T) {
		t.Parallel()

		log := &commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "test",
					},
				},
			},
		}

		sig := signer.SignLog(log)
		require.NotNil(t, sig)

		// Generate a different key
		wrongSeed := make([]byte, ed25519.SeedSize)
		_, err := rand.Read(wrongSeed)
		require.NoError(t, err)

		wrongPubKey := ed25519.NewKeyFromSeed(wrongSeed).Public().(ed25519.PublicKey)

		err = VerifyResponseSignature(sig, wrongPubKey)
		require.Error(t, err)
	})

	t.Run("verify fails with tampered payload", func(t *testing.T) {
		t.Parallel()

		log := &commonpb.Log{
			Sequence: 1,
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "test",
					},
				},
			},
		}

		sig := signer.SignLog(log)
		require.NotNil(t, sig)

		// Tamper with the payload
		sig.SignedPayload = append(sig.SignedPayload, 0xFF)

		err := VerifyResponseSignature(sig, signer.PublicKey())
		require.Error(t, err)
	})

	t.Run("verify fails with nil signature", func(t *testing.T) {
		t.Parallel()

		err := VerifyResponseSignature(nil, signer.PublicKey())
		require.Error(t, err)
	})

	t.Run("receipt is not part of signed content", func(t *testing.T) {
		t.Parallel()

		log1 := &commonpb.Log{
			Sequence: 1,
			Receipt:  "some-jwt-token",
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "test",
					},
				},
			},
		}

		log2 := &commonpb.Log{
			Sequence: 1,
			Receipt:  "different-jwt-token",
			Payload: &commonpb.LogPayload{
				Type: &commonpb.LogPayload_CreateLedger{
					CreateLedger: &commonpb.CreatedLedgerLog{
						Name: "test",
					},
				},
			},
		}

		sig1 := signer.SignLog(log1)
		sig2 := signer.SignLog(log2)

		// signed_payload should be identical since receipt is cleared
		require.Equal(t, sig1.GetSignedPayload(), sig2.GetSignedPayload())
	})
}

func TestVerifyResponseSignature_EmptyPayload(t *testing.T) {
	t.Parallel()

	seed := make([]byte, ed25519.SeedSize)
	_, err := rand.Read(seed)
	require.NoError(t, err)

	signer := NewResponseSigner(seed)

	sig := &signaturepb.ResponseSignature{
		KeyId:         signer.KeyID(),
		Signature:     make([]byte, ed25519.SignatureSize),
		SignedPayload: nil,
	}

	err = VerifyResponseSignature(sig, signer.PublicKey())
	require.Error(t, err)
	require.Contains(t, err.Error(), "empty signed_payload")
}

func TestVerifyResponseSignature_InvalidSignatureLength(t *testing.T) {
	t.Parallel()

	seed := make([]byte, ed25519.SeedSize)
	_, err := rand.Read(seed)
	require.NoError(t, err)

	signer := NewResponseSigner(seed)

	sig := &signaturepb.ResponseSignature{
		KeyId:         signer.KeyID(),
		Signature:     []byte("bad"),
		SignedPayload: []byte("some payload"),
	}

	err = VerifyResponseSignature(sig, signer.PublicKey())
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid response signature length")
}

func TestLoadPublicKeyFromFile_WrongSize(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "bad.bin")
	require.NoError(t, os.WriteFile(path, []byte("wrong size key data"), 0644))

	_, err := LoadPublicKeyFromFile(path)
	require.Error(t, err)
	require.Contains(t, err.Error(), "public key must be")
}

func TestLoadPublicKeyFromFile_NonExistent(t *testing.T) {
	t.Parallel()

	_, err := LoadPublicKeyFromFile("/nonexistent/pubkey.bin")
	require.Error(t, err)
}

func TestLoadSeedFromFile_NonExistent(t *testing.T) {
	t.Parallel()

	_, err := LoadSeedFromFile("/nonexistent/seed.bin")
	require.Error(t, err)
}

func TestLoadSeedFromFile(t *testing.T) {
	t.Parallel()

	t.Run("load raw binary seed", func(t *testing.T) {
		t.Parallel()

		seed := make([]byte, ed25519.SeedSize)
		_, err := rand.Read(seed)
		require.NoError(t, err)

		path := filepath.Join(t.TempDir(), "seed.bin")
		require.NoError(t, os.WriteFile(path, seed, 0600))

		loaded, err := LoadSeedFromFile(path)
		require.NoError(t, err)
		require.Equal(t, seed, loaded)
	})

	t.Run("load hex-encoded seed", func(t *testing.T) {
		t.Parallel()

		seed := make([]byte, ed25519.SeedSize)
		_, err := rand.Read(seed)
		require.NoError(t, err)

		path := filepath.Join(t.TempDir(), "seed.hex")
		require.NoError(t, os.WriteFile(path, []byte(hex.EncodeToString(seed)+"\n"), 0600))

		loaded, err := LoadSeedFromFile(path)
		require.NoError(t, err)
		require.Equal(t, seed, loaded)
	})

	t.Run("reject wrong size", func(t *testing.T) {
		t.Parallel()
		path := filepath.Join(t.TempDir(), "bad.bin")
		require.NoError(t, os.WriteFile(path, []byte("too short"), 0600))

		_, err := LoadSeedFromFile(path)
		require.Error(t, err)
	})
}

func TestLoadPublicKeyFromFile(t *testing.T) {
	t.Parallel()

	t.Run("load raw binary public key", func(t *testing.T) {
		t.Parallel()

		_, privKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		pubKey := privKey.Public().(ed25519.PublicKey)

		path := filepath.Join(t.TempDir(), "pubkey.bin")
		require.NoError(t, os.WriteFile(path, pubKey, 0644))

		loaded, err := LoadPublicKeyFromFile(path)
		require.NoError(t, err)
		require.Equal(t, pubKey, loaded)
	})

	t.Run("load hex-encoded public key", func(t *testing.T) {
		t.Parallel()

		_, privKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)

		pubKey := privKey.Public().(ed25519.PublicKey)

		path := filepath.Join(t.TempDir(), "pubkey.hex")
		require.NoError(t, os.WriteFile(path, []byte(hex.EncodeToString(pubKey)+"\n"), 0644))

		loaded, err := LoadPublicKeyFromFile(path)
		require.NoError(t, err)
		require.Equal(t, pubKey, loaded)
	})
}
