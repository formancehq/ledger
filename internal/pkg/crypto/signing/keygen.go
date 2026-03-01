package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// GenerateKeyPair generates an Ed25519 keypair and writes the seed and public key
// to the specified output directory. Returns the key ID (SHA256 fingerprint prefix).
func GenerateKeyPair(outputDir string) (string, error) {
	if err := os.MkdirAll(outputDir, 0700); err != nil {
		return "", fmt.Errorf("creating output directory: %w", err)
	}

	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return "", fmt.Errorf("generating random seed: %w", err)
	}

	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey := privKey.Public().(ed25519.PublicKey)

	seedPath := filepath.Join(outputDir, "seed.hex")
	if err := os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)+"\n"), 0600); err != nil {
		return "", fmt.Errorf("writing seed file: %w", err)
	}

	pubKeyPath := filepath.Join(outputDir, "pubkey.hex")
	if err := os.WriteFile(pubKeyPath, []byte(hex.EncodeToString(pubKey)+"\n"), 0644); err != nil {
		return "", fmt.Errorf("writing public key file: %w", err)
	}

	// Key ID is the SHA256 fingerprint of the public key (hex-encoded, first 16 chars).
	hash := sha256.Sum256(pubKey)
	keyID := hex.EncodeToString(hash[:8])

	return keyID, nil
}
