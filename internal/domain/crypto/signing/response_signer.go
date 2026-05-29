package signing

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// ResponseSigner signs Log messages with an Ed25519 key for server-side response signing.
type ResponseSigner struct {
	keyID      string
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
}

// NewResponseSigner creates a new ResponseSigner from a 32-byte Ed25519 seed.
func NewResponseSigner(seed []byte) *ResponseSigner {
	privateKey := ed25519.NewKeyFromSeed(seed)
	publicKey, _ := privateKey.Public().(ed25519.PublicKey)

	// Key ID is the SHA256 fingerprint of the public key (hex-encoded, first 16 chars)
	hash := sha256.Sum256(publicKey)
	keyID := hex.EncodeToString(hash[:8])

	return &ResponseSigner{
		keyID:      keyID,
		privateKey: privateKey,
		publicKey:  publicKey,
	}
}

// SignLog signs a Log message and returns a ResponseSignature.
// It clones the log, clears response_signature and receipt (both node-local),
// serializes it, signs the bytes, and returns the signature envelope.
func (s *ResponseSigner) SignLog(log *commonpb.Log) *signaturepb.ResponseSignature {
	// Clone and clear non-deterministic/node-local fields
	logCopy := log.CloneVT()
	logCopy.ResponseSignature = nil
	logCopy.Receipt = ""

	payload, err := logCopy.MarshalVT()
	if err != nil {
		return nil
	}

	sig := ed25519.Sign(s.privateKey, payload)

	return &signaturepb.ResponseSignature{
		KeyId:         s.keyID,
		Signature:     sig,
		SignedPayload: payload,
	}
}

// PublicKey returns the Ed25519 public key.
func (s *ResponseSigner) PublicKey() ed25519.PublicKey {
	return s.publicKey
}

// KeyID returns the key identifier (SHA256 fingerprint).
func (s *ResponseSigner) KeyID() string {
	return s.keyID
}

// VerifyResponseSignature verifies a ResponseSignature against a known public key.
func VerifyResponseSignature(sig *signaturepb.ResponseSignature, publicKey ed25519.PublicKey) error {
	if sig == nil {
		return errors.New("missing response signature")
	}

	if len(sig.GetSignedPayload()) == 0 {
		return errors.New("empty signed_payload in response signature")
	}

	if len(sig.GetSignature()) != ed25519.SignatureSize {
		return fmt.Errorf("invalid response signature length %d", len(sig.GetSignature()))
	}

	if !ed25519.Verify(publicKey, sig.GetSignedPayload(), sig.GetSignature()) {
		return errors.New("response signature verification failed")
	}

	return nil
}

// LoadSeedFromFile reads an Ed25519 seed from a file.
// The file may contain 32 raw bytes or a 64-char hex-encoded string.
func LoadSeedFromFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading seed file: %w", err)
	}

	// Try to interpret as hex-encoded seed
	seed := data

	trimmed := strings.TrimSpace(string(data))
	if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == ed25519.SeedSize {
		seed = decoded
	}

	if len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}

	return seed, nil
}

// LoadPublicKeyFromFile reads an Ed25519 public key from a file.
// The file may contain 32 raw bytes or a 64-char hex-encoded string.
func LoadPublicKeyFromFile(path string) (ed25519.PublicKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading public key file: %w", err)
	}

	key := data

	trimmed := strings.TrimSpace(string(data))
	if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == ed25519.PublicKeySize {
		key = decoded
	}

	if len(key) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key must be %d bytes, got %d", ed25519.PublicKeySize, len(key))
	}

	return ed25519.PublicKey(key), nil
}
