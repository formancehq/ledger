package signing

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

var (
	ErrInvalidSignature = errors.New("invalid signature")
	ErrMissingSignature = errors.New("missing signature")
	ErrUnknownKeyID     = errors.New("unknown key ID")
)

// Sign serializes an ApplyBatch and returns a SignedApplyBatch envelope
// carrying the exact bytes signed by the client. The returned envelope is
// opaque: the server verifies the signature against payload and then unmarshals
// payload — it never re-serializes the batch, so cross-language clients are safe
// regardless of their protobuf implementation's quirks. Signing the batch (not
// each request) authenticates its composition and ordering.
func Sign(batch *servicepb.ApplyBatch, keyID string, privateKey ed25519.PrivateKey) (*signaturepb.SignedApplyBatch, error) {
	payload, err := batch.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling batch for signing: %w", err)
	}

	return &signaturepb.SignedApplyBatch{
		KeyId:     keyID,
		Signature: ed25519.Sign(privateKey, payload),
		Payload:   payload,
	}, nil
}

// Verify checks the Ed25519 signature on a SignedApplyBatch envelope.
// It verifies the exact bytes provided by the client; no re-serialization.
func Verify(sr *signaturepb.SignedApplyBatch, publicKey ed25519.PublicKey) error {
	if sr == nil {
		return ErrMissingSignature
	}

	if len(sr.GetPayload()) == 0 {
		return fmt.Errorf("%w: empty payload", ErrInvalidSignature)
	}

	if len(sr.GetSignature()) != ed25519.SignatureSize {
		return fmt.Errorf("%w: invalid signature length %d", ErrInvalidSignature, len(sr.GetSignature()))
	}

	if !ed25519.Verify(publicKey, sr.GetPayload(), sr.GetSignature()) {
		return ErrInvalidSignature
	}

	return nil
}

// ExtractBatch deserializes the envelope payload into a trusted ApplyBatch.
// Callers must call Verify first; ExtractBatch does not check the signature.
func ExtractBatch(sr *signaturepb.SignedApplyBatch) (*servicepb.ApplyBatch, error) {
	if sr == nil {
		return nil, ErrMissingSignature
	}

	if len(sr.GetPayload()) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalidSignature)
	}

	batch := &servicepb.ApplyBatch{}

	if err := batch.UnmarshalVT(sr.GetPayload()); err != nil {
		return nil, fmt.Errorf("unmarshaling payload: %w", err)
	}

	return batch, nil
}
