package signing

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/signaturepb"
)

var (
	ErrInvalidSignature = errors.New("invalid signature")
	ErrMissingSignature = errors.New("missing signature")
	ErrUnknownKeyID     = errors.New("unknown key ID")
)

// SignPayload signs raw bytes and returns a RequestSignature.
func SignPayload(payload []byte, keyID string, privateKey ed25519.PrivateKey) *signaturepb.RequestSignature {
	sig := ed25519.Sign(privateKey, payload)
	return &signaturepb.RequestSignature{
		KeyId:         keyID,
		Signature:     sig,
		SignedPayload: payload,
	}
}

// Sign serializes a Request (without signature), signs the bytes, and attaches the RequestSignature.
func Sign(req *servicepb.Request, keyID string, privateKey ed25519.PrivateKey) error {
	// Clone the request and clear any existing signature to get the canonical form
	reqCopy := req.CloneVT()
	reqCopy.Signature = nil

	payload, err := reqCopy.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling request for signing: %w", err)
	}

	req.Signature = SignPayload(payload, keyID, privateKey)
	return nil
}

// Verify checks the Ed25519 signature on signed_payload.
// It does not re-serialize anything — it verifies the exact bytes provided by the client.
func Verify(sig *signaturepb.RequestSignature, publicKey ed25519.PublicKey) error {
	if sig == nil {
		return ErrMissingSignature
	}
	if len(sig.SignedPayload) == 0 {
		return fmt.Errorf("%w: empty signed_payload", ErrInvalidSignature)
	}
	if len(sig.Signature) != ed25519.SignatureSize {
		return fmt.Errorf("%w: invalid signature length %d", ErrInvalidSignature, len(sig.Signature))
	}

	if !ed25519.Verify(publicKey, sig.SignedPayload, sig.Signature) {
		return ErrInvalidSignature
	}
	return nil
}

// ExtractRequest deserializes the signed_payload into a trusted Request.
func ExtractRequest(sig *signaturepb.RequestSignature) (*servicepb.Request, error) {
	if sig == nil {
		return nil, ErrMissingSignature
	}
	if len(sig.SignedPayload) == 0 {
		return nil, fmt.Errorf("%w: empty signed_payload", ErrInvalidSignature)
	}

	req := &servicepb.Request{}
	if err := req.UnmarshalVT(sig.SignedPayload); err != nil {
		return nil, fmt.Errorf("unmarshaling signed_payload: %w", err)
	}
	return req, nil
}
