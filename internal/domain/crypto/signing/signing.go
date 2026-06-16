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

// Sign serializes a Request and returns a SignedRequest envelope carrying
// the exact bytes signed by the client. The returned envelope is opaque:
// the server verifies the signature against payload and then unmarshals
// payload — it never re-serializes the request, so cross-language clients
// are safe regardless of their protobuf implementation's quirks.
func Sign(req *servicepb.Request, keyID string, privateKey ed25519.PrivateKey) (*signaturepb.SignedRequest, error) {
	payload, err := req.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling request for signing: %w", err)
	}

	return &signaturepb.SignedRequest{
		KeyId:     keyID,
		Signature: ed25519.Sign(privateKey, payload),
		Payload:   payload,
	}, nil
}

// Verify checks the Ed25519 signature on a SignedRequest envelope.
// It verifies the exact bytes provided by the client; no re-serialization.
func Verify(sr *signaturepb.SignedRequest, publicKey ed25519.PublicKey) error {
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

// ExtractRequest deserializes the envelope payload into a trusted Request.
// Callers must call Verify first; ExtractRequest does not check the signature.
func ExtractRequest(sr *signaturepb.SignedRequest) (*servicepb.Request, error) {
	if sr == nil {
		return nil, ErrMissingSignature
	}

	if len(sr.GetPayload()) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalidSignature)
	}

	req := &servicepb.Request{}

	if err := req.UnmarshalVT(sr.GetPayload()); err != nil {
		return nil, fmt.Errorf("unmarshaling payload: %w", err)
	}

	return req, nil
}
