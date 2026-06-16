package servicepb

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// UnsignedEnvelope wraps a Request into an unsigned Envelope for ApplyRequest.
func UnsignedEnvelope(req *Request) *Envelope {
	return &Envelope{Variant: &Envelope_Unsigned{Unsigned: req}}
}

// SignedEnvelope wraps a SignedRequest into a signed Envelope for ApplyRequest.
func SignedEnvelope(sr *signaturepb.SignedRequest) *Envelope {
	return &Envelope{Variant: &Envelope_Signed{Signed: sr}}
}

// UnsignedEnvelopes is a convenience for batches of unsigned requests.
func UnsignedEnvelopes(reqs ...*Request) []*Envelope {
	envelopes := make([]*Envelope, len(reqs))
	for i, req := range reqs {
		envelopes[i] = UnsignedEnvelope(req)
	}

	return envelopes
}

// PeekRequest returns the Request inside an Envelope without verifying its
// signature. Use ONLY for non-authoritative inspection — for example scope
// routing, where the type drives an authorization decision but the trusted
// content comes from admission's signature-verified path. Callers that act
// on the Request beyond routing must instead go through the verification
// chain (signing.Verify + signing.ExtractRequest).
func PeekRequest(env *Envelope) (*Request, error) {
	switch v := env.GetVariant().(type) {
	case *Envelope_Unsigned:
		if v.Unsigned == nil {
			return nil, errors.New("empty unsigned envelope")
		}

		return v.Unsigned, nil
	case *Envelope_Signed:
		if v.Signed == nil {
			return nil, errors.New("empty signed envelope")
		}

		req := &Request{}
		if err := req.UnmarshalVT(v.Signed.GetPayload()); err != nil {
			return nil, fmt.Errorf("peeking signed payload: %w", err)
		}

		return req, nil
	default:
		return nil, errors.New("envelope has no variant")
	}
}
