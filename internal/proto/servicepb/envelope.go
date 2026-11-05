package servicepb

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

// UnsignedApplyRequest builds an unsigned ApplyRequest carrying a batch of
// requests under a single idempotency key (empty key = no idempotency).
func UnsignedApplyRequest(idempotencyKey string, reqs ...*Request) *ApplyRequest {
	return &ApplyRequest{
		Variant: &ApplyRequest_Unsigned{
			Unsigned: &ApplyBatch{Requests: reqs, IdempotencyKey: idempotencyKey},
		},
	}
}

// SignedApplyRequest wraps an opaque SignedApplyBatch (payload = serialized
// ApplyBatch bytes) into the ApplyRequest the Apply RPC carries.
func SignedApplyRequest(sr *signaturepb.SignedApplyBatch) *ApplyRequest {
	return &ApplyRequest{Variant: &ApplyRequest_Signed{Signed: sr}}
}

// PeekBatch returns the ApplyBatch inside an ApplyRequest WITHOUT verifying its
// signature. Use ONLY for non-authoritative inspection — for example scope
// routing, where the request type drives an authorization decision but the
// trusted content comes from admission's signature-verified path. Callers that
// act on the content beyond routing must go through the verification chain
// (signing.Verify + signing.ExtractBatch).
func PeekBatch(ar *ApplyRequest) (*ApplyBatch, error) {
	switch v := ar.GetVariant().(type) {
	case *ApplyRequest_Unsigned:
		if v.Unsigned == nil {
			return nil, errors.New("empty unsigned apply request")
		}

		return v.Unsigned, nil
	case *ApplyRequest_Signed:
		if v.Signed == nil {
			return nil, errors.New("empty signed apply request")
		}

		batch := &ApplyBatch{}
		if err := batch.UnmarshalVT(v.Signed.GetPayload()); err != nil {
			return nil, fmt.Errorf("peeking signed payload: %w", err)
		}

		return batch, nil
	default:
		return nil, errors.New("apply request has no variant")
	}
}
