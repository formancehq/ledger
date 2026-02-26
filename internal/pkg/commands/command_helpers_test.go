package commands

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// NewRequest creates a servicepb.Request with optional idempotency key for test purposes.
func NewRequest(req *servicepb.Request, idempotencyKey string) *servicepb.Request {
	req.IdempotencyKey = idempotencyKey
	return req
}
