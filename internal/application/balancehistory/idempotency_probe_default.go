//go:build !antithesis

package balancehistory

type idempotencyReductionProbe struct{}

func (idempotencyReductionProbe) Reset() {}

func (idempotencyReductionProbe) RecordPublished(_ []VerifiedProposal) {}
