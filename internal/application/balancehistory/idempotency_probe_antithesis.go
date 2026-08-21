//go:build antithesis

package balancehistory

import "github.com/antithesishq/antithesis-sdk-go/assert"

type publishedKeyedAudit struct {
	auditSequence  uint64
	minLogSequence uint64
	maxLogSequence uint64
}

type idempotencyReductionProbe struct {
	published map[publishedKeyedAudit]string
}

func (p *idempotencyReductionProbe) Reset() {
	p.published = nil
}

// RecordPublished runs only after the immutable history publication commits.
// Failures and zero-log successes cannot add monetary effects, so only keyed
// successful fresh-log ranges participate in the duplicate-reduction guard.
func (p *idempotencyReductionProbe) RecordPublished(proposals []VerifiedProposal) {
	if p.published == nil {
		p.published = make(map[publishedKeyedAudit]string)
	}

	for _, proposal := range proposals {
		entry := proposal.Entry
		if entry == nil || entry.GetSuccess() == nil {
			continue
		}
		key := entry.GetIdempotency().GetKey()
		success := entry.GetSuccess()
		if key == "" || success.GetMinLogSequence() == 0 || success.GetMaxLogSequence() == 0 {
			continue
		}

		current := publishedKeyedAudit{
			auditSequence:  entry.GetSequence(),
			minLogSequence: success.GetMinLogSequence(),
			maxLogSequence: success.GetMaxLogSequence(),
		}
		previousKey, duplicate := p.published[current]
		assert.AlwaysOrUnreachable(
			!duplicate,
			"historical balance: builder never publishes one keyed committed effect range twice",
			map[string]any{
				"first_idempotency_key":   previousKey,
				"current_idempotency_key": key,
				"audit_sequence":          current.auditSequence,
				"min_log_sequence":        current.minLogSequence,
				"max_log_sequence":        current.maxLogSequence,
			},
		)
		if !duplicate {
			p.published[current] = key
		}
	}
}
