//go:build antithesis

package balancehistory

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestIdempotencyReductionProbeAllowsExpiredKeyReuseWithNewAuditIdentity(t *testing.T) {
	t.Parallel()

	probe := idempotencyReductionProbe{}
	probe.RecordPublished([]VerifiedProposal{
		keyedPublishedProposal("reusable-key", 10, 20),
		keyedPublishedProposal("reusable-key", 11, 21),
	})

	require.Equal(t, map[publishedKeyedAudit]string{
		{auditSequence: 10, minLogSequence: 20, maxLogSequence: 20}: "reusable-key",
		{auditSequence: 11, minLogSequence: 21, maxLogSequence: 21}: "reusable-key",
	}, probe.published)

	probe.Reset()
	require.Nil(t, probe.published)
}

func keyedPublishedProposal(key string, auditSequence, logSequence uint64) VerifiedProposal {
	return VerifiedProposal{Entry: &auditpb.AuditEntry{
		Sequence:    auditSequence,
		Idempotency: &commonpb.Idempotency{Key: key},
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: logSequence,
			MaxLogSequence: logSequence,
		}},
	}}
}
