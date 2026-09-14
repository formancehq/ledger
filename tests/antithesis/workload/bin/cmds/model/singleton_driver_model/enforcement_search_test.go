package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// A revert can fail only after both a chart removal and a STRICT switch commit.
// The search must consider this joint state while preserving the observation's
// ticket boundary; neither predecessor alone explains the observed failure.
func TestCandidateBasesExplainsRevertAfterEnforcementAndChartChanges(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	seeded := c.modelState.Apply(bulkOf(
		oracletest.AddTypeReq("known"),
		oracletest.AddTypeReq("other"),
		oracletest.TxReq("world", "known:1", "USD", 10),
		enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false),
	))
	require.True(t, seeded.OK)
	c.modelState = seeded.State
	c.pending = []*pendingObservation{
		{minSeq: 5, obs: observation{bulk: bulkOf(oracletest.RemoveTypeReq("known")), ticket: 5}},
		{minSeq: 6, obs: observation{bulk: bulkOf(enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, true)), ticket: 6}},
	}
	failed := bulkOf(oracletest.RevertReqL("L", 1, false), enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true))
	explains := func(maxTicket uint64) bool {
		matched := false
		c.candidateBases(maxTicket, func(base oracle.GlobalState) bool {
			result := base.Apply(failed)
			matched = !result.OK && result.Reason == domain.ErrReasonAccountNotMatchingType
			return matched
		})
		return matched
	}
	require.True(t, c.modelState.Apply(failed).OK, "the initial AUDIT state accepts the revert")
	require.False(t, explains(5), "removing a type under AUDIT cannot explain a chart failure")
	require.True(t, explains(6), "removal followed by STRICT must explain the server's chart failure")
	require.True(t, c.modelState.Apply(failed).OK, "candidate exploration must not mutate the committed prefix")
}
